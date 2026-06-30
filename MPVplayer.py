# Copyright (c) 2021 by xfangfang. All Rights Reserved.
# Optimized version (v0.27) – property name fix and resilience polish
#
# v0.27 changes (based on v0.26):
#   - Fixed observed MPV property names to use hyphens (e.g., "time-pos")
#   - Config errors now trigger user notifications via "app_notify" events
#   - Thread pool shutdown has a timeout with forced termination fallback
#   - TempFileManager cleanup retries on failure (up to 3 attempts)
#   - Minor logging improvements and thread name consistency
#
# Macast Metadata
# <macast.title>MPVPlayer Renderer</macast.title>
# <macast.renderer>MPVplayerRenderer</macast.renderer>
# <macast.platform>win32</macast.platform>
# <macast.version>0.27</macast.version>
# <macast.host_version>0.7</macast.host_version>
# <macast.author>HWT</macast.author>
# <macast.desc>调用本地MPV，支持章节/音轨/字幕信息、播放列表管理、断点续播及完整配置</macast.desc>

import os
import sys
import time
import json
import logging
import shutil
import threading
import subprocess
import socket
import uuid
import tempfile
import queue
import atexit
import weakref
from collections import OrderedDict
from enum import Enum
from typing import Optional, List, Dict, Any, Union, Tuple, Callable
from contextlib import suppress
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError as FutureTimeoutError

import cherrypy
from macast import gui, Setting
from macast.renderer import Renderer
from macast.utils import SETTING_DIR

# ----------------------------------------------------------------------
# 常量 & 日志适配器
# ----------------------------------------------------------------------
IS_WINDOWS = sys.platform == 'win32'
logger = logging.getLogger("MPVPlayer")

class MPVLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        extra = kwargs.get('extra', {})
        extra.setdefault('pid', os.getpid())
        request_id = getattr(threading.current_thread(), 'request_id', None)
        if request_id:
            extra['request_id'] = request_id
        kwargs['extra'] = extra
        return msg, kwargs

log = MPVLoggerAdapter(logger, {})

def _sanitize_url(url: str) -> str:
    """去除 URL 中的敏感信息，仅保留 scheme://host 部分。"""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.hostname}"
    except Exception:
        return url[:50] + "..." if len(url) > 50 else url

# ----------------------------------------------------------------------
# 辅助函数
# ----------------------------------------------------------------------
def _format_time(seconds: float) -> str:
    key = int(seconds)
    cache = getattr(_format_time, '_cache', None)
    if cache is None or cache[0] != key:
        h, m, s = key // 3600, (key % 3600) // 60, key % 60
        _format_time._cache = (key, f"{h:02d}:{m:02d}:{s:02d}")
    return _format_time._cache[1]

# ----------------------------------------------------------------------
# Windows 管道包装类
# ----------------------------------------------------------------------
if IS_WINDOWS:
    try:
        from multiprocessing.connection import PipeConnection
    except ImportError:
        PipeConnection = None

    _HAS_PIPE_CONNECTION = PipeConnection is not None

    class PipeFileWrapper:
        MAX_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB limit

        def __init__(self, conn, on_overflow: Callable[[], None] = None):
            self.conn = conn
            self.buffer = b""
            self._on_overflow = on_overflow

        def readline(self) -> str:
            while b'\n' not in self.buffer:
                if len(self.buffer) > self.MAX_BUFFER_SIZE:
                    log.error("Pipe buffer exceeded 10MB limit, resetting connection")
                    self.buffer = b""
                    if self._on_overflow:
                        self._on_overflow()
                    return ''
                try:
                    chunk = self.conn.recv_bytes(4096)
                except (BrokenPipeError, ConnectionResetError, EOFError) as e:
                    log.debug(f"Pipe read error: {e}")
                    break
                if not chunk:
                    break
                self.buffer += chunk
            if b'\n' in self.buffer:
                line, self.buffer = self.buffer.split(b'\n', 1)
                return line.decode('utf-8')
            elif self.buffer:
                remaining = self.buffer
                self.buffer = b""
                log.warning(f"Partial message read (no newline): {remaining!r}")
                return remaining.decode('utf-8', errors='replace')
            return ''

        def close(self):
            pass

    class PipeConnectionWrapper:
        def __init__(self, conn, on_overflow: Callable[[], None] = None):
            self.conn = conn
            self._on_overflow = on_overflow

        def sendall(self, data: bytes) -> None:
            self.conn.send_bytes(data)

        def recv(self, bufsize: int) -> bytes:
            return self.conn.recv_bytes(bufsize)

        def makefile(self, mode: str = 'r') -> PipeFileWrapper:
            return PipeFileWrapper(self.conn, on_overflow=self._on_overflow)

        def close(self) -> None:
            with suppress(Exception):
                self.conn.close()

    def _winapi_connect(socket_path: str, connect_timeout: float):
        import _winapi
        try:
            _winapi.WaitNamedPipe(socket_path, int(connect_timeout * 1000))
        except Exception:
            pass
        handle = _winapi.CreateFile(
            socket_path,
            _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
            0, _winapi.NULL, _winapi.OPEN_EXISTING,
            _winapi.FILE_FLAG_OVERLAPPED, _winapi.NULL
        )
        if PipeConnection:
            return PipeConnectionWrapper(PipeConnection(handle))
        return None

# ----------------------------------------------------------------------
# 枚举定义
# ----------------------------------------------------------------------
class SettingProperty(Enum):
    MPVplayer_Path = "MPVplayer_Path"
    MPVplayer_Debug = "MPVplayer_Debug"
    MPVplayer_IPC_Connect_Timeout = "MPVplayer_IPC_Connect_Timeout"
    MPVplayer_Command_Timeout = "MPVplayer_Command_Timeout"
    MPVplayer_Connection_Wait = "MPVplayer_Connection_Wait"
    MPVplayer_Extra_Args = "MPVplayer_Extra_Args"
    MPVplayer_IPC_Retry_Count = "MPVplayer_IPC_Retry_Count"
    MPVplayer_IPC_Retry_Base = "MPVplayer_IPC_Retry_Base"
    MPVplayer_Process_Terminate_Timeout = "MPVplayer_Process_Terminate_Timeout"
    MPVplayer_Cleanup_Quit_Timeout = "MPVplayer_Cleanup_Quit_Timeout"
    MPVplayer_Position_Update_Interval = "MPVplayer_Position_Update_Interval"
    MPVplayer_Min_Position_Change = "MPVplayer_Min_Position_Change"
    MPVplayer_Max_Speed = "MPVplayer_Max_Speed"
    MPVplayer_Min_Speed = "MPVplayer_Min_Speed"
    MPVplayer_Event_Queue_Maxsize = "MPVplayer_Event_Queue_Maxsize"
    MPVplayer_Heartbeat_Interval = "MPVplayer_Heartbeat_Interval"
    MPVplayer_Heartbeat_Failure_Threshold = "MPVplayer_Heartbeat_Failure_Threshold"
    MPVplayer_Audio_Only_Extensions = "MPVplayer_Audio_Only_Extensions"
    MPVplayer_IPC_Command_Retry_Delay = "MPVplayer_IPC_Command_Retry_Delay"
    MPVplayer_Seamless_Switch_Timeout = "MPVplayer_Seamless_Switch_Timeout"
    MPVplayer_Resume_Enabled = "MPVplayer_Resume_Enabled"
    MPVplayer_Resume_Data_Path = "MPVplayer_Resume_Data_Path"
    MPVplayer_OSD_Default_Duration = "MPVplayer_OSD_Default_Duration"
    MPVplayer_Resume_Save_Interval = "MPVplayer_Resume_Save_Interval"
    MPVplayer_Start_Lock_Timeout = "MPVplayer_Start_Lock_Timeout"
    MPVplayer_Cleanup_Total_Timeout = "MPVplayer_Cleanup_Total_Timeout"

class ObservedProperty(Enum):
    PAUSE = 1
    TIME_POS = 2
    DURATION = 3
    VOLUME = 4
    MEDIA_TITLE = 5
    METADATA = 6
    CHAPTER_LIST = 7
    CHAPTER = 8
    PLAYLIST = 9
    AID = 10
    SID = 11
    TRACK_LIST = 12

    # v0.27: 显式映射到 MPV 属性名（使用连字符）
    @property
    def mpv_name(self) -> str:
        return {
            ObservedProperty.PAUSE: "pause",
            ObservedProperty.TIME_POS: "time-pos",
            ObservedProperty.DURATION: "duration",
            ObservedProperty.VOLUME: "volume",
            ObservedProperty.MEDIA_TITLE: "media-title",
            ObservedProperty.METADATA: "metadata",
            ObservedProperty.CHAPTER_LIST: "chapter-list",
            ObservedProperty.CHAPTER: "chapter",
            ObservedProperty.PLAYLIST: "playlist",
            ObservedProperty.AID: "aid",
            ObservedProperty.SID: "sid",
            ObservedProperty.TRACK_LIST: "track-list",
        }[self]

class RendererState(Enum):
    IDLE = 0
    RUNNING = 1
    CLEANING = 2
    STOPPED = 3

# ----------------------------------------------------------------------
# 配置类 (v0.27 增加错误通知)
# ----------------------------------------------------------------------
class Config:
    _mtime: Optional[float] = None
    _lock = threading.Lock()
    # 默认值
    IPC_RETRY_COUNT: int = 10
    IPC_RETRY_BASE: float = 0.5
    IPC_CONNECT_TIMEOUT: float = 2.0
    IPC_COMMAND_TIMEOUT: float = 5.0
    IPC_CONNECTION_WAIT: float = 1.5
    IPC_COMMAND_RETRY: int = 2
    IPC_COMMAND_RETRY_DELAY: float = 0.2
    SEAMLESS_SWITCH_TIMEOUT: float = 2.0
    PROCESS_TERMINATE_TIMEOUT: float = 3.0
    CLEANUP_QUIT_TIMEOUT: float = 1.0
    POSITION_UPDATE_INTERVAL: float = 0.5
    MIN_POSITION_CHANGE: float = 0.1
    MAX_SPEED: float = 2.0
    MIN_SPEED: float = 0.1
    EVENT_QUEUE_MAXSIZE: int = 500
    HEARTBEAT_INTERVAL: float = 5.0
    HEARTBEAT_FAILURE_THRESHOLD: int = 3
    EXTRA_ARGS: List[str] = []
    AUDIO_ONLY_EXTENSIONS: List[str] = ['.mp3', '.flac', '.wav', '.aac', '.ogg', '.m4a']
    RESUME_ENABLED: bool = True
    RESUME_DATA_PATH: str = os.path.join(SETTING_DIR, "mpv_resume.json")
    RESUME_SAVE_INTERVAL: float = 30.0
    OSD_DEFAULT_DURATION: int = 3000
    START_LOCK_TIMEOUT: float = 10.0
    CLEANUP_TOTAL_TIMEOUT: float = 15.0

    @classmethod
    def _safe_convert(cls, setting_prop: SettingProperty, default: Any, convert_func: Callable) -> Any:
        try:
            value = Setting.get(setting_prop, default)
            return convert_func(value)
        except Exception as e:
            log.warning(f"Invalid config for {setting_prop.value}, using default {default}: {e}")
            # v0.27: 通知用户配置错误（非阻塞）
            try:
                cherrypy.engine.publish("app_notify", "Config Error",
                                        f"Invalid value for '{setting_prop.value}': {e}. Using default.")
            except Exception:
                pass
            return default

    @classmethod
    def _get_setting_file_mtime(cls) -> float:
        try:
            return os.path.getmtime(Setting.setting_path)
        except OSError:
            return 0.0

    @classmethod
    def reload_if_changed(cls):
        current_mtime = cls._get_setting_file_mtime()
        with cls._lock:
            if cls._mtime is not None and current_mtime == cls._mtime:
                return
            new_conf = {}
            new_conf['IPC_CONNECT_TIMEOUT'] = cls._safe_convert(SettingProperty.MPVplayer_IPC_Connect_Timeout, cls.IPC_CONNECT_TIMEOUT, float)
            new_conf['IPC_COMMAND_TIMEOUT'] = cls._safe_convert(SettingProperty.MPVplayer_Command_Timeout, cls.IPC_COMMAND_TIMEOUT, float)
            new_conf['IPC_CONNECTION_WAIT'] = cls._safe_convert(SettingProperty.MPVplayer_Connection_Wait, cls.IPC_CONNECTION_WAIT, float)
            new_conf['IPC_RETRY_COUNT'] = cls._safe_convert(SettingProperty.MPVplayer_IPC_Retry_Count, cls.IPC_RETRY_COUNT, int)
            new_conf['IPC_RETRY_BASE'] = cls._safe_convert(SettingProperty.MPVplayer_IPC_Retry_Base, cls.IPC_RETRY_BASE, float)
            new_conf['PROCESS_TERMINATE_TIMEOUT'] = cls._safe_convert(SettingProperty.MPVplayer_Process_Terminate_Timeout, cls.PROCESS_TERMINATE_TIMEOUT, float)
            new_conf['CLEANUP_QUIT_TIMEOUT'] = cls._safe_convert(SettingProperty.MPVplayer_Cleanup_Quit_Timeout, cls.CLEANUP_QUIT_TIMEOUT, float)
            new_conf['POSITION_UPDATE_INTERVAL'] = cls._safe_convert(SettingProperty.MPVplayer_Position_Update_Interval, cls.POSITION_UPDATE_INTERVAL, float)
            new_conf['MIN_POSITION_CHANGE'] = cls._safe_convert(SettingProperty.MPVplayer_Min_Position_Change, cls.MIN_POSITION_CHANGE, float)
            new_conf['MAX_SPEED'] = cls._safe_convert(SettingProperty.MPVplayer_Max_Speed, cls.MAX_SPEED, float)
            new_conf['MIN_SPEED'] = cls._safe_convert(SettingProperty.MPVplayer_Min_Speed, cls.MIN_SPEED, float)
            new_conf['EVENT_QUEUE_MAXSIZE'] = cls._safe_convert(SettingProperty.MPVplayer_Event_Queue_Maxsize, cls.EVENT_QUEUE_MAXSIZE, int)
            new_conf['HEARTBEAT_INTERVAL'] = cls._safe_convert(SettingProperty.MPVplayer_Heartbeat_Interval, cls.HEARTBEAT_INTERVAL, float)
            new_conf['HEARTBEAT_FAILURE_THRESHOLD'] = cls._safe_convert(SettingProperty.MPVplayer_Heartbeat_Failure_Threshold, cls.HEARTBEAT_FAILURE_THRESHOLD, int)
            new_conf['IPC_COMMAND_RETRY_DELAY'] = cls._safe_convert(SettingProperty.MPVplayer_IPC_Command_Retry_Delay, cls.IPC_COMMAND_RETRY_DELAY, float)
            new_conf['SEAMLESS_SWITCH_TIMEOUT'] = cls._safe_convert(SettingProperty.MPVplayer_Seamless_Switch_Timeout, cls.SEAMLESS_SWITCH_TIMEOUT, float)
            new_conf['RESUME_ENABLED'] = cls._safe_convert(SettingProperty.MPVplayer_Resume_Enabled, cls.RESUME_ENABLED, bool)
            new_conf['RESUME_DATA_PATH'] = Setting.get(SettingProperty.MPVplayer_Resume_Data_Path, cls.RESUME_DATA_PATH) or os.path.join(SETTING_DIR, "mpv_resume.json")
            new_conf['RESUME_SAVE_INTERVAL'] = cls._safe_convert(SettingProperty.MPVplayer_Resume_Save_Interval, cls.RESUME_SAVE_INTERVAL, float)
            new_conf['OSD_DEFAULT_DURATION'] = cls._safe_convert(SettingProperty.MPVplayer_OSD_Default_Duration, cls.OSD_DEFAULT_DURATION, int)
            new_conf['START_LOCK_TIMEOUT'] = cls._safe_convert(SettingProperty.MPVplayer_Start_Lock_Timeout, cls.START_LOCK_TIMEOUT, float)
            new_conf['CLEANUP_TOTAL_TIMEOUT'] = cls._safe_convert(SettingProperty.MPVplayer_Cleanup_Total_Timeout, cls.CLEANUP_TOTAL_TIMEOUT, float)

            extra = Setting.get(SettingProperty.MPVplayer_Extra_Args, "")
            new_conf['EXTRA_ARGS'] = [arg.strip() for arg in extra.split(";") if arg.strip()] if isinstance(extra, str) and extra.strip() else []
            audio_ext = Setting.get(SettingProperty.MPVplayer_Audio_Only_Extensions, "")
            if isinstance(audio_ext, str) and audio_ext.strip():
                new_conf['AUDIO_ONLY_EXTENSIONS'] = [ext.strip() for ext in audio_ext.split(",") if ext.strip()]
            else:
                new_conf['AUDIO_ONLY_EXTENSIONS'] = ['.mp3', '.flac', '.wav', '.aac', '.ogg', '.m4a']

            for k, v in new_conf.items():
                setattr(cls, k, v)
            cls._mtime = current_mtime
            log.info("Configuration reloaded from file")

    @classmethod
    def load(cls):
        with cls._lock:
            cls._mtime = cls._get_setting_file_mtime()
        cls.reload_if_changed()

# ----------------------------------------------------------------------
# MPV 查找器
# ----------------------------------------------------------------------
class MpvFinder:
    _cache: Optional[str] = None

    @classmethod
    def find(cls) -> Optional[str]:
        if cls._cache is not None:
            return cls._cache

        path = Setting.get(SettingProperty.MPVplayer_Path, None)
        if path and os.path.isfile(path):
            cls._cache = path
            log.info(f"MPV path from config: {path}")
            return path

        exe_name = "mpv.exe" if IS_WINDOWS else "mpv"
        which = shutil.which(exe_name)
        if which:
            cls._cache = which
            log.info(f"MPV found in PATH: {which}")
            return which

        default_paths = [
            r"C:\Program Files\mpv\mpv.exe",
            r"D:\mpv\mpv.exe",
            r"C:\Program Files (x86)\mpv\mpv.exe",
            "/usr/bin/mpv",
            "/usr/local/bin/mpv",
        ]
        if IS_WINDOWS:
            try:
                import winreg
                with winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                                    r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\mpv.exe") as key:
                    reg_path, _ = winreg.QueryValueEx(key, "")
                    if os.path.isfile(reg_path):
                        cls._cache = reg_path
                        log.info(f"MPV found via registry: {reg_path}")
                        return reg_path
            except Exception as e:
                log.debug(f"Registry lookup failed: {e}")

        for p in default_paths:
            if os.path.isfile(p):
                cls._cache = p
                log.info(f"MPV found at default path: {p}")
                return p

        log.error("MPV executable not found.")
        return None

# ----------------------------------------------------------------------
# MPV 进程管理器
# ----------------------------------------------------------------------
class MPVProcessManager:
    def __init__(self, executor: ThreadPoolExecutor):
        self.executor = executor
        self.process: Optional[subprocess.Popen] = None
        self.monitor_future: Optional[Future] = None
        self._lock = threading.Lock()
        self._stop_monitor = threading.Event()
        self._on_exit_callback: Optional[Callable[[], None]] = None
        self._exit_callback_invoked = False

    def set_exit_callback(self, callback: Callable[[], None]):
        self._on_exit_callback = callback

    def start(self, cmd: List[str], debug: bool = False) -> bool:
        try:
            creationflags = subprocess.CREATE_NO_WINDOW if IS_WINDOWS else 0
            if debug:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                        creationflags=creationflags)
                self.executor.submit(self._log_output, proc)
            else:
                proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                        creationflags=creationflags)
            with self._lock:
                self.process = proc
            self._stop_monitor.clear()
            self._exit_callback_invoked = False
            self.monitor_future = self.executor.submit(self._monitor, proc)
            return True
        except Exception as e:
            log.exception("Failed to start MPV process")
            return False

    def _log_output(self, proc: subprocess.Popen):
        for line in iter(proc.stdout.readline, ''):
            log.debug(f"MPV: {line.rstrip()}")

    def _monitor(self, proc: subprocess.Popen):
        start_time = time.time()
        while proc.poll() is None:
            if self._stop_monitor.is_set():
                return
            time.sleep(0.2)
        exit_code = proc.poll()
        elapsed = time.time() - start_time
        log.info(f"MPV process exited with code {exit_code} after {elapsed:.1f}s")
        if self._on_exit_callback and not self._exit_callback_invoked:
            self._exit_callback_invoked = True
            self._on_exit_callback()

    def is_alive(self) -> bool:
        with self._lock:
            return self.process is not None and self.process.poll() is None

    def stop_monitoring(self):
        self._stop_monitor.set()

    def terminate(self, timeout: float = None) -> Optional[int]:
        with self._lock:
            proc = self.process
        if proc is None or proc.poll() is not None:
            return proc.returncode if proc else None
        log.info(f"Terminating MPV process (PID {proc.pid})")
        proc.terminate()
        try:
            proc.wait(timeout=timeout or Config.PROCESS_TERMINATE_TIMEOUT)
        except subprocess.TimeoutExpired:
            log.warning("MPV did not terminate, killing")
            proc.kill()
            proc.wait()
        return proc.returncode

    def kill(self):
        with self._lock:
            proc = self.process
            if proc and proc.poll() is None:
                proc.kill()
                proc.wait()

    def reset(self):
        self.stop_monitoring()
        with self._lock:
            self.process = None
        if self.monitor_future:
            self.monitor_future.cancel()

# ----------------------------------------------------------------------
# 临时文件管理器 (v0.27 清理重试)
# ----------------------------------------------------------------------
class TempFileManager:
    def __init__(self):
        self._temp_dir = None
        self._files: set = set()
        self._lock = threading.Lock()
        self._active = False
        self._cleanup_stale()

    def _cleanup_stale(self):
        if IS_WINDOWS:
            return
        import glob
        tmp_root = tempfile.gettempdir()
        for d in glob.glob(os.path.join(tmp_root, "macast_mpv_*")):
            try:
                shutil.rmtree(d, ignore_errors=True)
                log.info(f"Cleaned up stale temp dir: {d}")
            except Exception:
                pass

    def create(self) -> str:
        with self._lock:
            if self._temp_dir is None:
                self._temp_dir = tempfile.TemporaryDirectory(prefix="macast_mpv_")
                self._active = True
                log.info(f"Created temp dir: {self._temp_dir.name}")
            return self._temp_dir.name

    def create_temp_file(self, content: str, suffix: str = ".m3u") -> str:
        with self._lock:
            if self._temp_dir is None:
                self.create()
            fd, path = tempfile.mkstemp(suffix=suffix, dir=self._temp_dir.name)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(content)
            self._files.add(path)
            log.debug(f"Created temp file: {path} ({len(content)} bytes)")
            return path

    def copy_file_to_temp(self, src_path: str, suffix: Optional[str] = None) -> str:
        if not os.path.isfile(src_path):
            raise FileNotFoundError(f"Source file not found: {src_path}")
        with self._lock:
            if self._temp_dir is None:
                self.create()
            base = os.path.basename(src_path)
            if suffix is None:
                _, ext = os.path.splitext(base)
                suffix = ext or ".tmp"
            fd, dst_path = tempfile.mkstemp(suffix=suffix, dir=self._temp_dir.name, prefix="sub_")
            os.close(fd)
            shutil.copy2(src_path, dst_path)
            self._files.add(dst_path)
            log.info(f"Copied file {src_path} -> {dst_path}")
            return dst_path

    def delete_file(self, path: str) -> None:
        with self._lock:
            self._files.discard(path)
            with suppress(Exception):
                os.unlink(path)
                log.debug(f"Deleted temp file: {path}")

    def remove_all(self) -> None:
        with self._lock:
            for f in list(self._files):
                with suppress(Exception):
                    os.unlink(f)
                self._files.discard(f)
            self._files.clear()

    def cleanup(self, force: bool = False) -> None:
        with self._lock:
            if not self._active:
                return
            self._active = False
            self.remove_all()
            if self._temp_dir:
                # v0.27: 清理重试机制
                for attempt in range(3):
                    try:
                        self._temp_dir.cleanup()
                        log.info(f"Removed temp dir: {self._temp_dir.name}")
                        break
                    except Exception:
                        if attempt < 2:
                            time.sleep(0.5)
                        else:
                            log.warning(f"Failed to remove temp dir after 3 attempts: {self._temp_dir.name}")
                self._temp_dir = None

# ----------------------------------------------------------------------
# 播放状态管理
# ----------------------------------------------------------------------
class PlaybackState:
    def __init__(self, on_state_change: Callable[[str, Any], None]):
        self._on_state_change = on_state_change
        self._lock = threading.RLock()
        self._playing = False
        self._pause = False
        self._volume = 50
        self._position = 0.0
        self._duration = 0.0
        self._pending_position: Optional[float] = None
        self._last_pos_update = 0.0
        self._media_title = ""
        self._metadata: Dict[str, Any] = {}
        self._current_uri: Optional[str] = None
        self._chapters: List[Dict[str, Any]] = []
        self._current_chapter: int = -1
        self._playlist: List[Dict[str, Any]] = []
        self._aid: int = -1
        self._sid: int = -1
        self._track_list: List[Dict[str, Any]] = []
        self._current_is_playlist: bool = False
        self._current_start_pos: int = 0
        self._current_audio_only: bool = False

    def _update_attr(self, attr: str, value: Any, cb_key: str) -> None:
        with self._lock:
            setattr(self, attr, value)
        try:
            self._on_state_change(cb_key, value)
        except Exception as e:
            log.exception(f"State change callback error for {cb_key}: {e}")

    def set_current_media_info(self, uri: str, is_playlist: bool, start: int, audio_only: bool) -> None:
        with self._lock:
            self._current_uri = uri
            self._current_is_playlist = is_playlist
            self._current_start_pos = start
            self._current_audio_only = audio_only

    def get_current_uri(self) -> Optional[str]:
        with self._lock:
            return self._current_uri

    def is_current_playlist(self) -> bool:
        with self._lock:
            return self._current_is_playlist

    def get_current_start_pos(self) -> int:
        with self._lock:
            return self._current_start_pos

    def get_current_audio_only(self) -> bool:
        with self._lock:
            return self._current_audio_only

    def update_pause(self, paused: bool) -> None:
        self._update_attr('_pause', paused, 'pause')

    def update_volume(self, vol: int) -> None:
        self._update_attr('_volume', vol, 'volume')

    def update_duration(self, dur: float) -> None:
        self._update_attr('_duration', dur, 'duration')

    def update_position(self, pos: float) -> None:
        with self._lock:
            self._pending_position = pos
            self._flush_position()

    def _flush_position(self) -> None:
        now = time.monotonic()
        if self._pending_position is None:
            return
        interval = Config.POSITION_UPDATE_INTERVAL
        min_change = Config.MIN_POSITION_CHANGE
        should_update = False
        pos = 0.0
        with self._lock:
            if now - self._last_pos_update < interval:
                return
            if abs(self._pending_position - self._position) < min_change:
                return
            pos = self._pending_position
            self._position = pos
            self._last_pos_update = now
            should_update = True
        if should_update:
            try:
                self._on_state_change('position', pos)
            except Exception as e:
                log.exception(f"State change callback error for position: {e}")

    def flush_position_forced(self) -> None:
        pos = 0.0
        with self._lock:
            if self._pending_position is not None:
                pos = self._pending_position
                self._position = pos
                self._last_pos_update = time.monotonic()
        if pos:
            try:
                self._on_state_change('position', pos)
            except Exception as e:
                log.exception(f"State change callback error for position: {e}")

    def reset(self) -> None:
        with self._lock:
            self._playing = False
            self._pause = False
            self._position = 0.0
            self._pending_position = None
            self._duration = 0.0
            self._media_title = ""
            self._metadata = {}
            self._chapters = []
            self._current_chapter = -1
            self._playlist = []
            self._aid = -1
            self._sid = -1
            self._track_list = []

    def mark_playing(self, is_playing: bool) -> None:
        with self._lock:
            self._playing = is_playing

    def update_title(self, title: str) -> None:
        self._update_attr('_media_title', title, 'title')

    def update_metadata(self, metadata: Dict) -> None:
        self._update_attr('_metadata', metadata, 'metadata')

    def update_chapters(self, chapters: List[Dict[str, Any]]) -> None:
        self._update_attr('_chapters', chapters, 'chapters')

    def update_chapter(self, chapter_index: int) -> None:
        self._update_attr('_current_chapter', chapter_index, 'chapter')

    def update_playlist(self, playlist: List[Dict[str, Any]]) -> None:
        self._update_attr('_playlist', playlist, 'playlist')

    def update_aid(self, aid: int) -> None:
        self._update_attr('_aid', aid, 'aid')

    def update_sid(self, sid: int) -> None:
        self._update_attr('_sid', sid, 'sid')

    def update_track_list(self, tracks: List[Dict[str, Any]]) -> None:
        self._update_attr('_track_list', tracks, 'track_list')

    def get_chapters(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._chapters)

    def get_playlist(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._playlist)

    def get_position(self) -> float:
        with self._lock:
            return self._position

    def get_aid(self) -> int:
        with self._lock:
            return self._aid

    def get_sid(self) -> int:
        with self._lock:
            return self._sid

    def get_track_list(self) -> List[Dict[str, Any]]:
        with self._lock:
            return list(self._track_list)

# ----------------------------------------------------------------------
# 断点续播管理器
# ----------------------------------------------------------------------
class ResumeManager:
    MAX_ENTRIES = 1000

    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._dirty = False
        self._load()

    def _load(self):
        try:
            if os.path.isfile(self.path):
                with open(self.path, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                self._cache = OrderedDict(raw)
                self._trim()
        except Exception as e:
            log.warning(f"Failed to load resume data: {e}")
            self._cache = OrderedDict()

    def _trim(self):
        while len(self._cache) > self.MAX_ENTRIES:
            oldest = next(iter(self._cache))
            del self._cache[oldest]
            log.debug(f"Resume entry evicted due to limit: {oldest}")

    def _save(self):
        if not self._dirty:
            return
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            with open(self.path, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
            self._dirty = False
        except Exception as e:
            log.error(f"Failed to save resume data: {e}")

    def save_position(self, uri: str, pos: float, is_playlist: bool = False) -> None:
        if not Config.RESUME_ENABLED or pos <= 0 or is_playlist:
            return
        with self._lock:
            if uri in self._cache:
                del self._cache[uri]
            self._cache[uri] = pos
            self._dirty = True
            self._trim()
            self._save()

    def get_position(self, uri: str, is_playlist: bool = False) -> Optional[float]:
        if not Config.RESUME_ENABLED or is_playlist:
            return None
        with self._lock:
            pos = self._cache.get(uri)
            if pos is not None:
                del self._cache[uri]
                self._cache[uri] = pos
            return pos

    def remove(self, uri: str):
        with self._lock:
            if uri in self._cache:
                del self._cache[uri]
                self._dirty = True
                self._save()

# ----------------------------------------------------------------------
# IPC 客户端 (v0.27 使用正确的属性名)
# ----------------------------------------------------------------------
class MpvIpcClient:
    def __init__(self, renderer: 'MPVplayerRenderer', socket_path: str, executor: ThreadPoolExecutor):
        self._renderer_ref = weakref.ref(renderer)
        self._socket_path = socket_path
        self._executor = executor
        self._lock = threading.RLock()
        self._sock: Optional[Any] = None
        self._sock_file = None
        self._connected = threading.Event()
        self._stop_event = threading.Event()
        self._request_id = 0
        self._pending_requests: Dict[int, threading.Event] = {}
        self._request_lock = threading.Lock()
        self._message_queue: queue.Queue = queue.Queue(maxsize=Config.EVENT_QUEUE_MAXSIZE)
        self._consumer_future: Optional[Future] = None
        self._heartbeat_future: Optional[Future] = None
        self._heartbeat_failures = 0
        self._heartbeat_cond = threading.Condition()
        self._heartbeat_paused = threading.Event()
        self._config_reload_counter = 0
        self._receiver_future: Optional[Future] = None
        self._overflow_callback = self._on_pipe_overflow

    def _on_pipe_overflow(self):
        log.error("Pipe buffer overflow detected, resetting IPC connection")
        self._close_socket()
        self._reset_connection_state()

    def start(self) -> None:
        self._stop_event.clear()
        self._receiver_future = self._executor.submit(self._loop)
        self._consumer_future = self._executor.submit(self._consumer_loop)
        self._heartbeat_future = self._executor.submit(self._heartbeat_loop)

    def stop(self, timeout: float = 2.0) -> None:
        self._stop_event.set()
        with self._heartbeat_cond:
            self._heartbeat_cond.notify_all()
        for fut in (self._receiver_future, self._consumer_future, self._heartbeat_future):
            if fut:
                try:
                    fut.result(timeout=timeout)
                except FutureTimeoutError:
                    fut.cancel()
        self._close_socket()

    def pause_heartbeat(self) -> None:
        self._heartbeat_paused.set()
        with self._heartbeat_cond:
            self._heartbeat_cond.notify_all()

    def resume_heartbeat(self) -> None:
        self._heartbeat_paused.clear()
        with self._heartbeat_cond:
            self._heartbeat_cond.notify_all()

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            if self._config_reload_counter % 10 == 0:
                Config.reload_if_changed()
            self._config_reload_counter += 1
            if not self._connect():
                if self._stop_event.is_set():
                    break
                time.sleep(Config.IPC_RETRY_BASE)
                continue
            Config.reload_if_changed()
            self._receive_loop()
        self._connected.clear()
        log.debug("IPC client receiver loop ended")

    def _consumer_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                data = self._message_queue.get(timeout=0.5)
                if data is None:
                    continue
                self._apply_message(data)
            except queue.Empty:
                continue
            except Exception as e:
                log.exception("Consumer error in thread %s", threading.current_thread().name)
        log.debug("IPC client consumer loop ended")

    def _heartbeat_loop(self) -> None:
        while not self._stop_event.is_set():
            with self._heartbeat_cond:
                if self._heartbeat_paused.is_set() or not self._connected.is_set():
                    self._heartbeat_cond.wait()
                    continue
                self._heartbeat_cond.wait(timeout=Config.HEARTBEAT_INTERVAL)
            if self._stop_event.is_set():
                break
            if not self._connected.is_set():
                self._heartbeat_failures = 0
                continue
            if self._heartbeat_paused.is_set():
                continue
            if not self._send_heartbeat():
                self._heartbeat_failures += 1
                log.debug(f"Heartbeat failure {self._heartbeat_failures}/{Config.HEARTBEAT_FAILURE_THRESHOLD}")
                if self._heartbeat_failures >= Config.HEARTBEAT_FAILURE_THRESHOLD:
                    log.warning("Heartbeat threshold reached, forcing reconnect")
                    self._heartbeat_failures = 0
                    self._close_socket()
                    self._reset_connection_state()
                    self._connected.clear()
            else:
                self._heartbeat_failures = 0

    def _send_heartbeat(self) -> bool:
        try:
            return self.send_command(["get_property", "time-pos"], timeout=1.0, retry=0, wait_response=True) is not None
        except Exception:
            return False

    def _connect(self) -> bool:
        retry_count = Config.IPC_RETRY_COUNT
        base = Config.IPC_RETRY_BASE
        for attempt in range(1, retry_count + 1):
            if self._stop_event.is_set():
                return False
            sock = self._create_connection()
            if sock is not None:
                with self._lock:
                    self._sock = sock
                    self._sock_file = None
                self._connected.set()
                self._observe_properties()
                log.info("IPC connected to MPV")
                return True
            wait = min(base * (2 ** (attempt - 1)), 5.0)
            if attempt < retry_count:
                time.sleep(wait)
        log.error("IPC connection failed after all attempts")
        return False

    def _create_connection(self) -> Optional[Any]:
        try:
            if IS_WINDOWS:
                if _HAS_PIPE_CONNECTION:
                    try:
                        import win32pipe, win32file
                        handle = win32file.CreateFile(
                            self._socket_path,
                            win32file.GENERIC_READ | win32file.GENERIC_WRITE,
                            0, None, win32file.OPEN_EXISTING,
                            win32file.FILE_FLAG_OVERLAPPED, None
                        )
                        return PipeConnectionWrapper(PipeConnection(handle), on_overflow=self._overflow_callback)
                    except ImportError:
                        log.debug("pywin32 not installed, falling back to _winapi")
                wrapper = _winapi_connect(self._socket_path, Config.IPC_CONNECT_TIMEOUT)
                if wrapper:
                    wrapper._on_overflow = self._overflow_callback
                return wrapper
            else:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(Config.IPC_CONNECT_TIMEOUT)
                sock.connect(self._socket_path)
                return sock
        except (FileNotFoundError, ConnectionRefusedError, TimeoutError) as e:
            log.debug(f"IPC connection error: {e}")
            return None
        except Exception as e:
            log.warning(f"Unexpected error creating IPC connection: {e}", exc_info=True)
            return None

    def _close_socket(self) -> None:
        with self._lock:
            if self._sock:
                with suppress(Exception):
                    self._sock.close()
                self._sock = None
            if self._sock_file:
                with suppress(Exception):
                    self._sock_file.close()
                self._sock_file = None
            self._connected.clear()
        with self._heartbeat_cond:
            self._heartbeat_cond.notify_all()

    def _reset_connection_state(self):
        with self._request_lock:
            for evt in self._pending_requests.values():
                evt.set()
            self._pending_requests.clear()

    def _receive_loop(self) -> None:
        with self._lock:
            sock = self._sock
        if not sock:
            return
        try:
            if self._sock_file is None:
                self._sock_file = sock.makefile('r')
            f = self._sock_file
        except Exception as e:
            log.error(f"Failed to create file wrapper: {e}")
            self._close_socket()
            return

        while not self._stop_event.is_set() and self._connected.is_set():
            try:
                line = f.readline()
                if not line:
                    break
                self._handle_message(line.strip())
            except (ConnectionResetError, BrokenPipeError, OSError) as e:
                log.debug(f"IPC recv error: {e}")
                break
            except Exception as e:
                log.exception("IPC recv unexpected in thread %s", threading.current_thread().name)
                break
        self._close_socket()
        self._reset_connection_state()

    def _handle_message(self, msg: str) -> None:
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            log.warning(f"Invalid JSON: {msg}")
            return
        try:
            if self._message_queue.full():
                try:
                    last = self._message_queue.get_nowait()
                    if self._is_duplicate_event(last, data):
                        pass
                    else:
                        self._message_queue.put_nowait(last)
                        log.debug("Event queue full, dropped non-duplicate message")
                        return
                except queue.Empty:
                    pass
            self._message_queue.put_nowait(data)
        except queue.Full:
            log.warning("Event queue full after optimization, dropping message")

    def _is_duplicate_event(self, msg1: Dict, msg2: Dict) -> bool:
        if "id" in msg1 and "id" in msg2 and "data" in msg1 and "data" in msg2:
            return msg1["id"] == msg2["id"] and msg1.get("event") == msg2.get("event")
        if "event" in msg1 and "event" in msg2:
            return msg1["event"] == msg2["event"] and "request_id" not in msg1
        return False

    def _apply_message(self, data: Dict[str, Any]) -> None:
        renderer = self._renderer_ref()
        if renderer is None:
            return
        try:
            if "request_id" in data:
                req_id = data["request_id"]
                with self._request_lock:
                    evt = self._pending_requests.pop(req_id, None)
                if evt:
                    if "error" in data and data["error"] != "success":
                        log.warning(f"Command error for request {req_id}: {data.get('error')}")
                    evt.set()
                return

            if "id" in data and "data" in data:
                pid = data["id"]
                value = data.get("data")
                try:
                    prop = ObservedProperty(pid)
                except ValueError:
                    return
                if prop == ObservedProperty.PAUSE:
                    renderer.state.update_pause(bool(value))
                elif prop == ObservedProperty.TIME_POS:
                    if value is not None:
                        renderer.state.update_position(float(value))
                elif prop == ObservedProperty.DURATION:
                    if value is not None:
                        renderer.state.update_duration(float(value))
                elif prop == ObservedProperty.VOLUME:
                    if value is not None:
                        renderer.state.update_volume(int(value))
                elif prop == ObservedProperty.MEDIA_TITLE:
                    if value is not None:
                        renderer.state.update_title(str(value))
                elif prop == ObservedProperty.METADATA:
                    if value is not None:
                        renderer.state.update_metadata(value)
                elif prop == ObservedProperty.CHAPTER_LIST:
                    if isinstance(value, list):
                        renderer.state.update_chapters(value)
                elif prop == ObservedProperty.CHAPTER:
                    if value is not None:
                        renderer.state.update_chapter(int(value))
                elif prop == ObservedProperty.PLAYLIST:
                    if isinstance(value, list):
                        renderer.state.update_playlist(value)
                elif prop == ObservedProperty.AID:
                    if value is not None:
                        renderer.state.update_aid(int(value))
                elif prop == ObservedProperty.SID:
                    if value is not None:
                        renderer.state.update_sid(int(value))
                elif prop == ObservedProperty.TRACK_LIST:
                    if isinstance(value, list):
                        renderer.state.update_track_list(value)
                return

            if "event" in data:
                event = data["event"]
                if event == "start-file":
                    renderer.state.mark_playing(True)
                    if renderer.protocol:
                        renderer._publish_event("renderer_av_uri", renderer.protocol.get_state_url())
                elif event == "end-file":
                    renderer.state.flush_position_forced()
                    renderer._handle_playback_end()
                elif event == "playback-restart":
                    renderer.state.flush_position_forced()
                elif event == "idle":
                    renderer.state.mark_playing(False)
                    renderer.set_state_stop()
        except Exception as e:
            log.exception("_apply_message failed for message: %s", data)

    def send_command(self, command: List[Any], timeout: float = None,
                     retry: int = 0, wait_response: bool = False) -> Optional[Dict]:
        if timeout is None:
            timeout = Config.IPC_COMMAND_TIMEOUT
        max_attempts = retry + 1
        request_id = None
        evt = None
        if wait_response:
            with self._request_lock:
                self._request_id += 1
                request_id = self._request_id
                evt = threading.Event()
                self._pending_requests[request_id] = evt
            command = ["request_id", request_id] + command

        for attempt in range(max_attempts):
            with self._lock:
                if self._sock is None:
                    if wait_response and request_id is not None:
                        with self._request_lock:
                            self._pending_requests.pop(request_id, None)
                    return None
                msg = json.dumps({"command": command}) + "\n"
                try:
                    self._sock.sendall(msg.encode('utf-8'))
                    if log.isEnabledFor(logging.DEBUG):
                        log.debug(f"Command sent: {command} (attempt {attempt+1})")
                except (BrokenPipeError, ConnectionResetError, OSError, TimeoutError) as e:
                    log.warning(f"IPC send failed (attempt {attempt+1}/{max_attempts}): {e}")
                    if wait_response and request_id is not None:
                        with self._request_lock:
                            self._pending_requests.pop(request_id, None)
                        if evt:
                            evt.set()
                    self._close_socket()
                    self._connected.clear()
                    if attempt < max_attempts - 1:
                        time.sleep(Config.IPC_COMMAND_RETRY_DELAY)
                    continue
                except Exception as e:
                    log.error(f"Unexpected send error: {e}")
                    if wait_response and request_id is not None:
                        with self._request_lock:
                            self._pending_requests.pop(request_id, None)
                        if evt:
                            evt.set()
                    return None

            if wait_response and request_id is not None and evt:
                if evt.wait(timeout=timeout):
                    with self._request_lock:
                        self._pending_requests.pop(request_id, None)
                    return {}
                else:
                    log.warning(f"Command {command} did not get response within {timeout}s")
                    with self._request_lock:
                        self._pending_requests.pop(request_id, None)
                    return None
            else:
                return {}
        return None

    def _observe_properties(self) -> None:
        # v0.27: 使用正确的 MPV 属性名
        for prop in ObservedProperty:
            self.send_command(["observe_property", prop.value, prop.mpv_name], wait_response=False)

    def is_connected(self) -> bool:
        return self._connected.is_set() and self._sock is not None

# ----------------------------------------------------------------------
# 主渲染器类 (v0.27 线程池关闭增强)
# ----------------------------------------------------------------------
class MPVplayerRenderer(Renderer):
    def __init__(self) -> None:
        super().__init__()
        Config.load()

        self._executor = ThreadPoolExecutor(max_workers=6, thread_name_prefix="MPV")
        self._lock = threading.RLock()
        self._stop_event = threading.Event()
        self._cleanup_lock = threading.Lock()
        self._cleanup_complete = threading.Event()
        self._cleanup_complete.set()
        self._state_enum = RendererState.IDLE
        self._start_lock = threading.Lock()
        self._start_lock_holder: Optional[int] = None

        self.proc_manager = MPVProcessManager(self._executor)
        self.proc_manager.set_exit_callback(self._on_process_exit)

        self.temp_manager = TempFileManager()
        self.temp_manager.create()

        self.ipc_client: Optional[MpvIpcClient] = None
        self.state = PlaybackState(on_state_change=self._on_playback_state_change)
        self._subtitle_file: Optional[str] = None
        self._subtitle_mtime: Optional[float] = None
        self._subtitle_original_path: Optional[str] = None
        self._playlist_file: Optional[str] = None
        self.ipc_sock: Optional[str] = None

        self._resume_manager = ResumeManager(Config.RESUME_DATA_PATH)
        self._resume_save_future: Optional[Future] = None
        self._resume_save_stop = threading.Event()

        self._event_queue: queue.Queue = queue.Queue(maxsize=Config.EVENT_QUEUE_MAXSIZE)
        self._event_future = self._executor.submit(self._event_publisher_loop)

        atexit.register(self._atexit_cleanup)
        self._start_resume_saving()

    _cleanup_scheduled = False

    def _on_process_exit(self):
        if not self._stop_event.is_set():
            with self._lock:
                if self._cleanup_scheduled:
                    return
                self._cleanup_scheduled = True
            self._schedule_cleanup()

    # v0.27: 线程池关闭超时控制
    def _shutdown_executor(self, timeout: float = 5.0):
        try:
            self._executor.shutdown(wait=True, timeout=timeout)
            log.debug("Thread pool shut down gracefully")
        except Exception as e:
            log.error(f"Error shutting down executor: {e}")
            # 强制关闭
            self._executor.shutdown(wait=False)

    def _start_resume_saving(self):
        def saver():
            while not self._resume_save_stop.is_set():
                self._resume_save_stop.wait(timeout=Config.RESUME_SAVE_INTERVAL)
                if self._resume_save_stop.is_set():
                    break
                self._save_resume_periodic()
        self._resume_save_future = self._executor.submit(saver)

    def _save_resume_periodic(self):
        if not Config.RESUME_ENABLED or self.state.is_current_playlist():
            return
        uri = self.state.get_current_uri()
        pos = self.state.get_position()
        if uri and pos > 0:
            self._resume_manager.save_position(uri, pos, is_playlist=False)
            log.debug(f"Periodic resume save: {_sanitize_url(uri)} @ {pos}s")

    def _event_publisher_loop(self):
        log.debug("Event publisher loop started")
        while True:
            try:
                topic, args = self._event_queue.get(timeout=1)
                if topic is None:
                    log.debug("Event publisher loop exiting")
                    break
                cherrypy.engine.publish(topic, *args)
            except queue.Empty:
                continue
            except Exception as e:
                log.exception(f"Error publishing event {topic}: {e}")

    CRITICAL_EVENTS = {"renderer_av_stop", "app_notify"}

    def _publish_event(self, topic: str, *args) -> None:
        try:
            if topic in self.CRITICAL_EVENTS:
                try:
                    self._event_queue.put((topic, args), timeout=0.5)
                except queue.Full:
                    log.warning(f"Event queue full for critical event {topic}, publishing directly")
                    cherrypy.engine.publish(topic, *args)
            else:
                try:
                    self._event_queue.put_nowait((topic, args))
                except queue.Full:
                    log.warning(f"Event queue full, dropping non-critical event {topic}")
        except Exception as e:
            log.error(f"Error queuing event {topic}: {e}")

        qsize = self._event_queue.qsize()
        if qsize > Config.EVENT_QUEUE_MAXSIZE * 0.8:
            log.warning(f"Event queue nearly full: {qsize}/{Config.EVENT_QUEUE_MAXSIZE}")

    def stop(self) -> None:
        super().stop()
        self._set_state_enum(RendererState.STOPPED)
        self._stop_event.set()
        self._resume_save_stop.set()
        if not self._cleanup_done():
            self._do_cleanup()
        self._event_queue.put((None, ()))
        self._shutdown_executor(timeout=5.0)
        self.temp_manager.cleanup(force=True)
        atexit.unregister(self._atexit_cleanup)
        log.info("MPVPlayer stopped")

    def _set_state_enum(self, new_state: RendererState) -> None:
        with self._lock:
            if self._state_enum != new_state:
                old = self._state_enum
                self._state_enum = new_state
                log.debug(f"State changed: {old} -> {new_state}")
                if self.ipc_client:
                    if new_state in (RendererState.IDLE, RendererState.STOPPED, RendererState.CLEANING):
                        self.ipc_client.pause_heartbeat()
                    elif new_state == RendererState.RUNNING:
                        self.ipc_client.resume_heartbeat()

    def _get_state_enum(self) -> RendererState:
        with self._lock:
            return self._state_enum

    def _sync_dlna_state(self, key: str, value: Any):
        if key == 'pause':
            if value:
                self.set_state_pause()
            else:
                self.set_state_play()
        elif key == 'volume':
            self.set_state_volume(value)
        elif key == 'duration':
            self.set_state_duration(_format_time(value))
        elif key == 'position':
            self.set_state_position(_format_time(value))
        elif key == 'title':
            self._publish_event("renderer_av_title", value)
        elif key == 'metadata':
            pass
        elif key == 'chapters':
            self._publish_event("renderer_av_chapters", value)
        elif key == 'chapter':
            self._publish_event("renderer_av_chapter", value)
        elif key == 'playlist':
            self._publish_event("renderer_av_playlist", value)
        elif key == 'aid':
            self._publish_event("renderer_av_aid", value)
        elif key == 'sid':
            self._publish_event("renderer_av_sid", value)
        elif key == 'track_list':
            self._publish_event("renderer_av_track_list", value)

    def _on_playback_state_change(self, key: str, value: Any):
        self._sync_dlna_state(key, value)

    def _cleanup_done(self) -> bool:
        return self._cleanup_complete.is_set()

    def _do_cleanup(self) -> None:
        if not self._cleanup_lock.acquire(blocking=False):
            return
        self._cleanup_complete.clear()
        with self._lock:
            self._cleanup_scheduled = False
        future = self._executor.submit(self._cleanup_task)
        try:
            future.result(timeout=Config.CLEANUP_TOTAL_TIMEOUT)
        except FutureTimeoutError:
            log.error("Cleanup timed out, forcing process kill")
            self.proc_manager.kill()
        except Exception as e:
            log.exception("Cleanup task failed")
        finally:
            self._cleanup_complete.set()
            self._cleanup_lock.release()

    def _cleanup_task(self):
        try:
            log.info("Cleanup started")
            if Config.RESUME_ENABLED and not self.state.is_current_playlist():
                uri = self.state.get_current_uri()
                pos = self.state.get_position()
                if uri and pos > 0:
                    self._resume_manager.save_position(uri, pos, is_playlist=False)
                    log.info(f"Cleanup: saved resume position for {_sanitize_url(uri)} @ {pos}s")
            self._set_state_enum(RendererState.CLEANING)
            self._stop_event.set()
            if self.ipc_client and self.ipc_client.is_connected():
                self.ipc_client.send_command(["sub_remove", "all"], timeout=0.5, wait_response=False)
                self.ipc_client.send_command(["playlist-clear"], timeout=0.5, wait_response=False)
            if self.ipc_client and self.ipc_client.is_connected():
                self.ipc_client.send_command(["quit"], wait_response=False, timeout=0.5)
            time.sleep(0.1)
            if self.proc_manager.is_alive():
                try:
                    self.proc_manager.process.wait(timeout=Config.CLEANUP_QUIT_TIMEOUT)
                except subprocess.TimeoutExpired:
                    pass
            if self.ipc_client:
                self.ipc_client.stop()
                self.ipc_client = None
            self.proc_manager.kill()
            self._remove_socket_file()
            self.temp_manager.cleanup(force=False)
            self.state.reset()
            self.set_state_transport("STOPPED")
            self._publish_event("renderer_av_stop")
            log.info("Resource cleanup completed")
            self._set_state_enum(RendererState.IDLE)
        except Exception:
            log.exception("Error during cleanup task")
            raise

    def _remove_socket_file(self) -> None:
        if not IS_WINDOWS and self.ipc_sock:
            with suppress(Exception):
                os.unlink(self.ipc_sock)
                log.debug(f"Removed IPC socket: {self.ipc_sock}")
            self.ipc_sock = None

    def _schedule_cleanup(self) -> None:
        if self._cleanup_done() and self._get_state_enum() != RendererState.CLEANING:
            self._executor.submit(self._do_cleanup)

    def _wait_cleanup_finish(self, timeout: float = 5.0) -> None:
        self._cleanup_complete.wait(timeout=timeout)

    def _handle_playback_end(self) -> None:
        self.state.mark_playing(False)
        self._publish_event("renderer_av_stop")
        if not self.state.is_current_playlist():
            uri = self.state.get_current_uri()
            pos = self.state.get_position()
            if uri and pos > 0:
                self._resume_manager.save_position(uri, pos, is_playlist=False)
                log.info(f"Saved resume position for {_sanitize_url(uri)}: {pos}s")
        else:
            log.debug("Skipping resume position save for playlist")

    def _get_resume_position(self, uri: str) -> Optional[float]:
        return self._resume_manager.get_position(uri, is_playlist=False)

    def _wait_ipc_connected(self, timeout: float) -> bool:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.ipc_client and self.ipc_client.is_connected():
                return True
            time.sleep(0.1)
        return False

    def _generate_socket_path(self) -> str:
        temp_dir = self.temp_manager.create()
        if IS_WINDOWS:
            return r"\\.\pipe\macast_mpv_{}_{}".format(os.getpid(), uuid.uuid4().hex)
        else:
            return os.path.join(temp_dir, f"mpv_{os.getpid()}.sock")

    def _prepare_command(self, media: str, is_playlist: bool, audio_only: bool) -> List[str]:
        mpv_path = MpvFinder.find()
        if not mpv_path:
            raise RuntimeError("MPV executable not found")
        cmd = [mpv_path, "--fullscreen", f"--input-ipc-server={self.ipc_sock}",
               "--cache=yes", "--keep-open=yes", "--no-terminal"]
        if Config.EXTRA_ARGS:
            cmd.extend(Config.EXTRA_ARGS)
        if audio_only:
            if "--no-video" not in cmd:
                cmd.append("--no-video")
        elif not is_playlist and os.path.isfile(media):
            ext = os.path.splitext(media)[1].lower()
            if ext in Config.AUDIO_ONLY_EXTENSIONS:
                if "--no-video" not in cmd:
                    cmd.append("--no-video")
        if is_playlist:
            cmd.append(f"--playlist={media}")
        else:
            cmd.append(media)
        if self._subtitle_file and os.path.exists(self._subtitle_file):
            cmd.append(f"--sub-file={self._subtitle_file}")
        return cmd

    def _launch_mpv(self, media: str, is_playlist: bool = False, start: int = 0, audio_only: bool = False) -> bool:
        if self._start_lock_holder == threading.get_ident():
            log.critical("Recursive _launch_mpv call detected, forcing release")
            self._start_lock.release()
            self._start_lock_holder = None
        timeout = Config.START_LOCK_TIMEOUT
        if not self._start_lock.acquire(timeout=timeout):
            log.warning("Timeout waiting for previous launch, forcing cleanup")
            self._do_cleanup()
            self._wait_cleanup_finish(timeout=10.0)
            if not self._start_lock.acquire(timeout=5.0):
                self._publish_event("app_notify", "Error", "Cannot start: system busy, please retry later.")
                return False
        try:
            self._start_lock_holder = threading.get_ident()
            log.debug("_launch_mpv acquired start lock")
            Config.reload_if_changed()
            mpv_path = MpvFinder.find()
            if not mpv_path:
                if IS_WINDOWS:
                    subprocess.Popen(["notepad.exe", Setting.setting_path],
                                     creationflags=subprocess.CREATE_NO_WINDOW)
                self._publish_event("app_notify", "Error",
                                    "MPV not found. Please set 'MPVplayer_Path' in config.")
                return False
            if not is_playlist and start == 0:
                saved_pos = self._get_resume_position(media)
                if saved_pos and saved_pos > 0:
                    start = int(saved_pos)
                    log.info(f"Resuming from {start}s for {_sanitize_url(media)}")
            self.state.set_current_media_info(media, is_playlist, start, audio_only)
            proc_alive = self.proc_manager.is_alive()
            if proc_alive and self.ipc_client and self.ipc_client.is_connected():
                cmd = ["loadlist", media, "replace"] if is_playlist else ["loadfile", media, "replace"]
                if not is_playlist and start > 0:
                    cmd.append(f"start={start}")
                log.info(f"Sending seamless switch: {cmd} (media={_sanitize_url(media)})")
                if self.ipc_client.send_command(cmd, timeout=Config.SEAMLESS_SWITCH_TIMEOUT,
                                                retry=Config.IPC_COMMAND_RETRY, wait_response=False) is not None:
                    log.info(f"Seamless switch to {_sanitize_url(media)} sent successfully")
                    if is_playlist and start > 0:
                        self.ipc_client.send_command(["seek", start, "absolute"], wait_response=False)
                    self.set_state_transport("PLAYING")
                    self._publish_event("renderer_av_uri", media)
                    return True
                else:
                    log.warning("IPC switch command failed, will restart MPV")
            if not self._cleanup_done():
                self._schedule_cleanup()
                self._wait_cleanup_finish(timeout=5.0)
            self._stop_event.clear()
            self.ipc_sock = self._generate_socket_path()
            try:
                cmd = self._prepare_command(media, is_playlist, audio_only)
                log.info(f"Starting MPV (full restart): {' '.join(cmd)}")
                debug = Setting.get(SettingProperty.MPVplayer_Debug, False)
                if not self.proc_manager.start(cmd, debug):
                    return False
                self.ipc_client = MpvIpcClient(self, self.ipc_sock, self._executor)
                self.ipc_client.start()
                if not self._wait_ipc_connected(3.0):
                    log.error("IPC connection timeout after process start")
                    self.proc_manager.kill()
                    return False
                self.set_state_transport("PLAYING")
                self._set_state_enum(RendererState.RUNNING)
                return True
            except Exception as e:
                log.exception(f"Failed to prepare command: {e}")
                self._publish_event("app_notify", "Error", str(e))
                return False
        finally:
            self._start_lock_holder = None
            log.debug("_launch_mpv releasing start lock")
            self._start_lock.release()

    def set_subtitle(self, file_path: Optional[str] = None) -> None:
        old_sub = self._subtitle_file
        new_sub = None
        if file_path and os.path.isfile(file_path):
            new_mtime = os.path.getmtime(file_path)
            if (old_sub and 
                self._subtitle_original_path == file_path and 
                self._subtitle_mtime == new_mtime):
                log.debug("Subtitle file unchanged (same path and mtime), skipping")
                return
            try:
                copied_path = self.temp_manager.copy_file_to_temp(file_path, suffix=".srt")
                new_sub = copied_path
                self._subtitle_original_path = file_path
                self._subtitle_mtime = new_mtime
                log.info(f"Subtitle copied to temp: {copied_path}")
            except Exception as e:
                log.error(f"Failed to copy subtitle: {e}")
                self._publish_event("app_notify", "Error", f"Subtitle copy failed: {e}")
                return
        else:
            log.info("Subtitle removed for future playback")
            self._subtitle_original_path = None
            self._subtitle_mtime = None

        if old_sub:
            self.temp_manager.delete_file(old_sub)
        self._subtitle_file = new_sub

        if self.ipc_client and self.ipc_client.is_connected() and self.proc_manager.is_alive():
            if self._subtitle_file and os.path.exists(self._subtitle_file):
                self.ipc_client.send_command(["sub_remove", "all"], wait_response=False)
                if self.ipc_client.send_command(["sub_add", self._subtitle_file], wait_response=False) is not None:
                    log.info(f"Subtitle added dynamically: {self._subtitle_file}")
                else:
                    log.error(f"Failed to add subtitle: {self._subtitle_file}")
            else:
                self.ipc_client.send_command(["sub_remove", "all"], wait_response=False)
                log.info("All subtitles removed dynamically")

    # DLNA control methods
    def set_media_stop(self) -> None:
        if not self._cleanup_done():
            self._schedule_cleanup()
        else:
            log.debug("Already stopped")

    def set_media_pause(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "pause", True], wait_response=False)

    def set_media_resume(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "pause", False], wait_response=False)

    def set_media_volume(self, data: Union[int, str]) -> None:
        vol = int(data)
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "volume", vol], wait_response=False)

    def set_media_position(self, data: str) -> None:
        try:
            if ':' in data:
                parts = data.split(':')
                if len(parts) == 3:
                    seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                elif len(parts) == 2:
                    seconds = int(parts[0]) * 60 + int(parts[1])
                else:
                    seconds = float(data)
            else:
                seconds = float(data)
            if self.ipc_client:
                self.ipc_client.send_command(["seek", seconds, "absolute"], wait_response=False)
        except ValueError:
            log.error(f"Invalid position format: {data}")

    def set_media_playlist(self, urls: List[str], start: int = 0) -> None:
        if not urls:
            log.warning("Empty playlist, ignoring")
            return
        if self._playlist_file:
            self.temp_manager.delete_file(self._playlist_file)
            self._playlist_file = None
        if not self._cleanup_done():
            self._schedule_cleanup()
            self._wait_cleanup_finish()
        playlist_content = "#EXTM3U\n" + "\n".join(urls)
        try:
            playlist_path = self.temp_manager.create_temp_file(playlist_content, suffix=".m3u")
            self._playlist_file = playlist_path
            log.info(f"Playlist created: {playlist_path} ({len(urls)} items)")
        except Exception as e:
            log.error(f"Failed to create playlist file: {e}")
            self._publish_event("app_notify", "Error", f"Playlist creation failed: {e}")
            return
        self._launch_mpv(playlist_path, is_playlist=True, start=start)

    def set_media_url(self, url: str, start: int = 0, audio_only: bool = False) -> None:
        if not self._cleanup_done():
            self._schedule_cleanup()
            self._wait_cleanup_finish()
        self._launch_mpv(url, is_playlist=False, start=start, audio_only=audio_only)

    def set_media_speed(self, data: Union[float, str]) -> None:
        speed = float(data)
        if Config.MIN_SPEED <= speed <= Config.MAX_SPEED:
            if self.ipc_client:
                self.ipc_client.send_command(["set_property", "speed", speed], wait_response=False)
        else:
            log.warning(f"Speed {speed} out of range [{Config.MIN_SPEED}, {Config.MAX_SPEED}]")

    def set_media_mute(self, data: bool) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "mute", "yes" if data else "no"], wait_response=False)

    def set_media_next(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-next"], wait_response=False)

    def set_media_prev(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-prev"], wait_response=False)

    def set_media_fullscreen(self, data: bool) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "fullscreen", "yes" if data else "no"], wait_response=False)

    def cycle_audio_track(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["cycle", "audio"], wait_response=False)

    def cycle_subtitle_track(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["cycle", "sub"], wait_response=False)

    def set_audio_track_by_id(self, track_id: int) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "aid", track_id], wait_response=False)

    def set_subtitle_track_by_id(self, track_id: int) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "sid", track_id], wait_response=False)

    def set_media_chapter(self, index: int) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set", "chapter", index], wait_response=False)
            log.info(f"Jumped to chapter {index}")

    def next_chapter(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["add", "chapter", 1], wait_response=False)

    def prev_chapter(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["add", "chapter", -1], wait_response=False)

    def set_media_osd(self, text: str, duration: int = None) -> None:
        if duration is None:
            duration = Config.OSD_DEFAULT_DURATION
        if self.ipc_client:
            self.ipc_client.send_command(["show-text", text, duration], wait_response=False)
            log.info(f"OSD: {text} ({duration}ms)")

    def clear_resume_position(self, uri: str = None) -> None:
        if uri is None:
            uri = self.state.get_current_uri()
        if uri:
            self._resume_manager.remove(uri)
            log.info(f"Cleared resume position for {_sanitize_url(uri)}")

    def get_playlist(self) -> List[Dict[str, Any]]:
        return self.state.get_playlist()

    def playlist_clear(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-clear"], wait_response=False)
            log.info("Playlist cleared")

    def playlist_remove_index(self, index: int) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-remove", index], wait_response=False)
            log.info(f"Removed playlist index {index}")

    def playlist_move(self, index1: int, index2: int) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-move", index1, index2], wait_response=False)
            log.info(f"Moved playlist item from {index1} to {index2}")

    def playlist_shuffle(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-shuffle"], wait_response=False)
            log.info("Playlist shuffled")

    def get_audio_track_id(self) -> int:
        return self.state.get_aid()

    def get_subtitle_track_id(self) -> int:
        return self.state.get_sid()

    def get_track_list(self) -> List[Dict[str, Any]]:
        return self.state.get_track_list()

    def get_chapters(self) -> List[Dict[str, Any]]:
        return self.state.get_chapters()

    def start(self) -> None:
        super().start()
        self._set_state_enum(RendererState.RUNNING)
        log.info("MPVPlayer started")

    def _atexit_cleanup(self) -> None:
        if not self._cleanup_done():
            self._do_cleanup()

if __name__ == "__main__":
    gui(MPVplayerRenderer())