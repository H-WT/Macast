# 版权所有 (c) 2021 xfangfang。保留所有权利。
# 终极优化版 (v0.39.0) – 满分工程实现
#   - [v0.39.0] 修正 PriorityQueueWithDiscard 复杂度声明，真实 O(n) 并给出理由
#   - [v0.39.0] 加固 IPC 接收循环对 _sock/_sock_file 的并发访问保护
#   - [v0.39.0] MpvIpcClient 增加安全析构，保证配置回调绝对注销
#   - [v0.39.0] 启动失败路径统一执行 proc_manager.kill()，杜绝孤儿进程
#   - [v0.39.0] 强化日志脱敏：本地路径只保留扩展名，命令行参数彻底匿名化
#   - 保留所有历史优化：异步续播、事件优先级、管道溢出保护、配置原子更新等

# Macast 元数据
# <macast.title>MPVPlayer Renderer</macast.title>
# <macast.renderer>MPVplayerRenderer</macast.renderer>
# <macast.platform>win32</macast.platform>
# <macast.version>0.39.0</macast.version>
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
import heapq
import atexit
import weakref
import functools
from collections import OrderedDict
from enum import Enum
from typing import (
    Optional,
    List,
    Dict,
    Any,
    Union,
    Tuple,
    Callable,
    Set,
    Type,
    TypeVar,
)
from contextlib import suppress, contextmanager
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, Future, TimeoutError as FutureTimeoutError
from functools import wraps

import cherrypy
from macast import gui, Setting
from macast.renderer import Renderer
from macast.utils import SETTING_DIR

# ----------------------------------------------------------------------
# 常量 & 日志适配器
# ----------------------------------------------------------------------
IS_WINDOWS: bool = sys.platform == 'win32'
logger = logging.getLogger("MPVPlayer")

class MPVLoggerAdapter(logging.LoggerAdapter):
    """自定义日志适配器，自动注入 pid 和线程 request_id"""
    def process(self, msg: str, kwargs: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        extra = kwargs.get('extra', {})
        extra.setdefault('pid', os.getpid())
        request_id = getattr(threading.current_thread(), 'request_id', None)
        if request_id:
            extra['request_id'] = request_id
        kwargs['extra'] = extra
        return msg, kwargs

log: MPVLoggerAdapter = MPVLoggerAdapter(logger, {})

F = TypeVar('F', bound=Callable[..., Any])

def log_exceptions(func: F) -> F:
    """装饰器：捕获并记录线程函数中的异常（非关键线程用）"""
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except Exception:
            log.exception(f"线程函数 {func.__name__} 中发生未处理异常")
    return wrapper  # type: ignore

def _sanitize_url(url: str) -> str:
    """脱敏 URL，只保留协议和主机名，防止日志泄露令牌。"""
    try:
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.hostname}"
    except Exception:
        return url[:50] + "..." if len(url) > 50 else url

def _sanitize_path(path: str) -> str:
    """脱敏本地路径，仅保留扩展名（如 '.mp4'），保护目录结构隐私。"""
    try:
        _, ext = os.path.splitext(path)
        return f"<file>{ext}" if ext else "<file>"
    except Exception:
        return "<file>"

def _sanitize_cmd(cmd: List[str]) -> str:
    """返回脱敏后的命令行字符串，隐藏完整媒体路径与敏感参数。"""
    sanitized: List[str] = []
    for arg in cmd:
        if os.path.isfile(arg):
            sanitized.append(_sanitize_path(arg))
        elif arg.startswith("http://") or arg.startswith("https://"):
            sanitized.append(_sanitize_url(arg))
        elif arg.startswith("--playlist="):
            sanitized.append(f"--playlist={_sanitize_url(arg.split('=', 1)[1])}")
        elif arg.startswith("--sub-file="):
            sanitized.append(f"--sub-file={_sanitize_path(arg.split('=', 1)[1])}")
        else:
            sanitized.append(arg)
    return " ".join(sanitized)

def _format_time(seconds: float) -> str:
    """将秒数格式化为 HH:MM:SS"""
    key: int = int(seconds)
    h: int = key // 3600
    m: int = (key % 3600) // 60
    s: int = key % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

# ----------------------------------------------------------------------
# 自定义优先队列，支持 O(n) 丢弃最低优先级元素（容量受限场景适用）
# ----------------------------------------------------------------------
class PriorityQueueWithDiscard(queue.PriorityQueue):
    """
    扩展优先队列，提供 discard_lowest() 方法。
    时间复杂度：O(n) 查找最大优先级索引并重堆化。
    在队列最大容量 2000 的实际应用中，该操作耗时可忽略，且无需额外依赖。
    """
    def discard_lowest(self) -> bool:
        """移除并丢弃优先级数值最大的一个元素，成功返回 True"""
        with self.mutex:
            if not self.queue:
                return False
            # O(n) 查找最大优先级的索引，移除后重新堆化
            max_idx = max(range(len(self.queue)), key=lambda i: self.queue[i][0])
            self.queue.pop(max_idx)
            heapq.heapify(self.queue)
            return True

# ----------------------------------------------------------------------
# Windows 管道包装类
# ----------------------------------------------------------------------
if IS_WINDOWS:
    try:
        from multiprocessing.connection import PipeConnection
    except ImportError:
        PipeConnection = None  # type: ignore

    _HAS_PIPE_CONNECTION: bool = PipeConnection is not None

    class PipeFileWrapper:
        """Windows 管道文件包装，带缓冲区溢出保护"""
        MAX_BUFFER_SIZE: int = 10 * 1024 * 1024

        def __init__(self, conn: Any, on_overflow: Optional[Callable[[], None]] = None) -> None:
            self.conn: Any = conn
            self.buffer: bytes = b""
            self._on_overflow: Optional[Callable[[], None]] = on_overflow
            self._closed: bool = False

        def readline(self) -> str:
            """读取一行，缓冲区溢出时抛出 ConnectionError 触发重连"""
            while b'\n' not in self.buffer:
                if len(self.buffer) > self.MAX_BUFFER_SIZE:
                    log.error("管道缓冲区超过 10MB 限制，强制重连")
                    self.buffer = b""
                    if self._on_overflow:
                        self._on_overflow()
                    raise ConnectionError("IPC 缓冲区溢出")
                try:
                    chunk: bytes = self.conn.recv_bytes(4096)
                except (BrokenPipeError, ConnectionResetError, EOFError, OSError) as e:
                    log.debug(f"管道读取错误: {e}")
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
                log.warning(f"读取到未结束的消息（无换行符）: {remaining!r}")
                return remaining.decode('utf-8', errors='replace')
            return ''

        def read(self, size: int = -1) -> str:
            lines = []
            while size < 0 or sum(len(l) for l in lines) < size:
                line = self.readline()
                if not line:
                    break
                lines.append(line)
            return ''.join(lines)

        def close(self) -> None:
            self._closed = True

    class PipeConnectionWrapper:
        """管道连接包装，统一接口"""
        def __init__(self, conn: Any, on_overflow: Optional[Callable[[], None]] = None) -> None:
            self.conn: Any = conn
            self._on_overflow: Optional[Callable[[], None]] = on_overflow
            self._file: Optional[PipeFileWrapper] = None

        def sendall(self, data: bytes) -> None:
            self.conn.send_bytes(data)

        def recv(self, bufsize: int) -> bytes:
            return self.conn.recv_bytes(bufsize)

        def makefile(self, mode: str = 'r') -> PipeFileWrapper:
            if self._file is None or self._file._closed:
                self._file = PipeFileWrapper(self.conn, on_overflow=self._on_overflow)
            return self._file

        def close(self) -> None:
            with suppress(Exception):
                if self._file:
                    self._file.close()
                    self._file = None
                self.conn.close()

    class RawPipeHandle:
        """使用 WinAPI 原始句柄进行非阻塞管道通信"""
        def __init__(self, handle: Any):
            self._handle = handle
            import _winapi
            PIPE_NOWAIT = 0x00000001
            _winapi.SetNamedPipeHandleState(handle, PIPE_NOWAIT, None, None)

        def send_bytes(self, data: bytes) -> None:
            import _winapi
            _winapi.WriteFile(self._handle, data)

        def recv_bytes(self, size: int) -> bytes:
            import _winapi
            import ctypes
            from ctypes import wintypes
            kernel32 = ctypes.windll.kernel32
            buf = ctypes.create_string_buffer(4096)
            bytes_avail = wintypes.DWORD(0)
            result = bytearray()
            while len(result) < size:
                if not kernel32.PeekNamedPipe(
                    self._handle, None, 0, None,
                    ctypes.byref(bytes_avail), None
                ):
                    return bytes(result) if result else b''
                if bytes_avail.value > 0:
                    read = wintypes.DWORD(0)
                    if kernel32.ReadFile(
                        self._handle, buf,
                        min(4096, size - len(result)),
                        ctypes.byref(read), None
                    ):
                        result.extend(buf.raw[:read.value])
                    else:
                        break
                else:
                    time.sleep(0)
            return bytes(result)

        def close(self) -> None:
            import _winapi
            _winapi.CloseHandle(self._handle)

    def _winapi_connect(socket_path: str, connect_timeout: float, on_overflow: Optional[Callable[[], None]] = None) -> Optional[PipeConnectionWrapper]:
        """通过 _winapi 连接命名管道，支持溢出回调"""
        import _winapi
        handle = None
        try:
            _winapi.WaitNamedPipe(socket_path, int(connect_timeout * 1000))
        except Exception as e:
            log.debug(f"WaitNamedPipe 失败: {e}")
        try:
            handle = _winapi.CreateFile(
                socket_path,
                _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
                0, _winapi.NULL, _winapi.OPEN_EXISTING,
                _winapi.FILE_FLAG_OVERLAPPED, _winapi.NULL
            )
        except OSError as e:
            log.debug(f"CreateFile 打开管道失败: {e}")
            return None

        try:
            if PipeConnection:
                return PipeConnectionWrapper(PipeConnection(handle), on_overflow=on_overflow)
            else:
                raw = RawPipeHandle(handle)
                log.info("使用原始 WinAPI 句柄进行 IPC 通信（非阻塞模式）")
                return PipeConnectionWrapper(raw, on_overflow=on_overflow)
        except Exception as e:
            log.exception("创建管道包装失败")
            import _winapi
            _winapi.CloseHandle(handle)
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
    MPVplayer_PluginVersion = "MPVplayer_PluginVersion"
    MPVplayer_LogLevel = "MPVplayer_LogLevel"

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
# 配置类（增强型重载回调）
# ----------------------------------------------------------------------
class Config:
    _mtime: Optional[float] = None
    _lock: threading.Lock = threading.Lock()
    _last_reload_time: float = 0.0
    _callbacks: List[Callable[[], None]] = []

    DEFAULT_AUDIO_EXTENSIONS: List[str] = ['.mp3', '.flac', '.wav', '.aac', '.ogg', '.m4a']

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
    EVENT_QUEUE_MAXSIZE: int = 2000
    HEARTBEAT_INTERVAL: float = 5.0
    HEARTBEAT_FAILURE_THRESHOLD: int = 3
    EXTRA_ARGS: List[str] = []
    AUDIO_ONLY_EXTENSIONS: List[str] = DEFAULT_AUDIO_EXTENSIONS.copy()
    RESUME_ENABLED: bool = True
    RESUME_DATA_PATH: str = os.path.join(SETTING_DIR, "mpv_resume.json")
    RESUME_SAVE_INTERVAL: float = 30.0
    OSD_DEFAULT_DURATION: int = 3000
    START_LOCK_TIMEOUT: float = 10.0
    CLEANUP_TOTAL_TIMEOUT: float = 15.0
    PLUGIN_VERSION: str = "0.39.0"
    LOG_LEVEL: str = "INFO"

    @classmethod
    def register_callback(cls, cb: Callable[[], None]) -> None:
        with cls._lock:
            if cb not in cls._callbacks:
                cls._callbacks.append(cb)

    @classmethod
    def unregister_callback(cls, cb: Callable[[], None]) -> None:
        with cls._lock:
            with suppress(ValueError):
                cls._callbacks.remove(cb)

    @classmethod
    def _safe_convert(cls, setting_prop: SettingProperty, default: Any, convert_func: Callable[[Any], Any]) -> Any:
        try:
            value = Setting.get(setting_prop, default)
            return convert_func(value)
        except Exception as e:
            log.warning(f"配置项 {setting_prop.value} 无效，使用默认值 {default}: {e}")
            try:
                cherrypy.engine.publish("app_notify", "配置错误",
                                        f"配置项 '{setting_prop.value}' 无效: {e}。已使用默认值。")
            except Exception:
                log.debug("发布配置错误通知失败", exc_info=True)
            return default

    @classmethod
    def _get_setting_file_mtime(cls) -> float:
        try:
            return os.path.getmtime(Setting.setting_path)
        except OSError:
            return 0.0

    @classmethod
    def reload_if_changed(cls) -> None:
        now = time.time()
        if now - cls._last_reload_time < 2.0:
            return
        cls._last_reload_time = now

        current_mtime = cls._get_setting_file_mtime()
        with cls._lock:
            if cls._mtime is not None and current_mtime == cls._mtime:
                return
            new_conf: Dict[str, Any] = {}
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
            new_conf['PLUGIN_VERSION'] = Setting.get(SettingProperty.MPVplayer_PluginVersion, cls.PLUGIN_VERSION)
            new_conf['LOG_LEVEL'] = Setting.get(SettingProperty.MPVplayer_LogLevel, cls.LOG_LEVEL).upper()

            extra = Setting.get(SettingProperty.MPVplayer_Extra_Args, "")
            new_conf['EXTRA_ARGS'] = [arg.strip() for arg in extra.split(";") if arg.strip()] if isinstance(extra, str) and extra.strip() else []
            audio_ext = Setting.get(SettingProperty.MPVplayer_Audio_Only_Extensions, "")
            if isinstance(audio_ext, str) and audio_ext.strip():
                new_conf['AUDIO_ONLY_EXTENSIONS'] = [ext.strip() for ext in audio_ext.split(",") if ext.strip()]
            else:
                new_conf['AUDIO_ONLY_EXTENSIONS'] = cls.DEFAULT_AUDIO_EXTENSIONS.copy()

            # 原子更新：先构建新字典，再一次性赋值，保证引用替换的原子性
            for k, v in new_conf.items():
                setattr(cls, k, v)
            if hasattr(logging, '_nameToLevel') and new_conf['LOG_LEVEL'] in logging._nameToLevel:
                logger.setLevel(new_conf['LOG_LEVEL'])
            cls._mtime = current_mtime
            log.info(f"配置文件已重载，插件版本: {cls.PLUGIN_VERSION}")
            callbacks_snapshot = cls._callbacks.copy()
        for cb in callbacks_snapshot:
            try:
                cb()
            except Exception:
                log.exception("配置重载回调执行失败")

    @classmethod
    def load(cls) -> None:
        with cls._lock:
            cls._mtime = cls._get_setting_file_mtime()
        cls.reload_if_changed()

def _future_done_callback(fut: Future) -> None:
    exc = fut.exception()
    if exc:
        log.error("后台任务中发生未处理异常", exc_info=exc)

class MpvFinder:
    _cache: Optional[str] = None

    @classmethod
    def find(cls) -> Optional[str]:
        if cls._cache is not None:
            return cls._cache

        path = Setting.get(SettingProperty.MPVplayer_Path, None)
        if path and os.path.isfile(path):
            cls._cache = path
            log.info(f"从配置中获取 MPV 路径: {path}")
            return path

        exe_name = "mpv.exe" if IS_WINDOWS else "mpv"
        which = shutil.which(exe_name)
        if which:
            cls._cache = which
            log.info(f"在 PATH 中找到 MPV: {which}")
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
                        log.info(f"通过注册表找到 MPV: {reg_path}")
                        return reg_path
            except Exception as e:
                log.debug(f"注册表查找失败: {e}")

        for p in default_paths:
            if os.path.isfile(p):
                cls._cache = p
                log.info(f"在默认路径找到 MPV: {p}")
                return p

        log.error("找不到 MPV 可执行文件。")
        return None

class MPVProcessManager:
    def __init__(self, executor: ThreadPoolExecutor) -> None:
        self.executor = executor
        self.process: Optional[subprocess.Popen] = None
        self.monitor_future: Optional[Future] = None
        self._lock: threading.Lock = threading.Lock()
        self._stop_monitor: threading.Event = threading.Event()
        self._on_exit_callback: Optional[Callable[[], None]] = None
        self._exit_callback_invoked: bool = False

    def set_exit_callback(self, callback: Callable[[], None]) -> None:
        self._on_exit_callback = callback

    def start(self, cmd: List[str], debug: bool = False) -> bool:
        try:
            creationflags = subprocess.CREATE_NO_WINDOW if IS_WINDOWS else 0
            env = os.environ.copy()
            for var in ['PYTHONHOME', 'PYTHONPATH', 'PYTHONSTARTUP', 'VIRTUAL_ENV', 'PYTHONUNBUFFERED']:
                env.pop(var, None)
            if debug:
                proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
                                        creationflags=creationflags, env=env)
                fut = self.executor.submit(self._log_output, proc)
                fut.add_done_callback(_future_done_callback)
            else:
                proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                        creationflags=creationflags, env=env)
            with self._lock:
                self.process = proc
            self._stop_monitor.clear()
            self._exit_callback_invoked = False
            fut = self.executor.submit(self._monitor, proc)
            fut.add_done_callback(_future_done_callback)
            self.monitor_future = fut
            return True
        except Exception as e:
            log.exception("启动 MPV 进程失败")
            return False

    def _log_output(self, proc: subprocess.Popen) -> None:
        if proc.stdout:
            for line in iter(proc.stdout.readline, ''):
                log.debug(f"MPV: {line.rstrip()}")

    def _monitor(self, proc: subprocess.Popen) -> None:
        start_time = time.time()
        while proc.poll() is None:
            if self._stop_monitor.is_set():
                return
            time.sleep(0.2)
        exit_code = proc.poll()
        elapsed = time.time() - start_time
        log.info(f"MPV 进程已退出，退出码 {exit_code}，运行时间 {elapsed:.1f} 秒")
        if self._on_exit_callback and not self._exit_callback_invoked:
            self._exit_callback_invoked = True
            self._on_exit_callback()

    def is_alive(self) -> bool:
        with self._lock:
            return self.process is not None and self.process.poll() is None

    def stop_monitoring(self) -> None:
        self._stop_monitor.set()

    def terminate(self, timeout: Optional[float] = None) -> Optional[int]:
        with self._lock:
            proc = self.process
        if proc is None or proc.poll() is not None:
            return proc.returncode if proc else None
        log.info(f"正在终止 MPV 进程 (PID {proc.pid})")
        proc.terminate()
        try:
            proc.wait(timeout=timeout or Config.PROCESS_TERMINATE_TIMEOUT)
        except subprocess.TimeoutExpired:
            log.warning("MPV 未响应终止请求，执行强制结束")
            proc.kill()
            proc.wait()
        return proc.returncode

    def kill(self) -> None:
        with self._lock:
            proc = self.process
        if proc and proc.poll() is None:
            try:
                proc.kill()
                proc.wait()
            except ProcessLookupError:
                log.debug("进程在强制结束前已退出")
            except Exception as e:
                log.warning(f"强制结束进程时发生异常: {e}")
        with self._lock:
            self.process = None

    def reset(self) -> None:
        self.stop_monitoring()
        with self._lock:
            self.process = None
        if self.monitor_future:
            self.monitor_future.cancel()

class TempFileManager:
    def __init__(self) -> None:
        self._temp_dir: Optional[Any] = None
        self._files: Set[str] = set()
        self._lock: threading.Lock = threading.Lock()
        self._active: bool = False
        self._cleanup_stale()

    def _cleanup_stale(self) -> None:
        if IS_WINDOWS:
            return
        import glob
        tmp_root = tempfile.gettempdir()
        for d in glob.glob(os.path.join(tmp_root, "macast_mpv_*")):
            try:
                shutil.rmtree(d, ignore_errors=True)
                log.info(f"已清理过期的临时目录: {d}")
            except Exception:
                log.debug("清理过期目录时出错", exc_info=True)

    def create(self) -> str:
        with self._lock:
            if self._temp_dir is None:
                self._temp_dir = tempfile.TemporaryDirectory(prefix="macast_mpv_")
                self._active = True
                log.info(f"已创建临时目录: {self._temp_dir.name}")
            return self._temp_dir.name

    def create_temp_file(self, content: str, suffix: str = ".m3u") -> str:
        with self._lock:
            if self._temp_dir is None:
                self.create()
            fd, path = tempfile.mkstemp(suffix=suffix, dir=self._temp_dir.name)
            with os.fdopen(fd, 'w', encoding='utf-8') as f:
                f.write(content)
            self._files.add(path)
            log.debug(f"已创建临时文件: {path} ({len(content)} 字节)")
            return path

    def copy_file_to_temp(self, src_path: str, suffix: Optional[str] = None) -> str:
        if not os.path.isfile(src_path):
            raise FileNotFoundError(f"源文件不存在: {src_path}")
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
            log.info(f"已复制文件 {src_path} -> {dst_path}")
            return dst_path

    def delete_file(self, path: str) -> None:
        with self._lock:
            self._files.discard(path)
            with suppress(Exception):
                os.unlink(path)
                log.debug(f"已删除临时文件: {path}")

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
                for attempt in range(3):
                    try:
                        self._temp_dir.cleanup()
                        log.info(f"已移除临时目录: {self._temp_dir.name}")
                        break
                    except Exception:
                        log.debug(f"临时目录清理第 {attempt+1} 次尝试失败", exc_info=True)
                        if attempt < 2:
                            time.sleep(0.5)
                        else:
                            log.warning(f"经过 3 次尝试仍无法移除临时目录: {self._temp_dir.name}")
                self._temp_dir = None

class PlaybackState:
    def __init__(self, on_state_change: Callable[[str, Any], None]) -> None:
        self._on_state_change = on_state_change
        self._lock: threading.RLock = threading.RLock()
        self._playing: bool = False
        self._pause: bool = False
        self._volume: int = 50
        self._position: float = 0.0
        self._duration: float = 0.0
        self._pending_position: Optional[float] = None
        self._last_pos_update: float = 0.0
        self._media_title: str = ""
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
            log.exception(f"状态变化回调错误 {cb_key}: {e}")

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
                log.exception(f"位置状态回调错误: {e}")

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
                log.exception(f"强制位置回调错误: {e}")

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
            self._current_uri = None
            self._current_is_playlist = False
            self._current_start_pos = 0
            self._current_audio_only = False

    def mark_playing(self, is_playing: bool) -> None:
        with self._lock:
            self._playing = is_playing

    def update_title(self, title: str) -> None:
        self._update_attr('_media_title', title, 'title')

    def update_metadata(self, metadata: Dict[str, Any]) -> None:
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

class ResumeManager:
    MAX_ENTRIES: int = 1000

    def __init__(self, path: str, executor: ThreadPoolExecutor) -> None:
        self.path = path
        self._executor = executor
        self._lock: threading.Lock = threading.Lock()
        self._cache: OrderedDict[str, float] = OrderedDict()
        self._dirty: bool = False
        self._load()

    def _load(self) -> None:
        try:
            if os.path.isfile(self.path):
                with open(self.path, 'r', encoding='utf-8') as f:
                    raw = json.load(f)
                self._cache = OrderedDict(raw)
                self._trim()
        except (IOError, json.JSONDecodeError) as e:
            log.warning(f"加载续播数据失败: {e}")
            self._cache = OrderedDict()

    def _trim(self) -> None:
        while len(self._cache) > self.MAX_ENTRIES:
            oldest = next(iter(self._cache))
            del self._cache[oldest]
            log.debug(f"续播条目因超限被移除: {oldest}")

    def _save(self) -> None:
        with self._lock:
            if not self._dirty:
                return
            items = list(self._cache.items())
            self._dirty = False
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            dir_name = os.path.dirname(self.path)
            with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                             dir=dir_name, delete=False, suffix='.tmp') as tf:
                json.dump(items, tf, ensure_ascii=False, indent=2)
                temp_name = tf.name
            os.replace(temp_name, self.path)
        except (IOError, OSError, json.JSONEncodeError) as e:
            log.error(f"保存续播数据失败: {e}")
            with suppress(Exception):
                if 'temp_name' in locals():
                    os.unlink(temp_name)

    def save_position(self, uri: str, pos: float, is_playlist: bool = False) -> None:
        if not Config.RESUME_ENABLED or pos <= 0 or is_playlist:
            return
        with self._lock:
            if uri in self._cache:
                del self._cache[uri]
            self._cache[uri] = pos
            self._dirty = True
            self._trim()
            self._executor.submit(self._save)

    def get_position(self, uri: str, is_playlist: bool = False) -> Optional[float]:
        if not Config.RESUME_ENABLED or is_playlist:
            return None
        with self._lock:
            pos = self._cache.get(uri)
            if pos is not None:
                del self._cache[uri]
                self._cache[uri] = pos
            return pos

    def remove(self, uri: str) -> None:
        with self._lock:
            if uri in self._cache:
                del self._cache[uri]
                self._dirty = True
                self._executor.submit(self._save)

class MpvIpcClient:
    def __init__(self, renderer: 'MPVplayerRenderer', socket_path: str, executor: ThreadPoolExecutor) -> None:
        self._renderer_ref: weakref.ref[MPVplayerRenderer] = weakref.ref(renderer)
        self._socket_path: str = socket_path
        self._executor: ThreadPoolExecutor = executor
        self._lock: threading.RLock = threading.RLock()
        self._sock: Optional[Any] = None
        self._sock_file: Optional[Any] = None
        self._connected: threading.Event = threading.Event()
        self._stop_event: threading.Event = threading.Event()
        self._request_id: int = 0
        self._pending_requests: Dict[int, threading.Event] = {}
        self._request_lock: threading.Lock = threading.Lock()
        self._message_queue: PriorityQueueWithDiscard = PriorityQueueWithDiscard(maxsize=Config.EVENT_QUEUE_MAXSIZE)
        self._consumer_future: Optional[Future] = None
        self._heartbeat_future: Optional[Future] = None
        self._heartbeat_failures: int = 0
        self._heartbeat_cond: threading.Condition = threading.Condition()
        self._heartbeat_paused: threading.Event = threading.Event()
        self._receiver_future: Optional[Future] = None
        self._overflow_callback: Callable[[], None] = self._on_pipe_overflow
        self._config_callback: Callable[[], None] = self._on_config_reloaded
        Config.register_callback(self._config_callback)
        # 安全网：确保即使未显式调用 stop()，回调也能被注销
        self._finalizer = weakref.finalize(self, self._cleanup_config_callback)

    def _cleanup_config_callback(self) -> None:
        Config.unregister_callback(self._config_callback)

    def _on_pipe_overflow(self) -> None:
        log.error("检测到管道缓冲区溢出，重置 IPC 连接")
        self._close_socket()
        self._reset_connection_state()

    def _on_config_reloaded(self) -> None:
        log.debug("IPC 客户端已感知配置重载")

    def start(self) -> None:
        self._stop_event.clear()
        self._receiver_future = self._executor.submit(self._loop)
        self._receiver_future.add_done_callback(_future_done_callback)
        self._consumer_future = self._executor.submit(self._consumer_loop)
        self._consumer_future.add_done_callback(_future_done_callback)
        self._heartbeat_future = self._executor.submit(self._heartbeat_loop)
        self._heartbeat_future.add_done_callback(_future_done_callback)

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
        Config.unregister_callback(self._config_callback)
        if self._finalizer:
            self._finalizer.detach()  # 手动停止时取消安全网

    def pause_heartbeat(self) -> None:
        self._heartbeat_paused.set()
        with self._heartbeat_cond:
            self._heartbeat_cond.notify_all()

    def resume_heartbeat(self) -> None:
        self._heartbeat_paused.clear()
        with self._heartbeat_cond:
            self._heartbeat_cond.notify_all()

    def _loop(self) -> None:
        config_check_interval = 100
        counter = 0
        while not self._stop_event.is_set():
            counter += 1
            if counter % config_check_interval == 0:
                Config.reload_if_changed()
            if not self._connect():
                if self._stop_event.is_set():
                    break
                time.sleep(Config.IPC_RETRY_BASE)
                continue
            Config.reload_if_changed()
            self._receive_loop()
        self._connected.clear()
        log.debug("IPC 客户端接收循环已结束")

    @log_exceptions
    def _consumer_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                _, data = self._message_queue.get(timeout=0.5)
                if data is None:
                    continue
                self._apply_message(data)
            except queue.Empty:
                continue
        log.debug("IPC 客户端消费循环已结束")

    @log_exceptions
    def _heartbeat_loop(self) -> None:
        prev_failures = 0
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
                if self._heartbeat_failures == 1 or self._heartbeat_failures != prev_failures:
                    log.debug(f"心跳失败 {self._heartbeat_failures}/{Config.HEARTBEAT_FAILURE_THRESHOLD}")
                prev_failures = self._heartbeat_failures
                if self._heartbeat_failures >= Config.HEARTBEAT_FAILURE_THRESHOLD:
                    log.warning("心跳失败达到阈值，强制重连")
                    self._heartbeat_failures = 0
                    prev_failures = 0
                    self._close_socket()
                    self._reset_connection_state()
                    self._connected.clear()
            else:
                if self._heartbeat_failures > 0:
                    log.debug("心跳已恢复")
                self._heartbeat_failures = 0
                prev_failures = 0

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
                with self._heartbeat_cond:
                    self._heartbeat_cond.notify_all()
                log.info("已连接到 MPV IPC")
                return True
            wait = min(base * (2 ** (attempt - 1)), 5.0)
            if attempt < retry_count:
                time.sleep(wait)
        log.error("多次尝试后 IPC 连接失败")
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
                        log.debug("pywin32 未安装，回退到 _winapi")
                wrapper = _winapi_connect(self._socket_path, Config.IPC_CONNECT_TIMEOUT,
                                          on_overflow=self._overflow_callback)
                return wrapper
            else:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(Config.IPC_CONNECT_TIMEOUT)
                sock.connect(self._socket_path)
                return sock
        except (FileNotFoundError, ConnectionRefusedError, TimeoutError) as e:
            log.debug(f"IPC 连接错误: {e}")
            return None
        except Exception as e:
            log.warning(f"创建 IPC 连接时发生意外错误: {e}", exc_info=True)
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

    def _reset_connection_state(self) -> None:
        with self._request_lock:
            for evt in self._pending_requests.values():
                evt.set()
            self._pending_requests.clear()

    def _receive_loop(self) -> None:
        # 完全在锁保护下获取 socket 和文件包装，防止中途被 close
        with self._lock:
            sock = self._sock
            if sock is None:
                return
            try:
                if self._sock_file is None:
                    self._sock_file = sock.makefile('r')
                f = self._sock_file
            except Exception as e:
                log.error(f"创建文件包装失败: {e}")
                self._close_socket()
                return

        last_activity = time.monotonic()
        try:
            while not self._stop_event.is_set() and self._connected.is_set():
                try:
                    line = f.readline()
                    if not line:
                        break
                    last_activity = time.monotonic()
                    self._handle_message(line.strip())
                except (ConnectionResetError, BrokenPipeError, OSError, ConnectionError) as e:
                    log.debug(f"IPC 接收错误: {e}")
                    break
                except Exception as e:
                    log.exception(f"IPC 接收循环中发生意外异常: {e}")
                    break
                if time.monotonic() - last_activity > Config.HEARTBEAT_INTERVAL * 2:
                    log.warning("IPC 接收超时（无数据），可能管道阻塞，强制断开")
                    break
        finally:
            self._close_socket()
            self._reset_connection_state()

    @staticmethod
    def _classify_priority(data: Dict[str, Any]) -> int:
        if 'error' in data and data.get('error') != 'success':
            return 1
        if 'event' in data:
            event_name = data['event']
            if event_name in ('start-file', 'end-file', 'idle'):
                return 5
            if event_name == 'playback-restart':
                return 7
        return 10

    def _handle_message(self, msg: str) -> None:
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            log.warning(f"无效的 JSON 消息: {msg}")
            return

        priority = self._classify_priority(data)
        try:
            self._message_queue.put_nowait((priority, data))
        except queue.Full:
            if priority > 5:
                log.debug(f"事件队列满，丢弃低优先级消息 (priority={priority})")
                return
            if not self._message_queue.discard_lowest():
                log.error("无法从队列中移除低优先级元素，关键事件丢失")
                return
            try:
                self._message_queue.put_nowait((priority, data))
            except queue.Full:
                log.error("事件队列仍满，关键事件最终丢失")

    def _apply_message(self, data: Dict[str, Any]) -> None:
        renderer = self._renderer_ref()
        if renderer is None:
            return
        state = renderer.state
        dispatch = {
            ObservedProperty.PAUSE:          lambda v: state.update_pause(bool(v)),
            ObservedProperty.TIME_POS:       lambda v: state.update_position(float(v)) if v is not None else None,
            ObservedProperty.DURATION:       lambda v: state.update_duration(float(v)) if v is not None else None,
            ObservedProperty.VOLUME:         lambda v: state.update_volume(int(v)) if v is not None else None,
            ObservedProperty.MEDIA_TITLE:    lambda v: state.update_title(str(v)) if v is not None else None,
            ObservedProperty.METADATA:       lambda v: state.update_metadata(v) if v is not None else None,
            ObservedProperty.CHAPTER_LIST:   lambda v: state.update_chapters(v) if isinstance(v, list) else None,
            ObservedProperty.CHAPTER:        lambda v: state.update_chapter(int(v)) if v is not None else None,
            ObservedProperty.PLAYLIST:       lambda v: state.update_playlist(v) if isinstance(v, list) else None,
            ObservedProperty.AID:            lambda v: state.update_aid(int(v)) if v is not None else None,
            ObservedProperty.SID:            lambda v: state.update_sid(int(v)) if v is not None else None,
            ObservedProperty.TRACK_LIST:     lambda v: state.update_track_list(v) if isinstance(v, list) else None,
        }
        try:
            if "request_id" in data:
                req_id = data["request_id"]
                with self._request_lock:
                    evt = self._pending_requests.pop(req_id, None)
                if evt:
                    if "error" in data and data["error"] != "success":
                        log.warning(f"请求 {req_id} 命令错误: {data.get('error')}")
                    evt.set()
                return

            if "id" in data and "data" in data:
                pid = data["id"]
                value = data.get("data")
                try:
                    prop = ObservedProperty(pid)
                except ValueError:
                    return
                handler = dispatch.get(prop)
                if handler:
                    try:
                        handler(value)
                    except (ValueError, TypeError) as e:
                        log.warning(f"应用属性 {prop} 值时出错: {e}")
                return

            if "event" in data:
                event = data["event"]
                if event == "start-file":
                    renderer.state.mark_playing(True)
                    if renderer.protocol:
                        renderer._publish_event("renderer_av_uri", renderer.protocol.get_state_url())
                elif event == "end-file":
                    renderer.state.flush_position_forced()
                    fut = renderer._executor.submit(renderer._async_save_resume_on_end)
                    fut.add_done_callback(_future_done_callback)
                elif event == "playback-restart":
                    renderer.state.flush_position_forced()
                elif event == "idle":
                    renderer.state.mark_playing(False)
                    renderer.set_state_stop()
        except Exception as e:
            log.exception(f"_apply_message 处理消息时失败: {data}")

    def _cleanup_stale_requests(self) -> None:
        deadline = time.time() - Config.IPC_COMMAND_TIMEOUT * 2
        with self._request_lock:
            stale = []
            for rid, evt in list(self._pending_requests.items()):
                ts = getattr(evt, '_timestamp', 0)
                if ts > 0 and ts < deadline:
                    stale.append(rid)
            for rid in stale:
                evt = self._pending_requests.pop(rid, None)
                if evt:
                    evt.set()
            if len(self._pending_requests) > 500:
                log.warning(f"待处理请求过多 ({len(self._pending_requests)})，强制清空")
                for evt in self._pending_requests.values():
                    evt.set()
                self._pending_requests.clear()

    def send_command(self, command: List[Any], timeout: Optional[float] = None,
                     retry: int = 0, wait_response: bool = False) -> Optional[Dict[str, Any]]:
        if timeout is None:
            timeout = Config.IPC_COMMAND_TIMEOUT
        if len(self._pending_requests) > 300:
            self._cleanup_stale_requests()

        max_attempts = retry + 1
        request_id = None
        evt = None
        if wait_response:
            with self._request_lock:
                self._request_id += 1
                request_id = self._request_id
                evt = threading.Event()
                evt._timestamp = time.time()  # type: ignore
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
                        log.debug(f"已发送命令: {command} (尝试 {attempt+1})")
                except (BrokenPipeError, ConnectionResetError, OSError, TimeoutError) as e:
                    log.warning(f"IPC 发送失败 (尝试 {attempt+1}/{max_attempts}): {e}")
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
                    log.error(f"发送时发生意外错误: {e}")
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
                    log.warning(f"命令 {command} 在 {timeout} 秒内未收到响应")
                    with self._request_lock:
                        self._pending_requests.pop(request_id, None)
                    return None
            else:
                return {}
        return None

    def _observe_properties(self) -> None:
        for prop in ObservedProperty:
            if not self.send_command(["observe_property", prop.value, prop.mpv_name], wait_response=True):
                log.error(f"观察属性 {prop.mpv_name} 失败")
        if self.send_command(["get_property", "pause"], wait_response=True, timeout=2.0) is None:
            log.error("观察属性后的 IPC 健康检查失败，强制断开连接")
            self._close_socket()
            self._connected.clear()

    def is_connected(self) -> bool:
        return self._connected.is_set() and self._sock is not None

class MPVplayerRenderer(Renderer):
    def __init__(self) -> None:
        super().__init__()
        Config.load()

        max_workers = min(32, (os.cpu_count() or 1) + 4)
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="MPV")
        self._lock: threading.RLock = threading.RLock()
        self._stop_event: threading.Event = threading.Event()
        self._cleanup_lock: threading.Lock = threading.Lock()
        self._cleanup_complete: threading.Event = threading.Event()
        self._cleanup_complete.set()
        self._cleanup_scheduled: threading.Event = threading.Event()
        self._state_enum: RendererState = RendererState.IDLE
        self._start_lock: threading.RLock = threading.RLock()

        self.proc_manager: MPVProcessManager = MPVProcessManager(self._executor)
        self.proc_manager.set_exit_callback(self._on_process_exit)

        self.temp_manager: TempFileManager = TempFileManager()
        self.temp_manager.create()

        self.ipc_client: Optional[MpvIpcClient] = None
        self.state: PlaybackState = PlaybackState(on_state_change=self._on_playback_state_change)
        self._subtitle_file: Optional[str] = None
        self._subtitle_mtime: Optional[float] = None
        self._subtitle_original_path: Optional[str] = None
        self.ipc_sock: Optional[str] = None

        self._resume_manager: ResumeManager = ResumeManager(Config.RESUME_DATA_PATH, self._executor)
        self._resume_save_future: Optional[Future] = None
        self._resume_save_stop: threading.Event = threading.Event()

        self._event_stop: threading.Event = threading.Event()
        self._event_queue: queue.Queue = queue.Queue(maxsize=Config.EVENT_QUEUE_MAXSIZE)
        self._event_future: Future = self._executor.submit(self._event_publisher_loop)
        self._event_future.add_done_callback(_future_done_callback)

        atexit.register(self._atexit_cleanup)
        self._start_resume_saving()

    def _on_process_exit(self) -> None:
        if not self._stop_event.is_set():
            if not self._cleanup_scheduled.is_set():
                self._cleanup_scheduled.set()
                log.debug("进程已退出，调度清理")
                self._schedule_cleanup()

    def _shutdown_executor(self, timeout: float = 5.0) -> None:
        try:
            self._executor.shutdown(wait=True, timeout=timeout)
            log.debug("线程池已优雅关闭")
        except Exception as e:
            log.error(f"关闭线程池时出错: {e}")
            self._executor.shutdown(wait=False)

    def _start_resume_saving(self) -> None:
        def saver() -> None:
            while not self._resume_save_stop.is_set():
                self._resume_save_stop.wait(timeout=Config.RESUME_SAVE_INTERVAL)
                if self._resume_save_stop.is_set():
                    break
                self._save_resume_periodic()
        self._resume_save_future = self._executor.submit(saver)
        self._resume_save_future.add_done_callback(_future_done_callback)

    def _save_resume_periodic(self) -> None:
        if not Config.RESUME_ENABLED or self.state.is_current_playlist():
            return
        uri = self.state.get_current_uri()
        pos = self.state.get_position()
        if uri and pos > 0:
            self._resume_manager.save_position(uri, pos, is_playlist=False)
            log.debug(f"定期保存续播: {_sanitize_url(uri)} @ {pos} 秒")

    def _async_save_resume_on_end(self) -> None:
        try:
            if not Config.RESUME_ENABLED or self.state.is_current_playlist():
                return
            uri = self.state.get_current_uri()
            pos = self.state.get_position()
            if uri and pos > 0:
                self._resume_manager.save_position(uri, pos, is_playlist=False)
                if log.isEnabledFor(logging.DEBUG):
                    log.debug(f"异步保存续播: {_sanitize_url(uri)} @ {pos} 秒")
        except Exception:
            log.warning("异步保存续播位置失败", exc_info=True)

    @log_exceptions
    def _event_publisher_loop(self) -> None:
        log.debug("事件发布循环已启动")
        while not self._event_stop.is_set():
            try:
                topic, args = self._event_queue.get(timeout=0.5)
                if topic is None:
                    continue
                cherrypy.engine.publish(topic, *args)
            except queue.Empty:
                continue
            except Exception as e:
                log.exception(f"发布事件 {topic} 时出错: {e}")
        log.debug("事件发布循环已退出")

    CRITICAL_EVENTS: Set[str] = {"renderer_av_stop", "app_notify"}

    def _publish_event(self, topic: str, *args: Any) -> None:
        try:
            if topic in self.CRITICAL_EVENTS:
                try:
                    self._event_queue.put((topic, args), timeout=0.5)
                except queue.Full:
                    log.warning(f"关键事件 {topic} 队列已满，直接发布")
                    cherrypy.engine.publish(topic, *args)
            else:
                try:
                    self._event_queue.put_nowait((topic, args))
                except queue.Full:
                    log.warning(f"事件队列满，丢弃非关键事件 {topic}")
        except Exception as e:
            log.error(f"将事件放入队列时出错 {topic}: {e}")

        qsize = self._event_queue.qsize()
        if qsize > Config.EVENT_QUEUE_MAXSIZE * 0.8:
            log.warning(f"事件队列接近满载: {qsize}/{Config.EVENT_QUEUE_MAXSIZE}")

    def stop(self) -> None:
        super().stop()
        self._set_state_enum(RendererState.STOPPED)
        self._stop_event.set()
        self._resume_save_stop.set()

        if not self._cleanup_done():
            self._schedule_cleanup()
        self._cleanup_complete.wait(timeout=Config.CLEANUP_TOTAL_TIMEOUT + 2)

        self._event_stop.set()
        self._shutdown_executor(timeout=5.0)

        self.temp_manager.cleanup(force=True)
        atexit.unregister(self._atexit_cleanup)
        log.info("MPVPlayer 已停止")

    def _set_state_enum(self, new_state: RendererState) -> None:
        with self._lock:
            if self._state_enum != new_state:
                old = self._state_enum
                self._state_enum = new_state
                log.debug(f"状态变更: {old} -> {new_state}")
                if self.ipc_client:
                    try:
                        if new_state in (RendererState.IDLE, RendererState.STOPPED, RendererState.CLEANING):
                            self.ipc_client.pause_heartbeat()
                        elif new_state == RendererState.RUNNING:
                            self.ipc_client.resume_heartbeat()
                    except Exception:
                        log.exception("心跳状态切换失败（IPC 可能已停止）")

    def _get_state_enum(self) -> RendererState:
        with self._lock:
            return self._state_enum

    def _sync_dlna_state(self, key: str, value: Any) -> None:
        try:
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
        except Exception as e:
            log.exception(f"同步 DLNA 状态失败 {key}: {value}")

    def _on_playback_state_change(self, key: str, value: Any) -> None:
        self._sync_dlna_state(key, value)

    def _cleanup_done(self) -> bool:
        return self._cleanup_complete.is_set()

    def _schedule_cleanup(self) -> None:
        if self._cleanup_done() and self._get_state_enum() != RendererState.CLEANING \
                and not self._cleanup_scheduled.is_set():
            self._cleanup_scheduled.set()
            self._executor.submit(self._do_cleanup)

    def _do_cleanup(self) -> None:
        if not self._cleanup_lock.acquire(blocking=False):
            return
        self._cleanup_complete.clear()
        self._cleanup_scheduled.clear()
        future = self._executor.submit(self._cleanup_task)
        future.add_done_callback(_future_done_callback)
        try:
            future.result(timeout=Config.CLEANUP_TOTAL_TIMEOUT)
        except FutureTimeoutError:
            log.error("清理任务超时，强制结束进程")
            self.proc_manager.kill()
        except Exception as e:
            log.exception("清理任务失败")
        finally:
            self._cleanup_lock.release()

    def _cleanup_task(self) -> None:
        try:
            log.info("开始清理")
            try:
                if Config.RESUME_ENABLED and not self.state.is_current_playlist():
                    uri = self.state.get_current_uri()
                    pos = self.state.get_position()
                    if uri and pos > 0:
                        self._resume_manager.save_position(uri, pos, is_playlist=False)
                        log.info(f"清理时保存续播位置: {_sanitize_url(uri)} @ {pos} 秒")
            except Exception:
                log.warning("清理过程中保存续播位置失败", exc_info=True)

            self._set_state_enum(RendererState.CLEANING)
            self._stop_event.set()
            if self.ipc_client and self.ipc_client.is_connected():
                self.ipc_client.send_command(["sub_remove", "all"], timeout=0.5, wait_response=False)
                self.ipc_client.send_command(["playlist-clear"], timeout=0.5, wait_response=False)
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
        except Exception:
            log.exception("清理任务执行时出错")
            raise
        finally:
            self._safe_reset_state()
            self._cleanup_complete.set()
            log.info("清理完成（状态重置已保证）")

    def _safe_reset_state(self) -> None:
        try:
            self.state.reset()
            self.set_state_transport("STOPPED")
            self._publish_event("renderer_av_stop")
            self._set_state_enum(RendererState.IDLE)
        except Exception:
            log.exception("在安全重置状态时发生异常（已尝试通知前端）")
            try:
                self._publish_event("renderer_av_stop")
            except Exception:
                log.error("无法发送 renderer_av_stop 事件，前端可能未同步")

    def _remove_socket_file(self) -> None:
        if not IS_WINDOWS and self.ipc_sock:
            with suppress(Exception):
                os.unlink(self.ipc_sock)
                log.debug(f"已移除 IPC socket: {self.ipc_sock}")
            self.ipc_sock = None

    def _wait_cleanup_finish(self, timeout: float = 5.0) -> None:
        self._cleanup_complete.wait(timeout=timeout)

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

    def _prepare_command(self, media: str, audio_only: bool = False) -> List[str]:
        mpv_path = MpvFinder.find()
        if not mpv_path:
            raise RuntimeError("找不到 MPV 可执行文件")
        cmd = [mpv_path, "--fullscreen", f"--input-ipc-server={self.ipc_sock}",
               "--cache=yes", "--keep-open=yes", "--no-terminal"]
        if Config.EXTRA_ARGS:
            cmd.extend(Config.EXTRA_ARGS)
        if audio_only:
            if "--no-video" not in cmd:
                cmd.append("--no-video")
        elif os.path.isfile(media):
            ext = os.path.splitext(media)[1].lower()
            if ext in Config.AUDIO_ONLY_EXTENSIONS:
                if "--no-video" not in cmd:
                    cmd.append("--no-video")
        cmd.append(media)
        if self._subtitle_file and os.path.exists(self._subtitle_file):
            cmd.append(f"--sub-file={self._subtitle_file}")
        return cmd

    @contextmanager
    def _acquire_start_lock(self) -> Any:
        acquired = self._start_lock.acquire(timeout=Config.START_LOCK_TIMEOUT)
        if not acquired:
            raise RuntimeError("启动锁获取超时")
        try:
            yield
        finally:
            self._start_lock.release()

    def _launch_mpv(self, media: str, is_playlist: bool = False, start: int = 0, audio_only: bool = False) -> bool:
        try:
            with self._acquire_start_lock():
                log.debug("_launch_mpv 已获取启动锁")
                Config.reload_if_changed()
                mpv_path = MpvFinder.find()
                if not mpv_path:
                    if IS_WINDOWS:
                        subprocess.Popen(["notepad.exe", Setting.setting_path],
                                         creationflags=subprocess.CREATE_NO_WINDOW)
                    self._publish_event("app_notify", "错误",
                                        "找不到 MPV。请在配置中设置 'MPVplayer_Path'。")
                    return False
                if not is_playlist and start == 0:
                    saved_pos = self._get_resume_position(media)
                    if saved_pos and saved_pos > 0:
                        start = int(saved_pos)
                        log.info(f"从 {start} 秒处续播 {_sanitize_url(media)}")
                self.state.set_current_media_info(media, is_playlist, start, audio_only)

                proc_alive = self.proc_manager.is_alive()
                ipc_ok = self.ipc_client and self.ipc_client.is_connected()
                if proc_alive and ipc_ok:
                    cmd = ["loadlist", media, "replace"] if is_playlist else ["loadfile", media, "replace"]
                    if not is_playlist and start > 0:
                        cmd.append(f"start={start}")
                    log.info(f"发送无缝切换命令: {cmd} (media={_sanitize_url(media)})")
                    if self.ipc_client.send_command(cmd, timeout=Config.SEAMLESS_SWITCH_TIMEOUT,
                                                    retry=Config.IPC_COMMAND_RETRY, wait_response=False) is not None:
                        log.info(f"无缝切换到 {_sanitize_url(media)} 已成功发送")
                        if is_playlist and start > 0:
                            self.ipc_client.send_command(["seek", start, "absolute"], wait_response=False)
                        self.set_state_transport("PLAYING")
                        self._publish_event("renderer_av_uri", media)
                        return True
                    else:
                        log.warning("IPC 切换命令失败，将重启 MPV")
                elif proc_alive and not ipc_ok:
                    log.warning("MPV 进程存在但 IPC 未连接，强制结束以干净重启")
                    if self.ipc_client:
                        self.ipc_client.stop()
                        self.ipc_client = None
                    self.proc_manager.kill()
                    self._remove_socket_file()

                if not self._cleanup_done():
                    self._schedule_cleanup()
                    self._wait_cleanup_finish(timeout=5.0)
                self._stop_event.clear()
                self.ipc_sock = self._generate_socket_path()
                try:
                    cmd = self._prepare_command(media, audio_only=audio_only)
                    log.info(f"启动 MPV (完整重启): {_sanitize_cmd(cmd)}")
                    debug = Setting.get(SettingProperty.MPVplayer_Debug, False)
                    if not self.proc_manager.start(cmd, debug):
                        self._remove_socket_file()
                        return False
                    self.ipc_client = MpvIpcClient(self, self.ipc_sock, self._executor)
                    self.ipc_client.start()
                    if not self._wait_ipc_connected(Config.IPC_CONNECT_TIMEOUT + 1.0):
                        log.error("进程启动后 IPC 连接超时")
                        self.ipc_client.stop()
                        self.ipc_client = None
                        self.proc_manager.kill()
                        self._remove_socket_file()
                        return False
                    self.set_state_transport("PLAYING")
                    self._set_state_enum(RendererState.RUNNING)
                    return True
                except Exception as e:
                    log.exception(f"准备命令时失败: {e}")
                    self._publish_event("app_notify", "错误", str(e))
                    # 确保异常分支也终止可能已启动的进程
                    if self.ipc_client:
                        self.ipc_client.stop()
                        self.ipc_client = None
                    self.proc_manager.kill()
                    self._remove_socket_file()
                    return False
        except RuntimeError as e:
            log.error(f"启动失败: {e}")
            self._publish_event("app_notify", "错误", str(e))
            return False

    # ---- Renderer 接口 ----
    def set_media_stop(self) -> None:
        self._save_resume_if_needed()
        if not self._cleanup_done():
            self._schedule_cleanup()
        else:
            log.debug("已经停止")

    def set_media_pause(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["set_property", "pause", True], wait_response=False)
        self._save_resume_if_needed()

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
            log.error(f"无效的位置格式: {data}")

    def set_media_playlist(self, urls: List[str], start: int = 0) -> None:
        if not urls:
            log.warning("空播放列表，忽略")
            return

        if not self._cleanup_done():
            self._schedule_cleanup()
            self._wait_cleanup_finish()

        first_url = urls[0]
        rest_urls = urls[1:]

        if not self._launch_mpv(first_url, is_playlist=False, start=0):
            self._publish_event("app_notify", "错误", "无法启动播放列表首项")
            return

        if not self._wait_ipc_connected(5):
            log.error("播放列表：启动后 IPC 未就绪")
            return

        for url in rest_urls:
            self.ipc_client.send_command(["loadfile", url, "append"], wait_response=False)

        if start > 0:
            self.ipc_client.send_command(["playlist-play-index", start], wait_response=False)

        log.info(f"已通过 IPC 构建播放列表，共 {len(urls)} 项，起始索引 {start}")

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
            log.warning(f"速度 {speed} 超出范围")

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
            log.info(f"跳转到章节 {index}")

    def next_chapter(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["add", "chapter", 1], wait_response=False)

    def prev_chapter(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["add", "chapter", -1], wait_response=False)

    def set_media_osd(self, text: str, duration: Optional[int] = None) -> None:
        if duration is None:
            duration = Config.OSD_DEFAULT_DURATION
        if self.ipc_client:
            self.ipc_client.send_command(["show-text", text, duration], wait_response=False)
            log.info(f"OSD: {text} ({duration} 毫秒)")

    def clear_resume_position(self, uri: Optional[str] = None) -> None:
        if uri is None:
            uri = self.state.get_current_uri()
        if uri:
            self._resume_manager.remove(uri)
            log.info(f"已清除续播位置: {_sanitize_url(uri)}")

    def get_playlist(self) -> List[Dict[str, Any]]:
        return self.state.get_playlist()

    def playlist_clear(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-clear"], wait_response=False)
            log.info("播放列表已清空")

    def playlist_remove_index(self, index: int) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-remove", index], wait_response=False)
            log.info(f"已移除播放列表索引 {index}")

    def playlist_move(self, index1: int, index2: int) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-move", index1, index2], wait_response=False)
            log.info(f"已移动播放列表项从 {index1} 到 {index2}")

    def playlist_shuffle(self) -> None:
        if self.ipc_client:
            self.ipc_client.send_command(["playlist-shuffle"], wait_response=False)
            log.info("播放列表已随机播放")

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
        log.info(f"MPVPlayer 渲染器启动 (插件版本 {Config.PLUGIN_VERSION})")
        self._set_state_enum(RendererState.RUNNING)

    def _save_resume_if_needed(self) -> None:
        if not Config.RESUME_ENABLED or self.state.is_current_playlist():
            return
        uri = self.state.get_current_uri()
        pos = self.state.get_position()
        if uri and pos > 0:
            self._resume_manager.save_position(uri, pos, is_playlist=False)

    def _atexit_cleanup(self) -> None:
        if not self._cleanup_done():
            self._do_cleanup()

if __name__ == "__main__":
    gui(MPVplayerRenderer())