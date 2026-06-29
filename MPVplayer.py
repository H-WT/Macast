# Copyright (c) 2021 by xfangfang. All Rights Reserved.
# Optimized version with improved stability, thread safety, and cross-platform compatibility.
# Features:
#   - Seamless switching via loadfile replace
#   - Robust IPC with heartbeat and auto-reconnection
#   - Graceful resource cleanup with thread coordination
#   - Enhanced error handling and logging
#   - Temporary file management for socket and subtitles
#   - Subtitle support (set_subtitle)
#   - Playlist support (set_media_playlist)
#   - UI state update throttling (0.5s interval)
#   - Fine-grained locking for better concurrency
#   - Subtitle file copy to temp dir (robustness)
#   - Heartbeat failure detection with auto-kill
#   - Full type annotations
#   - Decoupled event publishing via dedicated queue thread
#
# Macast Metadata
# <macast.title>MPVPlayer Renderer</macast.title>
# <macast.renderer>MPVplayerRenderer</macast.renderer>
# <macast.platform>win32</macast.platform>
# <macast.version>0.9</macast.version>
# <macast.host_version>0.7</macast.host_version>
# <macast.author>HWT</macast.author>
# <macast.desc>调用本地MPV，需在配置文件设置MPVplayer_Path</macast.desc>

import os
import sys
import time
import json
import logging
import shutil
import threading
import subprocess
import socket
import random
import tempfile
import queue
from enum import Enum
from typing import Optional, List, Dict, Any, Union, Callable, Tuple
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor

import cherrypy
from macast import gui, Setting
from macast.renderer import Renderer
from macast.utils import SETTING_DIR

logger = logging.getLogger("MPVPlayer")

# ==================== 常量 ====================
IPC_RETRY_COUNT = 10
IPC_RETRY_BASE = 0.5
IPC_CONNECT_TIMEOUT = 2.0
HEARTBEAT_INTERVAL = 5.0
PROCESS_TERMINATE_TIMEOUT = 3.0
MAX_SPEED = 2.0
MIN_SPEED = 0.1
POSITION_UPDATE_INTERVAL = 0.5   # 状态更新节流间隔（秒）
HEARTBEAT_FAILURE_THRESHOLD = 3  # 连续心跳失败次数阈值

# ==================== MPV 路径查找器 ====================
class MpvFinder:
    """MPV 可执行文件查找（带缓存和默认路径）"""
    _cache: Optional[str] = None

    @classmethod
    def find(cls) -> Optional[str]:
        if cls._cache is not None:
            return cls._cache

        # 1. 从设置读取
        path = Setting.get(SettingProperty.MPVplayer_Path, None)
        if path and os.path.isfile(path):
            cls._cache = path
            return path

        # 2. 系统 PATH
        which = shutil.which("mpv.exe") or shutil.which("mpv")
        if which:
            cls._cache = which
            return which

        # 3. 默认安装路径（扩展）
        default_paths = [
            r"C:\Program Files\mpv\mpv.exe",
            r"D:\mpv\mpv.exe",
            r"C:\Program Files (x86)\mpv\mpv.exe",
            "/usr/bin/mpv",
            "/usr/local/bin/mpv",
        ]
        # 4. Windows 注册表（需 pywin32，可选）
        if os.name == 'nt':
            try:
                import winreg
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\mpv.exe")
                reg_path, _ = winreg.QueryValueEx(key, "")
                if os.path.isfile(reg_path):
                    cls._cache = reg_path
                    return reg_path
            except Exception:
                pass

        for p in default_paths:
            if os.path.isfile(p):
                cls._cache = p
                return p

        logger.error("MPV executable not found.")
        return None

# ==================== 设置枚举 ====================
class SettingProperty(Enum):
    MPVplayer_Path = "MPVplayer_Path"  # 使用字符串键名

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        return Setting.get(key.value, default)

# ==================== 临时目录管理 ====================
class TempDirManager:
    """管理临时目录、套接字文件和临时媒体文件"""
    def __init__(self) -> None:
        self.temp_dir: Optional[str] = None
        self.sock_path: Optional[str] = None
        self._files_to_cleanup: List[str] = []  # 记录需要清理的临时文件路径

    def create(self) -> str:
        if self.temp_dir is None:
            self.temp_dir = tempfile.mkdtemp(prefix="macast_mpv_")
        if self.sock_path is None:
            if os.name == 'nt':
                self.sock_path = r"\\.\pipe\macast_mpv_{}".format(random.randint(0, 999999))
            else:
                self.sock_path = os.path.join(self.temp_dir, "mpv.sock")
        return self.sock_path

    def create_temp_file(self, content: str, suffix: str = ".m3u") -> str:
        """创建临时文件并自动注册到清理列表"""
        if self.temp_dir is None:
            self.create()
        fd, path = tempfile.mkstemp(suffix=suffix, dir=self.temp_dir)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        self._files_to_cleanup.append(path)
        return path

    def copy_file_to_temp(self, src_path: str, suffix: Optional[str] = None) -> str:
        """将外部文件复制到临时目录，并注册清理"""
        if not os.path.isfile(src_path):
            raise FileNotFoundError(f"Source file not found: {src_path}")
        if self.temp_dir is None:
            self.create()
        # 生成目标文件名
        base = os.path.basename(src_path)
        if suffix is None:
            # 保留原扩展名
            _, ext = os.path.splitext(base)
            suffix = ext or ".tmp"
        # 创建唯一文件名
        fd, dst_path = tempfile.mkstemp(suffix=suffix, dir=self.temp_dir, prefix="sub_")
        os.close(fd)  # 仅生成路径，后续写入
        shutil.copy2(src_path, dst_path)
        self._files_to_cleanup.append(dst_path)
        return dst_path

    def cleanup(self) -> None:
        # 删除注册的临时文件
        for f in self._files_to_cleanup:
            if os.path.exists(f):
                try:
                    os.unlink(f)
                except Exception as e:
                    logger.warning(f"Failed to delete temp file {f}: {e}")
        self._files_to_cleanup.clear()

        # 删除整个临时目录（包括socket）
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                for f in os.listdir(self.temp_dir):
                    os.unlink(os.path.join(self.temp_dir, f))
                os.rmdir(self.temp_dir)
            except Exception as e:
                logger.warning(f"Failed to cleanup temp dir: {e}")
            self.temp_dir = None
            self.sock_path = None

# ==================== 主渲染器类（细粒度锁优化 + 事件解耦） ====================
class MPVplayerRenderer(Renderer):
    def __init__(self) -> None:
        super().__init__()
        # ----- 细粒度锁 -----
        self._state_lock = threading.RLock()      # 播放状态变量
        self._ipc_lock = threading.RLock()        # IPC 连接和线程
        self._process_lock = threading.RLock()    # 进程和监控线程
        self._cleanup_lock = threading.RLock()    # 清理状态

        self._stop_event = threading.Event()
        self._is_cleaning: bool = False
        self._cleanup_done = threading.Event()
        self._cleanup_done.set()
        self._cleanup_thread: Optional[threading.Thread] = None

        # 进程和IPC
        self.process: Optional[subprocess.Popen] = None
        self.ipc_sock: Optional[socket.socket] = None
        self.ipc_thread: Optional[threading.Thread] = None
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.monitor_thread: Optional[threading.Thread] = None
        self._process_exit_event = threading.Event()
        self._ipc_connected = threading.Event()
        self._reconnect_attempts: int = 0

        # 播放状态（由 _state_lock 保护）
        self._playing: bool = False
        self._pause: bool = False
        self._volume: int = 50
        self._position: float = 0.0
        self._duration: float = 0.0

        # 状态更新节流
        self._last_update_time: Dict[str, float] = {}

        # 临时目录和套接字
        self.temp_manager = TempDirManager()
        self.mpv_sock: Optional[str] = None

        # ----- 事件解耦（专用队列和线程） -----
        self._event_queue: queue.Queue = queue.Queue()
        self._event_stop = threading.Event()
        self._event_thread: Optional[threading.Thread] = None
        self._start_event_publisher()

        # 字幕和播放列表文件
        self._subtitle_file: Optional[str] = None
        self._playlist_file: Optional[str] = None

    # ==================== 事件发布器（独立线程） ====================
    def _start_event_publisher(self) -> None:
        """启动事件发布线程"""
        def _publisher_loop() -> None:
            while not self._event_stop.is_set():
                try:
                    topic, args, kwargs = self._event_queue.get(timeout=0.5)
                    try:
                        cherrypy.engine.publish(topic, *args, **kwargs)
                    except Exception as e:
                        logger.error(f"Event publish error: {e}")
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.exception(f"Event publisher loop error: {e}")
        self._event_thread = threading.Thread(target=_publisher_loop, daemon=True, name="EventPublisher")
        self._event_thread.start()

    def _stop_event_publisher(self) -> None:
        """停止事件发布线程"""
        self._event_stop.set()
        if self._event_thread and self._event_thread.is_alive():
            self._event_thread.join(timeout=2.0)
            if self._event_thread.is_alive():
                logger.warning("Event publisher thread did not stop in time")
        # 清空队列
        while not self._event_queue.empty():
            try:
                self._event_queue.get_nowait()
            except queue.Empty:
                break

    def _publish_event(self, topic: str, *args, **kwargs) -> None:
        """将事件放入队列，由专用线程发布（非阻塞）"""
        self._event_queue.put((topic, args, kwargs))

    # ==================== 套接字路径生成 ====================
    def _generate_socket_path(self) -> str:
        return self.temp_manager.create()

    # ==================== 资源清理协调 ====================

    def _wait_cleanup_finish(self) -> None:
        """阻塞直到清理完成（如果正在清理）或立即返回（若已清理）"""
        with self._cleanup_lock:
            if self._is_cleaning and threading.current_thread() != self._cleanup_thread:
                self._cleanup_done.wait()

    def _cleanup_resources(self) -> None:
        """
        内部清理函数，幂等，线程安全。
        严格按锁顺序：_cleanup_lock -> _process_lock -> _ipc_lock -> _state_lock
        """
        with self._cleanup_lock:
            if self._cleanup_done.is_set():
                return
            if self._is_cleaning:
                self._cleanup_done.wait()
                return
            self._is_cleaning = True
            self._cleanup_done.clear()
            self._cleanup_thread = threading.current_thread()
            logger.info("Starting resource cleanup")

        try:
            self._stop_event.set()

            # ----- 停止心跳和IPC线程 -----
            with self._ipc_lock:
                if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                    self.heartbeat_thread.join(timeout=1.0)
                    if self.heartbeat_thread.is_alive():
                        logger.warning("Heartbeat thread did not stop in time")
                    self.heartbeat_thread = None
                if self.ipc_thread and self.ipc_thread.is_alive():
                    self.ipc_thread.join(timeout=2.0)
                    if self.ipc_thread.is_alive():
                        logger.warning("IPC thread still alive after join")
                    self.ipc_thread = None

            # ----- 关闭socket -----
            with self._ipc_lock:
                if self.ipc_sock:
                    try:
                        self.ipc_sock.close()
                    except:
                        pass
                    self.ipc_sock = None
                if hasattr(self, 'ipc_sock_file') and self.ipc_sock_file:
                    try:
                        self.ipc_sock_file.close()
                    except:
                        pass
                    self.ipc_sock_file = None
                self._ipc_connected.clear()

            # ----- 终止进程 -----
            with self._process_lock:
                proc = self.process
                self.process = None
            if proc:
                try:
                    if self._ipc_connected.is_set():
                        self._send_command(["quit"])
                        proc.wait(timeout=2.0)
                    if proc.poll() is None:
                        proc.terminate()
                        proc.wait(timeout=PROCESS_TERMINATE_TIMEOUT)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.wait()
                except Exception as e:
                    logger.warning(f"Error terminating process: {e}")
                while proc.poll() is None:
                    time.sleep(0.1)
            self._process_exit_event.set()

            # ----- 删除字幕文件（已由temp_manager管理） -----
            self._subtitle_file = None
            self._playlist_file = None

            # 清理临时目录（包括所有临时文件、字幕副本、播放列表等）
            self.temp_manager.cleanup()
            self.mpv_sock = None

            with self._ipc_lock:
                self._reconnect_attempts = 0

            # ----- 更新播放状态 -----
            with self._state_lock:
                self._playing = False
                self._position = 0.0
                self._pause = False
            self.set_state_transport("STOPPED")
            self._publish_event("renderer_av_stop")
            logger.info("Media stopped and resources cleaned")

        except Exception as e:
            logger.exception("Unexpected error during cleanup")
        finally:
            with self._cleanup_lock:
                self._is_cleaning = False
                self._cleanup_done.set()
                self._cleanup_thread = None

    # ==================== IPC 核心方法 ====================

    def _send_command(self, command: List[Any], timeout: float = 2.0) -> bool:
        """发送IPC命令，仅需 _ipc_lock"""
        with self._ipc_lock:
            if self.ipc_sock is None:
                logger.error("IPC socket not connected")
                return False
            data = {"command": command}
            msg = json.dumps(data) + "\n"
            try:
                if os.name == 'nt':
                    self.ipc_sock.send_bytes(msg.encode('utf-8'))
                else:
                    self.ipc_sock.sendall(msg.encode('utf-8'))
                logger.debug(f"Command sent: {command}")
                return True
            except (BrokenPipeError, ConnectionResetError, OSError) as e:
                logger.warning(f"IPC send failed: {e}")
                self._ipc_connected.clear()
                return False
            except Exception as e:
                logger.error(f"Unexpected send error: {e}")
                return False

    def _ipc_loop(self) -> None:
        """IPC 主循环，使用 _ipc_lock 和 _process_lock"""
        with self._ipc_lock:
            self._ipc_connected.clear()
            self._reconnect_attempts = 0
        self._process_exit_event.clear()

        while not self._stop_event.is_set() and not self._is_process_dead():
            connected = False
            for attempt in range(1, IPC_RETRY_COUNT + 1):
                if self._stop_event.is_set() or self._is_process_dead():
                    if self._stop_event.is_set():
                        logger.debug("Stop event set, aborting IPC connection")
                    return

                try:
                    if os.name == 'nt':
                        import _winapi
                        from multiprocessing.connection import PipeConnection
                        try:
                            _winapi.WaitNamedPipe(self.mpv_sock, int(IPC_CONNECT_TIMEOUT * 1000))
                        except Exception:
                            pass
                        handle = _winapi.CreateFile(
                            self.mpv_sock,
                            _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
                            0, _winapi.NULL, _winapi.OPEN_EXISTING,
                            _winapi.FILE_FLAG_OVERLAPPED, _winapi.NULL
                        )
                        with self._ipc_lock:
                            self.ipc_sock = PipeConnection(handle)
                    else:
                        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                        sock.settimeout(IPC_CONNECT_TIMEOUT)
                        sock.connect(self.mpv_sock)
                        sock.settimeout(None)
                        with self._ipc_lock:
                            self.ipc_sock = sock
                    connected = True
                    with self._ipc_lock:
                        self._ipc_connected.set()
                        self._reconnect_attempts = 0
                    # 重连成功后重新观察属性
                    self._observe_properties()
                    with self._state_lock:
                        self._last_update_time.clear()
                    logger.info("IPC connected to MPV")
                    break
                except Exception as e:
                    logger.debug(f"IPC connection attempt {attempt}: {e}")
                    time.sleep(min(IPC_RETRY_BASE * (2 ** (attempt-1)), 5.0))
                    continue

            if not connected:
                logger.error("IPC connection failed after all attempts, terminating MPV")
                self._executor.submit(self._cleanup_resources)
                return

            # 连接成功，进入主循环
            with self._ipc_lock:
                ipc_sock = self.ipc_sock
            if os.name != 'nt':
                self.ipc_sock_file = ipc_sock.makefile('r')

            self._start_heartbeat()

            buffer = b""
            while not self._stop_event.is_set() and not self._is_process_dead():
                try:
                    if os.name == 'nt':
                        with self._ipc_lock:
                            data = self.ipc_sock.recv_bytes(1048576)
                        if not data:
                            break
                        buffer += data
                        while b"\n" in buffer:
                            line, buffer = buffer.split(b"\n", 1)
                            if line:
                                self._handle_ipc_message(line.decode('utf-8'))
                    else:
                        line = self.ipc_sock_file.readline()
                        if not line:
                            break
                        self._handle_ipc_message(line.strip())
                except socket.timeout:
                    continue
                except (ConnectionResetError, BrokenPipeError, OSError) as e:
                    logger.debug(f"IPC recv error: {e}")
                    break
                except Exception as e:
                    logger.exception(f"IPC recv unexpected: {e}")
                    break

            # --- 重连逻辑 ---
            with self._ipc_lock:
                reconnect_count = self._reconnect_attempts
            if (not self._stop_event.is_set() and
                not self._is_process_dead() and
                reconnect_count < IPC_RETRY_COUNT):
                with self._ipc_lock:
                    self._reconnect_attempts += 1
                    new_count = self._reconnect_attempts
                wait_time = min(IPC_RETRY_BASE * (2 ** (new_count - 1)), 5.0)
                logger.info(f"IPC lost. Reconnecting attempt {new_count} in {wait_time:.2f}s...")
                with self._ipc_lock:
                    if self.ipc_sock:
                        try:
                            self.ipc_sock.close()
                        except:
                            pass
                        self.ipc_sock = None
                    if hasattr(self, 'ipc_sock_file') and self.ipc_sock_file:
                        try:
                            self.ipc_sock_file.close()
                        except:
                            pass
                        self.ipc_sock_file = None
                    self._ipc_connected.clear()
                time.sleep(wait_time)
                continue
            elif reconnect_count >= IPC_RETRY_COUNT:
                logger.error("IPC reconnect exhausted. Cleaning up.")
                self._executor.submit(self._cleanup_resources)
                return
            else:
                break

        if self._is_process_dead() and not self._cleanup_done.is_set():
            logger.info("Process dead, triggering cleanup")
            self._executor.submit(self._cleanup_resources)

        with self._ipc_lock:
            self._ipc_connected.clear()
        logger.info("IPC loop ended")

    def _start_heartbeat(self) -> None:
        """启动心跳线程，带失败计数"""
        with self._ipc_lock:
            if self.heartbeat_thread and self.heartbeat_thread.is_alive():
                return

        def heartbeat() -> None:
            failure_count = 0
            while not self._stop_event.is_set() and not self._is_process_dead():
                if self._stop_event.wait(HEARTBEAT_INTERVAL):
                    break
                if not self._ipc_connected.is_set():
                    continue
                if self._send_command(["get_property", "pause"], timeout=1.0):
                    failure_count = 0  # 成功，重置计数
                else:
                    failure_count += 1
                    logger.warning(f"Heartbeat failed {failure_count}/{HEARTBEAT_FAILURE_THRESHOLD}")
                    with self._ipc_lock:
                        self._ipc_connected.clear()
                    if failure_count >= HEARTBEAT_FAILURE_THRESHOLD:
                        logger.error("Heartbeat failure threshold reached, killing MPV process.")
                        # 主动终止进程并清理
                        self._executor.submit(self._cleanup_resources)
                        break
        with self._ipc_lock:
            self.heartbeat_thread = threading.Thread(target=heartbeat, daemon=True, name="MPVHeartbeat")
            self.heartbeat_thread.start()

    def _observe_properties(self) -> None:
        properties: List[Tuple[str, int]] = [("pause", 1), ("time-pos", 2), ("duration", 3), ("volume", 4)]
        for prop, pid in properties:
            self._send_command(["observe_property", pid, prop])

    def _should_update_state(self, prop_name: str) -> bool:
        now = time.time()
        with self._state_lock:
            last = self._last_update_time.get(prop_name, 0)
            if now - last >= POSITION_UPDATE_INTERVAL:
                self._last_update_time[prop_name] = now
                return True
            return False

    def _handle_ipc_message(self, msg: str) -> None:
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {msg}")
            return

        # 只使用 _state_lock 保护状态变量
        if "id" in data and "data" in data:
            pid = data["id"]
            value = data.get("data")
            with self._state_lock:
                if pid == 1:      # pause
                    self._pause = bool(value)
                    if self._pause:
                        self.set_state_pause()
                    else:
                        self.set_state_play()
                elif pid == 2:    # time-pos
                    if value is not None:
                        self._position = float(value)
                        if self._should_update_state("position"):
                            self._update_position_state_locked()
                elif pid == 3:    # duration
                    if value is not None:
                        self._duration = float(value)
                        if self._should_update_state("duration"):
                            self._update_duration_state_locked()
                elif pid == 4:    # volume
                    if value is not None:
                        self._volume = int(value)
                        self.set_state_volume(self._volume)
        elif "event" in data:
            event = data["event"]
            if event == "start-file":
                with self._state_lock:
                    self._playing = True
                    self._last_update_time.clear()
                self._publish_event("renderer_av_uri", self.protocol.get_state_url())
            elif event == "end-file":
                with self._state_lock:
                    self._playing = False
                    self._position = 0.0
                self._publish_event("renderer_av_stop")
            elif event == "playback-restart":
                with self._state_lock:
                    if self._pause:
                        self.set_state_pause()
                    else:
                        self.set_state_play()
            elif event == "idle":
                with self._state_lock:
                    self._playing = False
                self.set_state_stop()

    def _update_position_state_locked(self) -> None:
        # 此方法在持有 _state_lock 时调用
        sec = int(self._position)
        pos_str = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
        self.set_state_position(pos_str)

    def _update_duration_state_locked(self) -> None:
        sec = int(self._duration)
        dur_str = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
        self.set_state_duration(dur_str)

    def _is_process_dead(self) -> bool:
        with self._process_lock:
            return self.process is None or self.process.poll() is not None

    def _start_ipc_thread(self) -> None:
        """启动 IPC 线程，确保旧线程停止"""
        with self._ipc_lock:
            if self.ipc_thread and self.ipc_thread.is_alive():
                self._stop_event.set()
                self.ipc_thread.join(timeout=2.0)
                if self.ipc_thread.is_alive():
                    logger.warning("Previous IPC thread did not stop")
            self._stop_event.clear()
            self.ipc_thread = threading.Thread(target=self._ipc_loop, daemon=True, name="MPVIPC")
            self.ipc_thread.start()

    # ==================== 统一启动逻辑 ====================
    def _launch_mpv(self, media: str, is_playlist: bool = False, start: int = 0) -> bool:
        """
        统一的 MPV 启动入口
        :param media: URL 或播放列表文件路径
        :param is_playlist: 是否为播放列表模式
        :param start: 起始索引（仅播放列表有效）
        """
        mpv_path = MpvFinder.find()
        if not mpv_path:
            if os.name == 'nt':
                subprocess.Popen(["notepad.exe", Setting.setting_path],
                               creationflags=subprocess.CREATE_NO_WINDOW)
            self._publish_event("app_notify", "Error",
                "MPV not found. Please set 'MPVplayer_Path' in config.")
            return False

        # 检查进程是否可复用（无缝切换）
        with self._process_lock:
            proc_ok = self.process is not None and self.process.poll() is None
        if proc_ok and self._ipc_connected.is_set():
            if is_playlist:
                cmd = ["loadlist", media, "replace"]
                if start > 0:
                    cmd.append(f"start={start}")
            else:
                cmd = ["loadfile", media, "replace"]
                if start:
                    cmd.append(f"start={start}")
            logger.info(f"Sending seamless switch: {cmd}")
            if self._send_command(cmd):
                logger.info(f"Switched to: {media}")
                self.set_state_transport("PLAYING")
                self._publish_event("renderer_av_uri", media)
                return True
            else:
                logger.warning("IPC command failed, fallback to restart")
                self._cleanup_resources()
                self._wait_cleanup_finish()

        # 完全重启
        self._wait_cleanup_finish()
        self._stop_event.clear()
        self.mpv_sock = self._generate_socket_path()

        # 构建命令行
        cmd = [mpv_path, "--fullscreen", f"--input-ipc-server={self.mpv_sock}",
               "--cache=yes", "--keep-open=yes", "--no-terminal"]

        if is_playlist:
            cmd.append(f"--playlist={media}")
        else:
            cmd.append(media)

        if self._subtitle_file and os.path.exists(self._subtitle_file):
            cmd.append(f"--sub-file={self._subtitle_file}")

        try:
            logger.info(f"Starting MPV: {cmd}")
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            with self._process_lock:
                self.process = proc
            self._start_ipc_thread()

            def monitor_process() -> None:
                proc.wait()
                exit_code = proc.poll()
                logger.info(f"MPV process exited with code {exit_code}")
                if not self._cleanup_done.is_set():
                    self._executor.submit(self._cleanup_resources)
                if exit_code != 0 and exit_code is not None:
                    logger.warning(f"MPV exited with error code {exit_code}")
                    self._publish_event("app_notify", "MPV Error", f"MPV exited with code {exit_code}")

            with self._process_lock:
                self.monitor_thread = threading.Thread(target=monitor_process, daemon=True, name="MPVMonitor")
                self.monitor_thread.start()

            self.set_state_transport("PLAYING")
            self._publish_event("renderer_av_uri", media)
            return True
        except Exception as e:
            logger.exception("Failed to start MPV")
            self._publish_event("app_notify", "Error", str(e))
            self._cleanup_resources()
            return False

    # ==================== DLNA 控制方法 ====================

    def set_media_stop(self) -> None:
        """停止播放"""
        if not self._cleanup_done.is_set():
            self._cleanup_resources()
        else:
            logger.debug("Already stopped")

    def set_media_pause(self) -> None:
        self._send_command(["set_property", "pause", True])

    def set_media_resume(self) -> None:
        self._send_command(["set_property", "pause", False])

    def set_media_volume(self, data: Union[int, str]) -> None:
        self._send_command(["set_property", "volume", int(data)])

    def set_media_position(self, data: str) -> None:
        try:
            if ":" in data:
                parts = data.split(":")
                if len(parts) == 3:
                    seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
                else:
                    seconds = float(data)
            else:
                seconds = float(data)
            self._send_command(["seek", seconds, "absolute"])
        except ValueError:
            logger.error(f"Invalid position format: {data}")

    # ==================== 外挂字幕支持（拷贝到临时目录） ====================
    def set_subtitle(self, file_path: Optional[str] = None) -> None:
        """
        设置或移除当前播放的字幕文件。
        :param file_path: 字幕文件路径，若为 None 或空字符串则移除所有字幕。
        注意：传入的文件会被复制到临时目录，以保证生命周期。
        """
        if file_path and os.path.isfile(file_path):
            # 复制字幕到临时目录
            try:
                copied_path = self.temp_manager.copy_file_to_temp(file_path, suffix=".srt")
                logger.info(f"Subtitle copied to temp: {copied_path}")
                self._subtitle_file = copied_path
            except Exception as e:
                logger.error(f"Failed to copy subtitle: {e}")
                self._publish_event("app_notify", "Error", f"Subtitle copy failed: {e}")
                return
        else:
            # 移除字幕，但保留副本供后续使用？这里选择清空
            self._subtitle_file = None
            logger.info("Subtitle removed for future playback")
            # 注意：旧的字幕副本会在 cleanup 时删除，无需额外操作

        # 如果当前正在播放且 IPC 已连接，动态加载/移除字幕
        if self._ipc_connected.is_set() and not self._is_process_dead():
            if self._subtitle_file and os.path.exists(self._subtitle_file):
                self._send_command(["sub_remove", "all"])
                if self._send_command(["sub_add", self._subtitle_file]):
                    logger.info(f"Subtitle added dynamically: {self._subtitle_file}")
                else:
                    logger.error(f"Failed to add subtitle: {self._subtitle_file}")
            else:
                self._send_command(["sub_remove", "all"])
                logger.info("All subtitles removed dynamically")

    # ==================== 无缝切换播放列表 ====================
    def set_media_playlist(self, urls: List[str], start: int = 0) -> None:
        """
        设置播放列表（支持无缝切换）。
        :param urls: 媒体 URL 列表（字符串列表）
        :param start: 起始索引（0-based），默认为 0
        """
        if not urls:
            logger.warning("Empty playlist, ignoring")
            return

        self._wait_cleanup_finish()

        # 生成临时 M3U 文件 (使用改进的 TempDirManager)
        playlist_content = "#EXTM3U\n" + "\n".join(urls)
        try:
            playlist_path = self.temp_manager.create_temp_file(playlist_content, suffix=".m3u")
            self._playlist_file = playlist_path
            logger.info(f"Playlist created: {playlist_path} ({len(urls)} items)")
        except Exception as e:
            logger.error(f"Failed to create playlist file: {e}")
            self._publish_event("app_notify", "Error", f"Playlist creation failed: {e}")
            return

        self._launch_mpv(playlist_path, is_playlist=True, start=start)

    # ==================== 原有 set_media_url（已适配统一启动） ====================
    def set_media_url(self, url: str, start: int = 0) -> None:
        """设置媒体 URL（支持无缝切换）"""
        self._wait_cleanup_finish()
        self._launch_mpv(url, is_playlist=False, start=start)

    def set_media_speed(self, data: Union[float, str]) -> None:
        speed = float(data)
        if MIN_SPEED <= speed <= MAX_SPEED:
            self._send_command(["set_property", "speed", speed])
        else:
            logger.warning(f"Speed {speed} out of range [{MIN_SPEED}, {MAX_SPEED}]")

    def set_media_mute(self, data: bool) -> None:
        self._send_command(["set_property", "mute", "yes" if data else "no"])

    # ==================== 生命周期 ====================

    def start(self) -> None:
        super().start()
        logger.info("MPVPlayer started")

    def stop(self) -> None:
        super().stop()
        self._wait_cleanup_finish()
        if not self._cleanup_done.is_set():
            self._cleanup_resources()
        # 停止事件发布线程
        self._stop_event_publisher()
        self._executor.shutdown(wait=True, timeout=5.0)
        logger.info("MPVPlayer stopped")

# ==================== 入口 ====================
if __name__ == "__main__":
    gui(MPVplayerRenderer())