# Copyright (c) 2021 by xfangfang. All Rights Reserved.
# Optimized version with improved stability, thread safety, and cross-platform compatibility.
# Features:
#   - Seamless switching via loadfile replace (now uses wait_response=False to avoid timeout)
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
#   - State machine for cleanup safety
#   - IPC message processing offloaded to event loop
#   - Command response confirmation (optional, disabled for seamless switch)
#   - Position updates merged and throttled
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
import atexit
from enum import Enum
from typing import Optional, List, Dict, Any, Union, Callable, Tuple
from contextlib import suppress

import cherrypy
from macast import gui, Setting
from macast.renderer import Renderer
from macast.utils import SETTING_DIR

logger = logging.getLogger("MPVPlayer")

# ==================== 跨平台常量 ====================
IS_WINDOWS = sys.platform == 'win32'

# ==================== 常量 ====================
IPC_RETRY_COUNT = 10
IPC_RETRY_BASE = 0.5
IPC_CONNECT_TIMEOUT = 2.0
HEARTBEAT_INTERVAL = 5.0
PROCESS_TERMINATE_TIMEOUT = 3.0
MAX_SPEED = 2.0
MIN_SPEED = 0.1
POSITION_UPDATE_INTERVAL = 0.5
HEARTBEAT_FAILURE_THRESHOLD = 3
IPC_COMMAND_RETRY = 2
EVENT_QUEUE_MAXSIZE = 200
COMMAND_RESPONSE_TIMEOUT = 3.0

# ==================== 状态枚举 ====================
class RendererState(Enum):
    IDLE = 0
    RUNNING = 1
    CLEANING = 2
    STOPPED = 3

# ==================== MPV 路径查找器 ====================
class MpvFinder:
    _cache: Optional[str] = None

    @classmethod
    def find(cls) -> Optional[str]:
        if cls._cache is not None:
            return cls._cache

        path = Setting.get(SettingProperty.MPVplayer_Path, None)
        if path and os.path.isfile(path):
            cls._cache = path
            return path

        exe_name = "mpv.exe" if IS_WINDOWS else "mpv"
        which = shutil.which(exe_name)
        if which:
            cls._cache = which
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
                key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                                     r"SOFTWARE\Microsoft\Windows\CurrentVersion\App Paths\mpv.exe")
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
    MPVplayer_Path = "MPVplayer_Path"

    @classmethod
    def get(cls, key: str, default: Any = None) -> Any:
        return Setting.get(key.value, default)

# ==================== 临时目录管理（跨平台适配） ====================
class TempDirManager:
    def __init__(self) -> None:
        self.temp_dir: Optional[str] = None
        self.sock_path: Optional[str] = None
        self._files_to_cleanup: List[str] = []

    def create(self) -> str:
        if self.temp_dir is None:
            self.temp_dir = tempfile.mkdtemp(prefix="macast_mpv_")
        if self.sock_path is None:
            if IS_WINDOWS:
                self.sock_path = r"\\.\pipe\macast_mpv_{}".format(random.randint(0, 999999))
            else:
                self.sock_path = os.path.join(self.temp_dir, "mpv.sock")
        return self.sock_path

    def create_temp_file(self, content: str, suffix: str = ".m3u") -> str:
        if self.temp_dir is None:
            self.create()
        fd, path = tempfile.mkstemp(suffix=suffix, dir=self.temp_dir)
        with os.fdopen(fd, 'w', encoding='utf-8') as f:
            f.write(content)
        self._files_to_cleanup.append(path)
        return path

    def copy_file_to_temp(self, src_path: str, suffix: Optional[str] = None) -> str:
        if not os.path.isfile(src_path):
            raise FileNotFoundError(f"Source file not found: {src_path}")
        if self.temp_dir is None:
            self.create()
        base = os.path.basename(src_path)
        if suffix is None:
            _, ext = os.path.splitext(base)
            suffix = ext or ".tmp"
        fd, dst_path = tempfile.mkstemp(suffix=suffix, dir=self.temp_dir, prefix="sub_")
        os.close(fd)
        shutil.copy2(src_path, dst_path)
        self._files_to_cleanup.append(dst_path)
        return dst_path

    def cleanup(self) -> None:
        # 先删除文件，再删除目录
        for f in self._files_to_cleanup:
            if not os.path.exists(f):
                continue
            for attempt in range(5):
                try:
                    os.unlink(f)
                    break
                except PermissionError:
                    time.sleep(0.2)
                except Exception as e:
                    if attempt == 4:
                        logger.warning(f"Failed to delete temp file {f} after 5 attempts: {e}")
                    else:
                        time.sleep(0.2)
        self._files_to_cleanup.clear()

        if self.temp_dir and os.path.exists(self.temp_dir):
            for attempt in range(5):
                try:
                    shutil.rmtree(self.temp_dir, ignore_errors=True)
                    break
                except PermissionError:
                    time.sleep(0.2)
                except Exception as e:
                    if attempt == 4:
                        logger.warning(f"Failed to cleanup temp dir {self.temp_dir} after 5 attempts: {e}")
                    else:
                        time.sleep(0.2)
            self.temp_dir = None
            self.sock_path = None

# ==================== 主渲染器类 ====================
class MPVplayerRenderer(Renderer):
    def __init__(self) -> None:
        super().__init__()
        # ----- 锁 -----
        self._state_lock = threading.RLock()
        self._ipc_lock = threading.RLock()
        self._process_lock = threading.RLock()
        self._cleanup_lock = threading.RLock()

        self._stop_event = threading.Event()
        self._cleanup_done = threading.Event()
        self._cleanup_done.set()
        self._state = RendererState.IDLE

        # 进程和IPC
        self.process: Optional[subprocess.Popen] = None
        self.ipc_sock: Optional[socket.socket] = None
        self.ipc_sock_file = None
        self.ipc_thread: Optional[threading.Thread] = None
        self.heartbeat_thread: Optional[threading.Thread] = None
        self.monitor_thread: Optional[threading.Thread] = None
        self._process_exit_event = threading.Event()
        self._ipc_connected = threading.Event()
        self._reconnect_attempts: int = 0

        # 播放状态
        self._playing: bool = False
        self._pause: bool = False
        self._volume: int = 50
        self._position: float = 0.0
        self._duration: float = 0.0
        self._pending_position: Optional[float] = None
        self._last_pos_update: float = 0.0

        # 临时目录和套接字
        self.temp_manager = TempDirManager()
        self.mpv_sock: Optional[str] = None

        # 事件队列
        self._event_queue: queue.Queue = queue.Queue(maxsize=EVENT_QUEUE_MAXSIZE)
        self._event_stop = threading.Event()
        self._event_thread: Optional[threading.Thread] = None
        self._start_event_publisher()

        # 字幕和播放列表
        self._subtitle_file: Optional[str] = None
        self._playlist_file: Optional[str] = None

        # 命令响应确认（保留但无缝切换不使用）
        self._next_request_id = 0
        self._pending_requests: Dict[int, threading.Event] = {}
        self._request_lock = threading.Lock()

        atexit.register(self._atexit_cleanup)

    # ==================== 状态管理 ====================
    def _set_state(self, new_state: RendererState) -> None:
        with self._state_lock:
            if self._state != new_state:
                old = self._state
                self._state = new_state
                logger.debug(f"State changed: {old} -> {new_state}")

    def _is_state(self, state: RendererState) -> bool:
        with self._state_lock:
            return self._state == state

    # ==================== 事件发布 ====================
    def _start_event_publisher(self) -> None:
        def _publisher_loop() -> None:
            while not self._event_stop.is_set():
                try:
                    item = self._event_queue.get(timeout=0.5)
                    if item is None:
                        continue
                    topic, args = item
                    if topic == "ipc_event":
                        self._apply_ipc_event(args[0])
                    else:
                        cherrypy.engine.publish(topic, *args)
                except queue.Empty:
                    continue
                except Exception as e:
                    logger.exception(f"Event publisher loop error: {e}")
        self._event_thread = threading.Thread(target=_publisher_loop, daemon=True, name="EventPublisher")
        self._event_thread.start()

    def _stop_event_publisher(self) -> None:
        self._event_stop.set()
        if self._event_thread and self._event_thread.is_alive():
            self._event_thread.join(timeout=2.0)
            if self._event_thread.is_alive():
                logger.warning("Event publisher thread did not stop in time")
        while not self._event_queue.empty():
            with suppress(queue.Empty):
                self._event_queue.get_nowait()

    def _publish_event(self, topic: str, *args) -> None:
        try:
            self._event_queue.put_nowait((topic, args))
        except queue.Full:
            with suppress(queue.Empty):
                self._event_queue.get_nowait()
            self._event_queue.put((topic, args))
            logger.warning(f"Event queue full, dropped oldest event for {topic}")

    # ==================== 套接字路径 ====================
    def _generate_socket_path(self) -> str:
        return self.temp_manager.create()

    # ==================== 资源清理 ====================
    def _wait_cleanup_finish(self) -> None:
        self._cleanup_done.wait()

    def _atexit_cleanup(self) -> None:
        if not self._cleanup_done.is_set():
            self._do_cleanup()

    def _stop_threads(self) -> None:
        # 停止心跳（使用 stop_event）
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=2.0)
            if self.heartbeat_thread.is_alive():
                logger.warning("Heartbeat thread did not stop in time")
            self.heartbeat_thread = None
        if self.ipc_thread and self.ipc_thread.is_alive():
            self.ipc_thread.join(timeout=2.0)
            if self.ipc_thread.is_alive():
                logger.warning("IPC thread still alive after join")
            self.ipc_thread = None
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=1.0)

    def _close_socket(self) -> None:
        with self._ipc_lock:
            if self.ipc_sock:
                with suppress(Exception):
                    self.ipc_sock.close()
                self.ipc_sock = None
            if self.ipc_sock_file:
                with suppress(Exception):
                    self.ipc_sock_file.close()
                self.ipc_sock_file = None
            self._ipc_connected.clear()

    def _terminate_process(self) -> None:
        with self._process_lock:
            proc = self.process
            self.process = None
        if proc is None:
            return
        # 尝试优雅退出
        if self._ipc_connected.is_set():
            self._send_command(["quit"], wait_response=False)
            proc.wait(timeout=2.0)
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=PROCESS_TERMINATE_TIMEOUT)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
        # 确保进程退出
        while proc.poll() is None:
            time.sleep(0.1)
        logger.info(f"MPV process terminated with exit code {proc.returncode}", extra={"pid": proc.pid})
        self._process_exit_event.set()

    def _cleanup_temp_files(self) -> None:
        self._subtitle_file = None
        self._playlist_file = None
        self.temp_manager.cleanup()
        self.mpv_sock = None
        with self._ipc_lock:
            self._reconnect_attempts = 0

    def _reset_play_state(self) -> None:
        with self._state_lock:
            self._playing = False
            self._position = 0.0
            self._pause = False
            self._pending_position = None
        self.set_state_transport("STOPPED")
        self._publish_event("renderer_av_stop")

    def _do_cleanup(self) -> None:
        """同步执行完整的资源清理，保证线程安全"""
        with self._cleanup_lock:
            if self._cleanup_done.is_set():
                return
            if self._is_state(RendererState.CLEANING):
                self._cleanup_done.wait()
                return
            self._set_state(RendererState.CLEANING)
            self._cleanup_done.clear()
            logger.info("Starting resource cleanup")

        try:
            self._stop_event.set()
            # 1. 终止进程
            self._terminate_process()
            # 2. 关闭socket
            self._close_socket()
            # 3. 停止所有线程
            self._stop_threads()
            # 4. 清理临时文件
            self._cleanup_temp_files()
            # 5. 重置播放状态
            self._reset_play_state()
            logger.info("Resource cleanup completed")
        except Exception as e:
            logger.exception("Unexpected error during cleanup")
        finally:
            with self._cleanup_lock:
                self._cleanup_done.set()
                self._set_state(RendererState.IDLE)

    def _schedule_cleanup(self) -> None:
        """外部触发清理（同步调用，但防止重入）"""
        if self._cleanup_done.is_set() or self._is_state(RendererState.CLEANING):
            return
        self._do_cleanup()

    # ==================== IPC 核心 ====================
    def _send_command(self, command: List[Any], timeout: float = 2.0,
                      retry: int = 0, wait_response: bool = False) -> bool:
        """
        发送IPC命令。若wait_response为True，则等待MPV返回对应的响应（通过request_id）。
        仅对关键命令（如loadfile）启用。
        """
        attempt = 0
        max_attempts = retry + 1
        request_id = None
        if wait_response:
            with self._request_lock:
                request_id = self._next_request_id
                self._next_request_id += 1
                if self._next_request_id > 1000000:
                    self._next_request_id = 0
                evt = threading.Event()
                self._pending_requests[request_id] = evt

        while attempt < max_attempts:
            with self._ipc_lock:
                if self.ipc_sock is None:
                    logger.error("IPC socket not connected")
                    return False
                data = {"command": command}
                if request_id is not None:
                    data["request_id"] = request_id
                msg = json.dumps(data) + "\n"
                try:
                    if IS_WINDOWS:
                        self.ipc_sock.send_bytes(msg.encode('utf-8'))
                    else:
                        self.ipc_sock.sendall(msg.encode('utf-8'))
                    logger.debug(f"Command sent: {command}")
                    break
                except (BrokenPipeError, ConnectionResetError, OSError) as e:
                    logger.warning(f"IPC send failed: {e}")
                    self._ipc_connected.clear()
                    attempt += 1
                    if attempt < max_attempts:
                        time.sleep(0.2)
                    continue
                except Exception as e:
                    logger.error(f"Unexpected send error: {e}")
                    return False
        else:
            logger.error(f"Command failed after {max_attempts} attempts: {command}")
            return False

        if wait_response and request_id is not None:
            evt = self._pending_requests.get(request_id)
            if evt is None:
                return False
            success = evt.wait(timeout)
            with self._request_lock:
                self._pending_requests.pop(request_id, None)
            if not success:
                logger.warning(f"Command {command} response timeout")
                return False
            # 检查响应数据是否表示成功（可选）
        return True

    def _create_ipc_connection(self) -> Optional[socket.socket]:
        """创建跨平台IPC连接，返回socket对象或None"""
        try:
            if IS_WINDOWS:
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
                return PipeConnection(handle)
            else:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                sock.connect(self.mpv_sock)
                return sock
        except Exception as e:
            logger.debug(f"IPC connection error: {e}")
            return None

    def _connect_to_mpv(self) -> bool:
        for attempt in range(1, IPC_RETRY_COUNT + 1):
            if self._stop_event.is_set():
                return False
            sock = self._create_ipc_connection()
            if sock is not None:
                with self._ipc_lock:
                    self.ipc_sock = sock
                    self._ipc_connected.set()
                    self._reconnect_attempts = 0
                self._observe_properties()
                with self._state_lock:
                    self._pending_position = None
                    self._last_pos_update = 0.0
                logger.info("IPC connected to MPV")
                return True
            time.sleep(min(IPC_RETRY_BASE * (2 ** (attempt - 1)), 5.0))
        logger.error("IPC connection failed after all attempts")
        return False

    def _process_messages(self) -> bool:
        """处理IPC消息，返回False表示连接断开需要重连"""
        with self._ipc_lock:
            ipc_sock = self.ipc_sock
        if not IS_WINDOWS:
            self.ipc_sock_file = ipc_sock.makefile('r')

        buffer = b""
        while not self._stop_event.is_set() and not self._is_process_dead():
            try:
                if IS_WINDOWS:
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
                    try:
                        line = self.ipc_sock_file.readline()
                    except socket.timeout:
                        continue
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
        return False

    def _handle_disconnect_and_retry(self) -> None:
        if self._stop_event.is_set() or self._is_process_dead():
            return
        with self._ipc_lock:
            self._reconnect_attempts += 1
            attempt = self._reconnect_attempts
        if attempt >= IPC_RETRY_COUNT:
            logger.error("IPC reconnect exhausted. Cleaning up.")
            self._schedule_cleanup()
            return
        wait_time = min(IPC_RETRY_BASE * (2 ** (attempt - 1)), 5.0)
        logger.info(f"IPC lost. Reconnecting attempt {attempt} in {wait_time:.2f}s...")
        with self._ipc_lock:
            if self.ipc_sock:
                with suppress(Exception):
                    self.ipc_sock.close()
                self.ipc_sock = None
            if self.ipc_sock_file:
                with suppress(Exception):
                    self.ipc_sock_file.close()
                self.ipc_sock_file = None
            self._ipc_connected.clear()
        time.sleep(wait_time)

    def _ipc_loop(self) -> None:
        with self._ipc_lock:
            self._ipc_connected.clear()
            self._reconnect_attempts = 0
        self._process_exit_event.clear()

        while not self._stop_event.is_set() and not self._is_process_dead():
            if not self._connect_to_mpv():
                if self._stop_event.is_set() or self._is_process_dead():
                    break
                time.sleep(1.0)
                continue

            self._start_heartbeat()
            self._process_messages()

            if self._stop_event.is_set() or self._is_process_dead():
                break
            self._handle_disconnect_and_retry()

        if self._is_process_dead() and not self._cleanup_done.is_set():
            logger.info("Process dead, triggering cleanup")
            self._schedule_cleanup()

        with self._ipc_lock:
            self._ipc_connected.clear()
        logger.info("IPC loop ended")

    def _start_ipc_thread(self) -> None:
        with self._ipc_lock:
            if self.ipc_thread and self.ipc_thread.is_alive():
                self._stop_event.set()
                self.ipc_thread.join(timeout=2.0)
            self._stop_event.clear()
            self.ipc_thread = threading.Thread(target=self._ipc_loop, daemon=True, name="MPVIPC")
            self.ipc_thread.start()

    def _start_heartbeat(self) -> None:
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=1.0)
            if self.heartbeat_thread.is_alive():
                logger.warning("Old heartbeat thread did not stop in time")
        self.heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name="MPVHeartbeat"
        )
        self.heartbeat_thread.start()

    def _heartbeat_loop(self) -> None:
        failure_count = 0
        while not self._stop_event.is_set() and not self._is_process_dead():
            if self._stop_event.wait(HEARTBEAT_INTERVAL):
                break
            if not self._ipc_connected.is_set():
                continue
            if self._send_command(["get_property", "version"], timeout=0.5, wait_response=False):
                failure_count = 0
            else:
                failure_count += 1
                logger.warning(f"Heartbeat failed {failure_count}/{HEARTBEAT_FAILURE_THRESHOLD}")
                with self._ipc_lock:
                    self._ipc_connected.clear()
                if failure_count >= HEARTBEAT_FAILURE_THRESHOLD:
                    logger.error("Heartbeat failure threshold reached, killing MPV process.")
                    self._schedule_cleanup()
                    break

    def _observe_properties(self) -> None:
        properties: List[Tuple[str, int]] = [("pause", 1), ("time-pos", 2), ("duration", 3), ("volume", 4)]
        for prop, pid in properties:
            self._send_command(["observe_property", pid, prop], wait_response=False)

    # ==================== IPC 消息处理（解耦） ====================
    def _handle_ipc_message(self, msg: str) -> None:
        """IPC线程仅负责解析并将消息入队"""
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {msg}")
            return
        # 将原始数据放入事件队列，由发布线程处理
        self._event_queue.put(("ipc_event", data))

    def _apply_ipc_event(self, data: Dict[str, Any]) -> None:
        """在事件发布线程中处理IPC消息，更新状态并发布事件"""
        # 检查是否为命令响应
        if "request_id" in data:
            req_id = data["request_id"]
            with self._request_lock:
                evt = self._pending_requests.pop(req_id, None)
            if evt:
                evt.set()
            return

        # 处理属性更新
        if "id" in data and "data" in data:
            pid = data["id"]
            value = data.get("data")
            with self._state_lock:
                if pid == 1:  # pause
                    self._pause = bool(value)
                    if self._pause:
                        self.set_state_pause()
                    else:
                        self.set_state_play()
                elif pid == 2:  # time-pos
                    if value is not None:
                        self._pending_position = float(value)
                        # 延迟更新，由发布循环定时刷新
                elif pid == 3:  # duration
                    if value is not None:
                        self._duration = float(value)
                        self._update_duration_state()
                elif pid == 4:  # volume
                    if value is not None:
                        self._volume = int(value)
                        self.set_state_volume(self._volume)
            return

        # 处理事件
        if "event" in data:
            event = data["event"]
            if event == "start-file":
                with self._state_lock:
                    self._playing = True
                    self._pending_position = None
                    self._last_pos_update = 0.0
                self._publish_event("renderer_av_uri", self.protocol.get_state_url())
            elif event == "end-file":
                with self._state_lock:
                    self._playing = False
                    self._position = 0.0
                    self._pending_position = None
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

        # 定期刷新位置（每0.5秒）
        self._flush_position_update()

    def _flush_position_update(self) -> None:
        """检查并更新位置状态（节流）"""
        now = time.monotonic()
        with self._state_lock:
            if self._pending_position is not None and now - self._last_pos_update >= POSITION_UPDATE_INTERVAL:
                self._position = self._pending_position
                self._update_position_state()
                self._last_pos_update = now

    def _update_position_state(self) -> None:
        with self._state_lock:
            sec = int(self._position)
            pos_str = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
            self.set_state_position(pos_str)

    def _update_duration_state(self) -> None:
        with self._state_lock:
            sec = int(self._duration)
            dur_str = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
            self.set_state_duration(dur_str)

    def _is_process_dead(self) -> bool:
        with self._process_lock:
            return self.process is None or self.process.poll() is not None

    # ==================== 启动和停止 ====================
    def _build_mpv_command(self, media: str, is_playlist: bool) -> List[str]:
        cmd = [MpvFinder.find(), "--fullscreen", f"--input-ipc-server={self.mpv_sock}",
               "--cache=yes", "--keep-open=yes", "--no-terminal"]
        if is_playlist:
            cmd.append(f"--playlist={media}")
        else:
            cmd.append(media)
        if self._subtitle_file and os.path.exists(self._subtitle_file):
            cmd.append(f"--sub-file={self._subtitle_file}")
        return cmd

    def _launch_mpv(self, media: str, is_playlist: bool = False, start: int = 0) -> bool:
        mpv_path = MpvFinder.find()
        if not mpv_path:
            if IS_WINDOWS:
                subprocess.Popen(["notepad.exe", Setting.setting_path],
                                 creationflags=subprocess.CREATE_NO_WINDOW)
            self._publish_event("app_notify", "Error",
                                "MPV not found. Please set 'MPVplayer_Path' in config.")
            return False

        # 尝试无缝切换
        with self._process_lock:
            proc_ok = self.process is not None and self.process.poll() is None
        if proc_ok and self._ipc_connected.is_set():
            if is_playlist:
                cmd = ["loadlist", media, "replace"]
                # loadlist 不支持 start 参数，需额外 seek
            else:
                cmd = ["loadfile", media, "replace"]
                if start > 0:
                    cmd.append(f"start={start}")
            logger.info(f"Sending seamless switch: {cmd}")
            # 【关键修复】不等待响应，避免超时导致误判失败，保持与原版一致的行为
            if self._send_command(cmd, retry=IPC_COMMAND_RETRY, wait_response=False):
                logger.info(f"Switched to: {media}")
                # 若是播放列表且需要跳转，发送 seek
                if is_playlist and start > 0:
                    self._send_command(["seek", start, "absolute"], wait_response=False)
                self.set_state_transport("PLAYING")
                self._publish_event("renderer_av_uri", media)
                return True
            else:
                logger.warning("IPC command failed, fallback to restart")
                self._schedule_cleanup()
                self._wait_cleanup_finish()

        # 完全重启
        self._wait_cleanup_finish()
        self._stop_event.clear()
        self.mpv_sock = self._generate_socket_path()
        cmd = self._build_mpv_command(media, is_playlist)

        try:
            logger.info(f"Starting MPV: {cmd}")
            proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            with self._process_lock:
                self.process = proc
            self._start_ipc_thread()

            def monitor_process() -> None:
                proc.wait()
                exit_code = proc.poll()
                logger.info(f"MPV process exited with code {exit_code}", extra={"pid": proc.pid})
                if not self._cleanup_done.is_set():
                    self._schedule_cleanup()
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
            self._schedule_cleanup()
            return False

    # ==================== DLNA 控制方法 ====================
    def set_media_stop(self) -> None:
        if not self._cleanup_done.is_set():
            self._schedule_cleanup()
        else:
            logger.debug("Already stopped")

    def set_media_pause(self) -> None:
        self._send_command(["set_property", "pause", True], wait_response=False)

    def set_media_resume(self) -> None:
        self._send_command(["set_property", "pause", False], wait_response=False)

    def set_media_volume(self, data: Union[int, str]) -> None:
        self._send_command(["set_property", "volume", int(data)], wait_response=False)

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
            self._send_command(["seek", seconds, "absolute"], wait_response=False)
        except ValueError:
            logger.error(f"Invalid position format: {data}")

    def set_subtitle(self, file_path: Optional[str] = None) -> None:
        if file_path and os.path.isfile(file_path):
            try:
                copied_path = self.temp_manager.copy_file_to_temp(file_path, suffix=".srt")
                logger.info(f"Subtitle copied to temp: {copied_path}")
                self._subtitle_file = copied_path
            except Exception as e:
                logger.error(f"Failed to copy subtitle: {e}")
                self._publish_event("app_notify", "Error", f"Subtitle copy failed: {e}")
                return
        else:
            self._subtitle_file = None
            logger.info("Subtitle removed for future playback")

        if self._ipc_connected.is_set() and not self._is_process_dead():
            if self._subtitle_file and os.path.exists(self._subtitle_file):
                self._send_command(["sub_remove", "all"], wait_response=False)
                if self._send_command(["sub_add", self._subtitle_file], wait_response=False):
                    logger.info(f"Subtitle added dynamically: {self._subtitle_file}")
                else:
                    logger.error(f"Failed to add subtitle: {self._subtitle_file}")
            else:
                self._send_command(["sub_remove", "all"], wait_response=False)
                logger.info("All subtitles removed dynamically")

    def set_media_playlist(self, urls: List[str], start: int = 0) -> None:
        if not urls:
            logger.warning("Empty playlist, ignoring")
            return

        self._wait_cleanup_finish()
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

    def set_media_url(self, url: str, start: int = 0) -> None:
        self._wait_cleanup_finish()
        self._launch_mpv(url, is_playlist=False, start=start)

    def set_media_speed(self, data: Union[float, str]) -> None:
        speed = float(data)
        if MIN_SPEED <= speed <= MAX_SPEED:
            self._send_command(["set_property", "speed", speed], wait_response=False)
        else:
            logger.warning(f"Speed {speed} out of range [{MIN_SPEED}, {MAX_SPEED}]")

    def set_media_mute(self, data: bool) -> None:
        self._send_command(["set_property", "mute", "yes" if data else "no"], wait_response=False)

    def start(self) -> None:
        super().start()
        self._set_state(RendererState.RUNNING)
        logger.info("MPVPlayer started")

    def stop(self) -> None:
        super().stop()
        self._set_state(RendererState.STOPPED)
        self._stop_event.set()
        self._wait_cleanup_finish()
        if not self._cleanup_done.is_set():
            self._do_cleanup()
        self._stop_event_publisher()
        logger.info("MPVPlayer stopped")

# ==================== 入口 ====================
if __name__ == "__main__":
    gui(MPVplayerRenderer())