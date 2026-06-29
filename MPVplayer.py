# Copyright (c) 2021 by xfangfang. All Rights Reserved.
# Modified for MPV proper support, fullscreen on start, with IPC control.
#
# Macast Metadata
# <macast.title>MPVPlayer Renderer</macast.title>
# <macast.renderer>MPVplayerRenderer</macast.renderer>
# <macast.platform>win32</macast.platform>
# <macast.version>0.6</macast.version>
# <macast.host_version>0.7</macast.host_version>
# <macast.author>xfangfang</macast.author>
# <macast.desc>MPVPlayer support for Macast, with full IPC control.</macast.desc>

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
import cherrypy
from enum import Enum
from contextlib import contextmanager

from macast import gui, Setting
from macast.renderer import Renderer
from macast.utils import SETTING_DIR

logger = logging.getLogger("MPVPlayer")
subtitle = os.path.join(SETTING_DIR, "macast.ass")


class SettingProperty(Enum):
    MPVplayer_Path = 0


def get_mpv_path():
    """查找 MPV 可执行文件路径"""
    path = Setting.get(SettingProperty.MPVplayer_Path, None)
    if path and os.path.exists(path):
        return path

    which = shutil.which("mpv.exe")
    if which:
        return which

    default_paths = [
        r"C:\Program Files\mpv\mpv.exe",
        r"D:\mpv\mpv.exe",
    ]
    for p in default_paths:
        if os.path.exists(p):
            return p

    logger.error("MPV executable not found. Please set 'MPVplayer_Path' in config.")
    return None


class MPVplayerRenderer(Renderer):
    def __init__(self):
        super(MPVplayerRenderer, self).__init__()
        self.process = None
        self.pid = None

        # IPC 相关
        self.ipc_sock = None
        self.ipc_thread = None
        self.ipc_running = False
        self.command_lock = threading.Lock()

        # 播放状态（由 IPC 实时更新）
        self._playing = False
        self._pause = False
        self._volume = 50
        self._position = 0      # 秒
        self._duration = 0      # 秒

        # IPC 套接字路径
        if os.name == 'nt':
            # Windows 使用命名管道
            self.mpv_sock = r"\\.\pipe\macast_mpv_{}".format(random.randint(0, 9999))
        else:
            # Unix 使用域套接字
            self.mpv_sock = "/tmp/macast_mpv_{}".format(random.randint(0, 9999))

    # ==================== IPC 核心方法 ====================

    def send_command(self, command):
        """向 MPV 发送 IPC 命令"""
        data = {"command": command}
        msg = json.dumps(data) + "\n"
        with self.command_lock:
            try:
                if os.name == 'nt':
                    self.ipc_sock.send(msg.encode())
                else:
                    self.ipc_sock.sendall(msg.encode())
                return True
            except Exception as e:
                logger.error(f"send_command failed: {e}")
                return False

    def _ipc_loop(self):
        """IPC 通信主循环：连接 MPV、收发消息"""
        self.ipc_running = True

        # 等待 MPV 进程启动并创建套接字
        while self.ipc_running and self.process and self.process.poll() is None:
            try:
                time.sleep(0.5)
                if os.name == 'nt':
                    # Windows 命名管道
                    import _winapi
                    from multiprocessing.connection import PipeConnection
                    handle = _winapi.CreateFile(
                        self.mpv_sock,
                        _winapi.GENERIC_READ | _winapi.GENERIC_WRITE,
                        0, _winapi.NULL, _winapi.OPEN_EXISTING,
                        _winapi.FILE_FLAG_OVERLAPPED, _winapi.NULL
                    )
                    self.ipc_sock = PipeConnection(handle)
                else:
                    # Unix 域套接字
                    self.ipc_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                    self.ipc_sock.connect(self.mpv_sock)

                logger.info("IPC connected to MPV")
                break
            except Exception as e:
                logger.debug(f"IPC connecting... {e}")
                continue
        else:
            logger.error("IPC: MPV process died before connection")
            return

        # 注册属性观察
        self._observe_properties()

        # 主循环：读取并处理消息
        buffer = b""
        while self.ipc_running and self.process and self.process.poll() is None:
            try:
                if os.name == 'nt':
                    data = self.ipc_sock.recv_bytes(1048576)
                else:
                    data = self.ipc_sock.recv(1048576)
                if not data:
                    break
                buffer += data
                # 按换行符分割消息
                while b"\n" in buffer:
                    line, buffer = buffer.split(b"\n", 1)
                    if line:
                        self._handle_ipc_message(line.decode())
            except Exception as e:
                logger.debug(f"IPC recv error: {e}")
                break

        self.ipc_running = False
        if self.ipc_sock:
            try:
                self.ipc_sock.close()
            except:
                pass
            self.ipc_sock = None
        logger.info("IPC loop ended")

    def _observe_properties(self):
        """注册需要监听的 MPV 属性"""
        properties = [
            ("pause", 1),
            ("time-pos", 2),
            ("duration", 3),
            ("volume", 4),
        ]
        for prop, id in properties:
            self.send_command(["observe_property", id, prop])

    def _handle_ipc_message(self, msg):
        """处理来自 MPV 的 IPC 消息"""
        try:
            data = json.loads(msg)
        except json.JSONDecodeError:
            logger.warning(f"Invalid JSON: {msg}")
            return

        # 处理属性变化
        if "id" in data and "data" in data:
            prop_id = data["id"]
            value = data["data"]
            if prop_id == 1:      # pause
                self._pause = value if value is not None else False
                if self._pause:
                    self.set_state_pause()
                else:
                    self.set_state_play()
            elif prop_id == 2:    # time-pos
                if value is not None:
                    self._position = float(value)
                    self._update_position_state()
            elif prop_id == 3:    # duration
                if value is not None:
                    self._duration = float(value)
                    self._update_duration_state()
            elif prop_id == 4:    # volume
                if value is not None:
                    self._volume = int(value)
                    self.set_state_volume(self._volume)

        # 处理事件
        elif "event" in data:
            event = data["event"]
            if event == "start-file":
                self._playing = True
                cherrypy.engine.publish("renderer_av_uri", self.protocol.get_state_url())
            elif event == "end-file":
                self._playing = False
                self._position = 0
                self.set_state_stop()
                cherrypy.engine.publish("renderer_av_stop")
            elif event == "playback-restart":
                if self._pause:
                    self.set_state_pause()
                else:
                    self.set_state_play()

    def _update_position_state(self):
        """更新播放进度状态"""
        sec = int(self._position)
        pos_str = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
        self.set_state_position(pos_str)

    def _update_duration_state(self):
        """更新总时长状态"""
        sec = int(self._duration)
        dur_str = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
        self.set_state_duration(dur_str)

    def _start_ipc_thread(self):
        """启动 IPC 线程"""
        if self.ipc_thread and self.ipc_thread.is_alive():
            return
        self.ipc_thread = threading.Thread(target=self._ipc_loop, daemon=True)
        self.ipc_thread.start()

    # ==================== DLNA 控制方法 ====================

    def set_media_stop(self):
        """停止播放"""
        self.send_command(["stop"])
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
            except Exception as e:
                logger.warning(f"Error terminating: {e}")
            self.process = None
            self.pid = None

        self.ipc_running = False
        if self.ipc_thread and self.ipc_thread.is_alive():
            self.ipc_thread.join(timeout=2)

        try:
            if os.path.exists(subtitle):
                os.remove(subtitle)
        except Exception as e:
            logger.warning(f"Failed to remove subtitle: {e}")

        self.set_state_transport("STOPPED")
        cherrypy.engine.publish("renderer_av_stop")

    def set_media_pause(self):
        """暂停"""
        self.send_command(["set_property", "pause", True])

    def set_media_resume(self):
        """恢复播放"""
        self.send_command(["set_property", "pause", False])

    def set_media_volume(self, data):
        """设置音量 (0-100)"""
        self.send_command(["set_property", "volume", data])

    def set_media_position(self, data):
        """跳转到指定位置 (格式: 00:00:00)"""
        # 解析时间字符串为秒
        parts = data.split(":")
        if len(parts) == 3:
            seconds = int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
            self.send_command(["seek", seconds, "absolute"])

    def set_media_url(self, url, start=0):
        """播放新 URL"""
        self.set_media_stop()

        mpv_path = get_mpv_path()
        if not mpv_path:
            subprocess.Popen(["notepad.exe", Setting.setting_path],
                           creationflags=subprocess.CREATE_NO_WINDOW)
            cherrypy.engine.publish("app_notify", "Error",
                "MPV not found. Please set 'MPVplayer_Path' in config.")
            return

        # 构建命令行
        cmd = [
            mpv_path,
            url,
            "--fullscreen",
            f"--input-ipc-server={self.mpv_sock}",   # 关键：启用 IPC
            "--cache=yes",
            "--keep-open=yes",
            "--no-terminal",
        ]
        if os.path.exists(subtitle):
            cmd.append(f"--sub-file={subtitle}")

        try:
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            self.pid = self.process.pid
            logger.info(f"MPV started with PID {self.pid}, IPC: {self.mpv_sock}")

            # 启动 IPC 线程
            self._start_ipc_thread()

            # 等待进程结束（在独立线程中运行，不阻塞主线程）
            def wait_for_process():
                self.process.wait()
                logger.info("MPV process exited")
                if self.process is not None and self.process.poll() is not None:
                    self.set_media_stop()

            threading.Thread(target=wait_for_process, daemon=True).start()

            self.set_state_transport("PLAYING")
            cherrypy.engine.publish("renderer_av_uri", url)

        except Exception as e:
            logger.exception("Failed to start MPV")
            cherrypy.engine.publish("app_notify", "Error", str(e))

    def set_media_speed(self, data):
        """设置播放速度"""
        self.send_command(["set_property", "speed", float(data)])

    def set_media_mute(self, data):
        """静音开关"""
        self.send_command(["set_property", "mute", "yes" if data else "no"])

    # ==================== 生命周期 ====================

    def start(self):
        super(MPVplayerRenderer, self).start()
        logger.info("MPVPlayer started")

    def stop(self):
        super(MPVplayerRenderer, self).stop()
        self.set_media_stop()
        logger.info("MPVPlayer stopped")


if __name__ == "__main__":
    gui(MPVplayerRenderer())