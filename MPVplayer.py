# Copyright (c) 2021 by xfangfang. All Rights Reserved.
# Modified for MPV proper support, fullscreen on start.
#
# Macast Metadata
# <macast.title>MPVPlayer Renderer</macast.title>
# <macast.renderer>MPVplayerRenderer</macast.renderer>
# <macast.platform>win32</macast.platform>
# <macast.version>0.5</macast.version>
# <macast.host_version>0.7</macast.host_version>
# <macast.author>xfangfang</macast.author>
# <macast.desc>MPVPlayer support for Macast, improved version with fullscreen.</macast.desc>

import os
import time
import logging
import shutil
import threading
import subprocess
import cherrypy
from enum import Enum
from contextlib import contextmanager

from macast import gui, Setting
from macast.renderer import Renderer
from macast.utils import SETTING_DIR

logger = logging.getLogger("MPVPlayer")
subtitle = os.path.join(SETTING_DIR, "macast.ass")   # 可选字幕

class SettingProperty(Enum):
    MPVplayer_Path = 0   # 用户自定义路径

def get_mpv_path():
    """查找 MPV 可执行文件路径，优先级：
       1. 用户配置项（Setting）
       2. 系统 PATH 环境变量中的 mpv.exe
       3. 常见安装目录（可选，此处仅作示例）
    """
    # 1. 从 Macast 配置读取
    path = Setting.get(SettingProperty.MPVplayer_Path, None)
    if path and os.path.exists(path):
        return path

    # 2. 查找 PATH
    which = shutil.which("mpv.exe")
    if which:
        return which

    # 3. 备选：常见默认路径（按需添加）
    default_paths = [
        r"C:\Program Files\mpv\mpv.exe",
        r"D:\mpv\mpv.exe",
    ]
    for p in default_paths:
        if os.path.exists(p):
            return p

    # 未找到，记录错误并返回 None
    logger.error("MPV executable not found. Please set 'MPVplayer_Path' in config.")
    return None

class MPVplayerRenderer(Renderer):
    def __init__(self):
        super(MPVplayerRenderer, self).__init__()
        self.pid = None          # 播放器进程 ID
        self.process = None      # Popen 对象，便于管理
        self.start_position = 0  # 当前模拟进度（秒）
        self.position_thread = None
        self.position_running = False

    def _position_tick(self):
        """模拟进度更新线程，仅在播放期间运行"""
        while self.position_running:
            time.sleep(1)
            self.start_position += 1
            sec = self.start_position
            position = f"{sec//3600:02d}:{(sec%3600)//60:02d}:{sec%60:02d}"
            self.set_state_position(position)

    def _start_position_thread(self):
        """启动或重启进度模拟线程"""
        self._stop_position_thread()   # 先停旧线程
        self.position_running = True
        self.position_thread = threading.Thread(target=self._position_tick, daemon=False)
        self.position_thread.start()

    def _stop_position_thread(self):
        """安全停止进度线程"""
        self.position_running = False
        if self.position_thread and self.position_thread.is_alive():
            self.position_thread.join(timeout=2.0)

    def set_media_stop(self):
        """停止播放，清理进程和字幕"""
        self._stop_position_thread()   # 停止进度模拟
        # 终止播放器进程（先尝试正常关闭，再强制）
        if self.process:
            try:
                self.process.terminate()        # 发送 SIGTERM
                self.process.wait(timeout=3)    # 等待最多3秒
            except subprocess.TimeoutExpired:
                self.process.kill()             # 超时则强制
                self.process.wait()
            except Exception as e:
                logger.warning(f"Error terminating process: {e}")
            self.process = None
            self.pid = None

        # 清理字幕文件
        try:
            if os.path.exists(subtitle):
                os.remove(subtitle)
        except Exception as e:
            logger.warning(f"Failed to remove subtitle: {e}")

        self.set_state_transport('STOPPED')
        cherrypy.engine.publish('renderer_av_stop')

    def start_player(self, url):
        """在独立线程中启动 MPV 播放器（全屏，显示窗口）"""
        mpv_path = get_mpv_path()
        if not mpv_path:
            # 打开配置文件夹提示用户设置
            subprocess.Popen(['notepad.exe', Setting.setting_path], creationflags=subprocess.CREATE_NO_WINDOW)
            cherrypy.engine.publish('app_notify', "Error", "MPV not found. Please set 'MPVplayer_Path' in config and restart Macast.")
            logger.error("MPV path not configured.")
            return

        # 构建命令行参数（MPV 标准语法）
        cmd = [
            mpv_path,
            url,
            "--fullscreen",                     # 👈 启动即全屏
            f"--sub-file={subtitle}" if os.path.exists(subtitle) else "",
            "--cache=yes",
            "--keep-open=yes",                  # 播放结束后保持窗口
        ]
        cmd = [arg for arg in cmd if arg]       # 过滤空字符串

        try:
            # 🔽 移除 CREATE_NO_WINDOW，窗口正常显示
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            self.pid = self.process.pid
            logger.info(f"MPV started with PID {self.pid}, URL: {url}, fullscreen")

            # 等待进程结束（阻塞当前线程）
            self.process.wait()
            logger.info("MPV process exited normally")

        except Exception as e:
            logger.exception("Failed to start MPV")
            cherrypy.engine.publish('app_notify', "Error", str(e))
        finally:
            # 进程退出后自动停止状态（可能用户手动关闭）
            if self.process is None or self.process.poll() is not None:
                self.set_media_stop()   # 清理资源

    def set_media_url(self, url, start=0):
        """DLNA 推送新媒体"""
        # 先停止当前播放
        self.set_media_stop()
        self.start_position = 0   # 重置进度

        # 启动播放线程（非守护，确保能完整运行）
        t = threading.Thread(target=self.start_player, args=(url,), daemon=False)
        t.start()

        # 启动进度模拟
        self._start_position_thread()
        self.set_state_transport("PLAYING")
        cherrypy.engine.publish('renderer_av_uri', url)

    def stop(self):
        """Macast 调用停止"""
        super(MPVplayerRenderer, self).stop()
        self.set_media_stop()
        logger.info("MPVPlayer stop")

    def start(self):
        super(MPVplayerRenderer, self).start()
        logger.info("MPVPlayer start")

if __name__ == '__main__':
    gui(MPVplayerRenderer())