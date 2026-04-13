# -*- coding: utf-8 -*-
"""
全链路日志：根记录器 + 控制台 INFO + 按日文件 logs/。
"""

from __future__ import annotations

import logging
import sys
from datetime import datetime
from pathlib import Path

from config import CONFIG

_CONFIGURED = False


def setup_logging(name: str = "comp7095") -> logging.Logger:
    """
    初始化根日志（仅一次）：控制台 INFO、文件 DEBUG/INFO。

    Args:
        name: 兼容参数，返回同名 Logger（子模块日志会向上冒泡到 root）。

    Returns:
        命名 Logger。
    """
    global _CONFIGURED
    root = logging.getLogger()
    if not _CONFIGURED:
        root.setLevel(logging.INFO)
        fmt = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(fmt)
        root.addHandler(ch)

        log_dir = Path.cwd() / CONFIG.local_logs_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        day = datetime.now().strftime("%Y%m%d")
        fh_path = log_dir / f"pipeline_{day}.log"
        fh = logging.FileHandler(fh_path, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        root.addHandler(fh)
        _CONFIGURED = True
        root.info("日志初始化完成，文件：%s", fh_path.resolve())

    return logging.getLogger(name)


def get_logger(name: str | None = None) -> logging.Logger:
    """返回子模块 Logger（冒泡至 root，无需重复挂载 handler）。"""
    if not _CONFIGURED:
        setup_logging()
    return logging.getLogger(name or "comp7095")
