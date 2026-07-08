# -*- coding: utf-8 -*-
"""
结构化日志配置 —— JSON 格式输出 + MySQL 持久化 + 文件轮转

使用 Python 标准 logging + 自定义 MySQL Handler
"""

from __future__ import annotations
import logging
import logging.handlers
import json
import os
import sys
from datetime import datetime, timezone
from typing import Optional


class JsonFormatter(logging.Formatter):
    """JSON 格式日志格式化器（生产环境用）"""

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # 附加上下文字段
        if hasattr(record, "trace_id"):
            log_entry["trace_id"] = record.trace_id

        if record.exc_info and record.exc_info[0]:
            log_entry["exception"] = self.formatException(record.exc_info)

        # 合并 extra 字段
        for key in dir(record):
            if key.startswith("_extra_"):
                log_entry[key[7:]] = getattr(record, key)

        return json.dumps(log_entry, ensure_ascii=False)


class MySQLogHandler(logging.Handler):
    """
    将 ERROR 及以上日志写入 MySQL 的 system_logs 表

    注意：异步写入，使用队列避免阻塞主线程
    """

    def __init__(self):
        super().__init__(level=logging.ERROR)
        self._enabled = True

    def emit(self, record: logging.LogRecord):
        if not self._enabled:
            return

        try:
            import asyncio
            from ..infrastructure.database import _session_factory

            if _session_factory is None:
                return

            extra_data = {}
            if record.exc_info and record.exc_info[0]:
                extra_data["exception"] = self.format(record)

            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(
                    self._write_log(
                        record.levelname,
                        record.name,
                        record.getMessage(),
                        extra_data,
                        getattr(record, "trace_id", None),
                    )
                )
        except Exception:
            pass

    async def _write_log(
        self,
        level: str,
        logger_name: str,
        message: str,
        extra: dict,
        trace_id: Optional[str],
    ):
        try:
            from ..infrastructure.models import SystemLogModel

            async with _session_factory() as session:
                log_entry = SystemLogModel(
                    level=level,
                    logger_name=logger_name,
                    message=message[:2000],
                    extra_data=extra or None,
                    trace_id=trace_id,
                )
                session.add(log_entry)
                await session.commit()
        except Exception:
            pass


class TraceIdFilter(logging.Filter):
    """为每条日志添加 trace_id"""

    def __init__(self):
        super().__init__()
        import uuid
        self._trace_id = str(uuid.uuid4())[:12]

    def filter(self, record: logging.LogRecord) -> bool:
        record.trace_id = self._trace_id
        return True


def setup_logging(
    level: str = "INFO",
    log_dir: str = "./logs",
    json_format: bool = False,
    enable_mysql: bool = False,
):
    """
    配置全局日志系统

    Args:
        level: 日志级别 (DEBUG/INFO/WARNING/ERROR)
        log_dir: 日志文件目录
        json_format: 是否使用 JSON 格式（生产环境建议 True）
        enable_mysql: 是否将 ERROR 日志写入 MySQL
    """
    # 创建日志目录
    os.makedirs(log_dir, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # 清除已有 handler
    root_logger.handlers.clear()

    trace_filter = TraceIdFilter()

    # ---- 控制台 handler ----
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG)
    if json_format:
        console.setFormatter(JsonFormatter())
    else:
        console.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%H:%M:%S",
        ))
    console.addFilter(trace_filter)
    root_logger.addHandler(console)

    # ---- 文件 handler（轮转） ----
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "app.log"),
        maxBytes=10 * 1024 * 1024,  # 10 MB
        backupCount=10,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG)
    if json_format:
        file_handler.setFormatter(JsonFormatter())
    else:
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s [%(module)s:%(lineno)d]: %(message)s"
        ))
    file_handler.addFilter(trace_filter)
    root_logger.addHandler(file_handler)

    # ---- 错误日志单独文件 ----
    error_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, "error.log"),
        maxBytes=10 * 1024 * 1024,
        backupCount=5,
        encoding="utf-8",
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(JsonFormatter() if json_format else logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s [%(module)s:%(lineno)d]: %(message)s"
    ))
    error_handler.addFilter(trace_filter)
    root_logger.addHandler(error_handler)

    # ---- MySQL handler (可选) ----
    if enable_mysql:
        try:
            mysql_handler = MySQLogHandler()
            root_logger.addHandler(mysql_handler)
        except Exception:
            pass

    # 降低第三方库日志噪音
    for lib in ["uvicorn", "httpx", "openai", "chromadb", "sqlalchemy"]:
        logging.getLogger(lib).setLevel(logging.WARNING)

    # 记录启动信息
    logger = logging.getLogger("smart_agent")
    logger.info(f"日志系统已初始化 (level={level}, json={json_format}, mysql={enable_mysql})")

    return root_logger
