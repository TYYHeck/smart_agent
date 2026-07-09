# -*- coding: utf-8 -*-
"""
路由模块 —— 将 web_server.py 中的 36 个 API 端点按功能拆分为独立路由文件。

架构:
  routers/
  ├── __init__.py   ← 导出所有 router（一键注册到 FastAPI app）
  ├── auth.py       ← /api/auth/*      认证
  ├── chat.py       ← /api/chat        SSE 流式对话
  ├── tasks.py      ← /api/tasks/*     任务管理 + 编排
  ├── agents.py     ← /api/agents/*    Agent CRUD
  ├── system.py     ← /health, /metrics, /api/system/*, /api/config/*, /api/models
  └── files.py      ← /api/files/*     文件管理

使用方式（在 web_server.py 或 main.py 中）:
    from src.ui.routers import all_routers
    for r in all_routers:
        app.include_router(r)

注意: 旧版 web_server.py 中的内联路由仍然可用（向后兼容）。
"""

from .auth import router as auth_router
from .chat import router as chat_router
from .tasks import router as tasks_router
from .agents import router as agents_router
from .files import router as files_router
from .system import system_router, config_router, model_router

all_routers = [
    auth_router,
    chat_router,
    tasks_router,
    agents_router,
    files_router,
    system_router,
    config_router,
    model_router,
]
