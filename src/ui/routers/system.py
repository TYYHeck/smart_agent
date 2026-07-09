# -*- coding: utf-8 -*-
"""配置与系统路由 —— 健康检查/指标/配置管理"""

from __future__ import annotations
import os
from datetime import datetime
from fastapi import APIRouter, Depends, JSONResponse, Response
from pydantic import BaseModel, Field

from src.auth.dependencies import get_current_user

config_router = APIRouter(prefix="/api/config", tags=["配置"])
model_router = APIRouter(tags=["模型"])


class SwitchModelRequest(BaseModel):
    model: str = Field(..., description="模型 ID")
    provider: str | None = Field(None, description="提供商")
    base_url: str | None = Field(None, description="自定义 API 地址")


class ToggleModeRequest(BaseModel):
    mode: str = Field(..., description="模式: planning / rag / reflection")


class UpdateConfigRequest(BaseModel):
    section: str = Field(..., description="配置节名称")
    data: dict = Field(..., description="更新的键值对")


@config_router.get("")
async def api_config(current_user = Depends(get_current_user)):
    from ..web_server import get_agent
    agent = get_agent()
    return {
        "model": agent.llm.config.model if agent.llm else "N/A",
        "provider": agent.llm.config.provider if agent.llm else "N/A",
        "tools": len(agent.tools),
        "planning": agent.enable_planning,
        "rag": agent.enable_rag,
        "reflection": agent.enable_reflection,
        "langchain": agent._agent_graph is not None,
        "models": agent.available_models(),
    }


@config_router.get("/full")
async def api_config_full(current_user = Depends(get_current_user)):
    import yaml
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "config.yaml",
    )
    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    else:
        cfg = {}
    return {"ok": True, "config": cfg}


@config_router.post("/update")
async def api_config_update(req: UpdateConfigRequest, current_user = Depends(get_current_user)):
    import yaml
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "config.yaml",
    )
    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    else:
        cfg = {}

    if req.section not in cfg:
        cfg[req.section] = {}
    if isinstance(cfg[req.section], dict):
        cfg[req.section].update(req.data)
    else:
        cfg[req.section] = req.data

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

    return {"ok": True, "section": req.section}


@model_router.get("/api/models")
async def api_models(current_user = Depends(get_current_user)):
    from ..web_server import get_agent
    agent = get_agent()
    return {
        "current": agent.llm.config.model if agent.llm else "N/A",
        "models": agent.available_models(),
    }


@model_router.post("/api/switch_model")
async def api_switch_model(req: SwitchModelRequest, current_user = Depends(get_current_user)):
    from ..web_server import get_agent
    agent = get_agent()
    agent.switch_model(model=req.model, provider=req.provider, base_url=req.base_url)
    return {
        "ok": True,
        "model": agent.llm.config.model if agent.llm else "N/A",
        "provider": agent.llm.config.provider if agent.llm else "N/A",
    }


@model_router.post("/api/toggle_mode")
async def api_toggle_mode(req: ToggleModeRequest, current_user = Depends(get_current_user)):
    from ..web_server import get_agent
    agent = get_agent()
    if req.mode == "planning":
        agent.enable_planning = not agent.enable_planning
        return {"ok": True, "mode": "planning", "enabled": agent.enable_planning}
    elif req.mode == "rag":
        agent.enable_rag = not agent.enable_rag
        return {"ok": True, "mode": "rag", "enabled": agent.enable_rag}
    elif req.mode == "reflection":
        agent.enable_reflection = not agent.enable_reflection
        return {"ok": True, "mode": "reflection", "enabled": agent.enable_reflection}
    else:
        return JSONResponse(
            {"ok": False, "error": f"未知模式: {req.mode}"},
            status_code=400,
        )


# ── 系统路由 ──

system_router = APIRouter(tags=["系统"])


@system_router.get("/health")
async def health_check():
    from ..web_server import _agent

    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.1.0",
        "checks": {
            "server": "ok",
            "database": "unknown",
            "llm": "unknown",
        },
    }

    try:
        from src.infrastructure.database import is_db_available, _engine
        if is_db_available() and _engine:
            import sqlalchemy
            async with _engine.connect() as conn:
                await conn.execute(sqlalchemy.text("SELECT 1"))
            health["checks"]["database"] = "ok"
        else:
            health["checks"]["database"] = "disabled"
    except Exception:
        health["checks"]["database"] = "error"

    try:
        if _agent and _agent.llm:
            health["checks"]["llm"] = "ok"
        else:
            health["checks"]["llm"] = "not_initialized"
    except Exception:
        health["checks"]["llm"] = "error"

    if "error" in health["checks"].values():
        health["status"] = "degraded"

    return health


@system_router.get("/metrics")
async def api_metrics():
    try:
        from src.middleware.metrics import get_metrics_text
        return Response(content=get_metrics_text(), media_type="text/plain; version=0.0.4")
    except ImportError:
        return Response(content="# metrics disabled\n", media_type="text/plain")


@system_router.get("/api/system/info")
async def api_system_info(current_user = Depends(get_current_user)):
    import platform
    import psutil
    from ..web_server import _agent

    process = psutil.Process()
    mem = process.memory_info()

    db_status = "disabled"
    try:
        from src.infrastructure.database import is_db_available
        db_status = "connected" if is_db_available() else "disabled"
    except Exception:
        pass

    return {
        "ok": True,
        "system": {
            "python_version": platform.python_version(),
            "platform": platform.platform(),
            "cpu_count": psutil.cpu_count(),
            "memory_used_mb": round(mem.rss / 1024 / 1024, 2),
            "uptime_seconds": round(
                (datetime.now() - datetime.fromtimestamp(process.create_time())).total_seconds()
            ),
        },
        "database": db_status,
        "agent": {
            "name": _agent.name if _agent else "N/A",
            "model": _agent.llm.config.model if _agent and _agent.llm else "N/A",
            "provider": _agent.llm.config.provider if _agent and _agent.llm else "N/A",
        },
    }


@system_router.get("/api/commands")
async def api_commands():
    return {
        "ok": True,
        "commands": [
            {"cmd": "/help", "desc": "显示帮助信息", "args": ""},
            {"cmd": "/exit", "desc": "退出程序", "args": ""},
            {"cmd": "/task", "desc": "发布任务", "args": "<描述>"},
            {"cmd": "/agent", "desc": "Agent 管理", "args": "<子命令>"},
            {"cmd": "/model", "desc": "切换模型", "args": "<模型ID>"},
            {"cmd": "/clear", "desc": "清空对话", "args": ""},
            {"cmd": "/mode", "desc": "切换模式", "args": "<planning|rag|reflection>"},
            {"cmd": "/tools", "desc": "列出可用工具", "args": ""},
            {"cmd": "/status", "desc": "查看当前状态", "args": ""},
        ],
    }


@system_router.get("/api/dashboard/stats")
async def api_dashboard_stats(current_user = Depends(get_current_user)):
    from ..web_server import _agent, _db_initialized
    import psutil

    tm = get_task_manager()
    queue = tm.queue_status()
    agents = tm.list_agents()

    process = psutil.Process()
    mem = process.memory_info()

    return {
        "ok": True,
        "agent_name": _agent.name if _agent else "N/A",
        "model": _agent.llm.config.model if _agent and _agent.llm else "N/A",
        "tools": len(_agent.tools) if _agent else 0,
        "pending_tasks": queue["pending"],
        "running_tasks": queue["running"],
        "registered_agents": len(agents),
        "memory_mb": round(mem.rss / 1024 / 1024, 2),
        "storage": "MySQL" if _db_initialized else "内存",
    }
