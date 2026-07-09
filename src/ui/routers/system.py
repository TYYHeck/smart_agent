# -*- coding: utf-8 -*-
"""配置与系统路由 —— 健康检查/指标/配置管理"""

from __future__ import annotations
import os
from datetime import datetime
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel, Field

from src.auth.dependencies import get_current_user
from src.core.task_manager import get_task_manager

config_router = APIRouter(prefix="/api/config", tags=["配置"])
model_router = APIRouter(tags=["模型"])


class SwitchModelRequest(BaseModel):
    model: str = Field(..., description="模型 ID")
    provider: str | None = Field(None, description="提供商")
    base_url: str | None = Field(None, description="自定义 API 地址")
    api_key: str | None = Field(None, description="API Key")


class ToggleModeRequest(BaseModel):
    mode: str = Field(..., description="模式: planning / rag / reflection")


class UpdateConfigRequest(BaseModel):
    section: str = Field(..., description="配置节名称")
    data: dict = Field(..., description="更新的键值对")


class UpdateLLMConfigRequest(BaseModel):
    provider: str = Field(..., description="提供商: openai/deepseek/zhipu/qwen/ollama/custom")
    model: str = Field("", description="模型 ID")
    api_key: str = Field("", description="API Key（支持 ${VAR} 引用环境变量）")
    base_url: str = Field("", description="自定义 API 地址（custom 提供商时必填）")


class QueryModelsRequest(BaseModel):
    provider: str = Field(..., description="提供商")
    api_key: str = Field("", description="API Key")
    base_url: str = Field("", description="自定义 API 地址")


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
    agent.switch_model(model=req.model, provider=req.provider,
                       base_url=req.base_url, api_key=req.api_key)
    return {
        "ok": True,
        "model": agent.llm.config.model if agent.llm else "N/A",
        "provider": agent.llm.config.provider if agent.llm else "N/A",
    }


# ── LLM 提供商配置 API ──

@config_router.post("/llm")
async def api_config_llm_update(req: UpdateLLMConfigRequest, current_user = Depends(get_current_user)):
    """更新 LLM 提供商配置：写入 config.yaml 并应用到运行时"""
    import yaml
    from ..web_server import get_agent

    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        "config.yaml",
    )
    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
    else:
        cfg = {}

    cfg.setdefault("llm", {})
    cfg["llm"]["provider"] = req.provider
    cfg["llm"]["api_key"] = req.api_key
    if req.model:
        cfg["llm"]["model"] = req.model
    if req.base_url:
        cfg["llm"]["base_url"] = req.base_url
    elif "base_url" in cfg.get("llm", {}):
        cfg["llm"].pop("base_url", None)

    with open(config_path, "w", encoding="utf-8") as f:
        yaml.dump(cfg, f, allow_unicode=True, default_flow_style=False)

    # 运行时切换
    try:
        agent = get_agent()
        actual_model = req.model or (agent.llm.config.model if agent.llm else "gpt-3.5-turbo")
        agent.switch_model(
            model=actual_model,
            provider=req.provider,
            api_key=req.api_key,
            base_url=req.base_url or None,
        )
    except Exception as e:
        return {"ok": True, "saved": True, "switched": False, "warning": str(e)}

    return {
        "ok": True,
        "saved": True,
        "switched": True,
        "provider": req.provider,
        "model": agent.llm.config.model if agent.llm else "N/A",
    }


@model_router.post("/api/models/query")
async def api_query_models(req: QueryModelsRequest, current_user = Depends(get_current_user)):
    """查询指定 provider 的可用模型列表"""
    from ..web_server import _agent
    models = _agent.query_models(
        provider=req.provider,
        api_key=req.api_key,
        base_url=req.base_url,
    ) if _agent else []
    return {"ok": True, "provider": req.provider, "models": models}


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
            {"cmd": "/debug", "desc": "切换调试模式", "args": ""},
            {"cmd": "/recall", "desc": "搜索长期记忆", "args": "<查询>"},
            {"cmd": "/orchestrate", "desc": "多 Agent 编排", "args": "<子命令>"},
            {"cmd": "/kb", "desc": "知识库管理", "args": "<子命令>"},
            {"cmd": "/files", "desc": "输出文件管理", "args": "<子命令>"},
            {"cmd": "/sysinfo", "desc": "系统信息", "args": ""},
            {"cmd": "/config", "desc": "配置管理", "args": "<子命令>"},
        ],
    }


# ── 工具详情 API (CLI /tools 对应) ──

@system_router.get("/api/tools")
async def api_tools(current_user = Depends(get_current_user)):
    """列出所有已注册工具（含参数、描述、危险标记）"""
    from ..web_server import get_agent
    agent = get_agent()
    tools = []
    for tool in agent.tools.list_all():
        tools.append({
            "name": tool.name,
            "description": tool.description,
            "parameters": list(tool.parameters.keys()),
            "dangerous": tool.dangerous,
        })
    return {"ok": True, "tools": tools, "count": len(tools)}


# ── 调试模式 API (CLI /debug 对应) ──

@system_router.post("/api/toggle_debug")
async def api_toggle_debug(current_user = Depends(get_current_user)):
    """切换调试模式（显示 Agent 内部错误详情）"""
    from ..web_server import get_agent
    agent = get_agent()
    if not hasattr(agent, '_debug'):
        agent._debug = False
    agent._debug = not agent._debug
    return {"ok": True, "debug": agent._debug}


# ── 记忆管理 API (CLI /clear /recall 对应) ──

class RecallRequest(BaseModel):
    query: str = Field(..., min_length=1, description="搜索查询")
    top_k: int = Field(5, ge=1, le=20, description="返回条数")


@system_router.post("/api/memory/recall")
async def api_memory_recall(req: RecallRequest, current_user = Depends(get_current_user)):
    """搜索长期记忆"""
    from ..web_server import get_agent
    agent = get_agent()
    try:
        results = agent.memory.recall(req.query, top_k=req.top_k)
        return {"ok": True, "results": results}
    except Exception as e:
        return JSONResponse(
            {"ok": False, "error": str(e)},
            status_code=500,
        )


@system_router.post("/api/memory/clear")
async def api_memory_clear(current_user = Depends(get_current_user)):
    """清空短期对话记忆"""
    from ..web_server import get_agent
    agent = get_agent()
    agent.memory.short.clear()
    agent.memory.set_system(agent._build_system_prompt())
    return {"ok": True, "message": "短期记忆已清空"}


# /api/dashboard/stats 端点已移至 web_server.py (返回格式与前端匹配)
# 此处不再注册，避免路径冲突导致仪表盘无数据
