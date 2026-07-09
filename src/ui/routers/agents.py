# -*- coding: utf-8 -*-
"""Agent 管理路由 —— CRUD"""

from __future__ import annotations
from fastapi import APIRouter, Depends, JSONResponse
from pydantic import BaseModel, Field

from src.auth.dependencies import get_current_user
from src.core.task_manager import get_task_manager, AgentProxy

router = APIRouter(prefix="/api/agents", tags=["Agent 管理"])


class CreateAgentRequest(BaseModel):
    name: str = Field(..., min_length=1, description="Agent 名称")
    model: str = Field("deepseek-chat", description="LLM 模型 ID")
    provider: str = Field("deepseek", description="LLM 提供商")
    skills: list[str] = Field(default_factory=list, description="技能标签")
    description: str = Field("", description="Agent 描述")


class UpdateAgentRequest(BaseModel):
    skills: list[str] | None = Field(None, description="更新技能标签")
    description: str | None = Field(None, description="更新描述")


async def _persist_agent_to_db(name: str, model: str, provider: str, skills: list[str], description: str):
    """将 Agent 配置写入 MySQL agent_configs 表"""
    import logging
    _log = logging.getLogger("smart_agent.web")

    if not name or not name.strip():
        return

    from ..web_server import _db_initialized
    if not _db_initialized:
        return

    from src.infrastructure.database import _session_factory
    if _session_factory is None:
        return

    from src.infrastructure.models import AgentConfigModel
    try:
        async with _session_factory() as session:
            existing = await session.get(AgentConfigModel, name)
            if existing:
                existing.model = model
                existing.provider = provider
                existing.skills = skills
                existing.description = description
            else:
                cfg = AgentConfigModel(
                    name=name, model=model, provider=provider,
                    skills=skills, description=description,
                )
                session.add(cfg)
            await session.commit()
            _log.info(f"Agent '{name}' 已持久化到数据库")
    except Exception as e:
        _log.error(f"Agent '{name}' 持久化失败: {e}", exc_info=True)


async def _do_delete_agent(name: str, tm):
    """删除 Agent（内存+数据库）"""
    tm.unregister_agent(name)
    try:
        from src.infrastructure.database import _session_factory
        from src.infrastructure.models import AgentConfigModel
        if _session_factory is not None:
            async with _session_factory() as session:
                cfg = await session.get(AgentConfigModel, name)
                if cfg:
                    await session.delete(cfg)
                    await session.commit()
    except Exception:
        pass


@router.get("/list")
async def api_list_agents(current_user = Depends(get_current_user)):
    tm = get_task_manager()
    agents = tm.list_agents()
    return {"ok": True, "agents": agents}


@router.post("/register")
async def api_register_agent(current_user = Depends(get_current_user)):
    from ..web_server import get_agent
    agent = get_agent()
    tm = get_task_manager()
    proxy = AgentProxy(name=agent.name, agent=agent)
    tm.register_agent(proxy)
    tm.start_dispatcher()
    return {"ok": True, "agent_name": agent.name}


@router.post("/unregister")
async def api_unregister_agent(current_user = Depends(get_current_user)):
    from ..web_server import get_agent
    agent = get_agent()
    tm = get_task_manager()
    tm.unregister_agent(agent.name)
    return {"ok": True}


@router.post("/create")
async def api_create_agent(req: CreateAgentRequest, current_user = Depends(get_current_user)):
    from src.core.llm import LLMConfig
    from src.tools.builtin_tools import register_all
    from src.core.agent import Agent

    if not req.name or not req.name.strip():
        return JSONResponse({"ok": False, "error": "Agent 名称不能为空"}, status_code=400)

    config = LLMConfig(provider=req.provider, model=req.model)
    new_agent = Agent()
    new_agent.name = req.name
    skill_desc = f"专注于{'、'.join(req.skills)}" if req.skills else "通用"
    new_agent.system_prompt = (
        f"你是 {req.name}，一个{skill_desc}的 AI 助手。"
        f"请用你的专业知识高效完成用户的任务。"
    )
    new_agent.init(config)
    register_all(new_agent.tools)
    if hasattr(new_agent, '_rebuild_graph'):
        new_agent._rebuild_graph()

    tm = get_task_manager()
    proxy = AgentProxy(
        name=req.name, agent=new_agent,
        skills=req.skills or [],
        description=req.description or f"{skill_desc}型 Agent",
    )
    tm.register_agent(proxy)
    tm.start_dispatcher()

    await _persist_agent_to_db(
        name=req.name, model=req.model, provider=req.provider,
        skills=req.skills or [], description=req.description,
    )

    return {"ok": True, "agent": proxy.to_dict()}


@router.post("/{name}/update")
async def api_update_agent(
    name: str,
    req: UpdateAgentRequest,
    current_user = Depends(get_current_user),
):
    tm = get_task_manager()
    if name not in tm._agents:
        return JSONResponse({"ok": False, "error": "Agent 未找到"}, status_code=404)

    proxy = tm._agents[name]
    if req.skills is not None:
        proxy.skills = req.skills
    if req.description is not None:
        proxy.description = req.description

    # 持久化
    cfg = proxy.agent
    await _persist_agent_to_db(
        name=name,
        model=cfg.llm.config.model if cfg.llm else "deepseek-chat",
        provider=cfg.llm.config.provider if cfg.llm else "deepseek",
        skills=proxy.skills,
        description=proxy.description,
    )

    return {"ok": True, "agent": proxy.to_dict()}


@router.delete("/{name}")
async def api_delete_agent(name: str, current_user = Depends(get_current_user)):
    tm = get_task_manager()
    if name not in tm._agents:
        return JSONResponse({"ok": False, "error": "Agent 未找到"}, status_code=404)

    await _do_delete_agent(name, tm)
    return {"ok": True}


@router.post("/cleanup")
async def api_cleanup_agents(current_user = Depends(get_current_user)):
    """清理空名字等无效 Agent"""
    tm = get_task_manager()
    removed = []
    for agent_name in list(tm._agents.keys()):
        if not agent_name or not agent_name.strip():
            await _do_delete_agent(agent_name, tm)
            removed.append(repr(agent_name))
    try:
        from src.infrastructure.database import _session_factory
        from src.infrastructure.models import AgentConfigModel
        from sqlalchemy import delete
        if _session_factory is not None:
            async with _session_factory() as session:
                result = await session.execute(
                    delete(AgentConfigModel).where(AgentConfigModel.name == "")
                )
                await session.commit()
                if result.rowcount:
                    removed.append("DB:空name记录")
    except Exception:
        pass
    return {"ok": True, "removed": removed}
