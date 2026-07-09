# -*- coding: utf-8 -*-
"""Agent 管理路由 —— CRUD"""

from __future__ import annotations
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.auth.dependencies import get_current_user
from src.core.task_manager import get_task_manager, AgentProxy

router = APIRouter(prefix="/api/agents", tags=["Agent 管理"])


# ── system_prompt 长度上限（防止超大文本塞入数据库/LLM）──
SYSTEM_PROMPT_MAX_LEN = 5000
MAX_ITERATIONS_MIN = 1
MAX_ITERATIONS_MAX = 50


class CreateAgentRequest(BaseModel):
    name: str = Field(..., min_length=1, description="Agent 名称")
    model: str = Field("deepseek-chat", description="LLM 模型 ID")
    provider: str = Field("deepseek", description="LLM 提供商")
    skills: list[str] = Field(default_factory=list, description="技能标签")
    description: str = Field("", description="Agent 描述")
    system_prompt: str = Field("", description="自定义 System Prompt（空则自动生成）")
    max_iterations: int = Field(15, ge=MAX_ITERATIONS_MIN, le=MAX_ITERATIONS_MAX,
                                description="最大迭代次数 (1-50)")
    enable_planning: bool = Field(False, description="启用计划模式")
    enable_rag: bool = Field(True, description="启用 RAG 知识库")
    enable_reflection: bool = Field(False, description="启用反思模式")


class UpdateAgentRequest(BaseModel):
    skills: list[str] | None = Field(None, description="更新技能标签")
    description: str | None = Field(None, description="更新描述")
    system_prompt: str | None = Field(None, description="更新 System Prompt")
    max_iterations: int | None = Field(None, ge=MAX_ITERATIONS_MIN, le=MAX_ITERATIONS_MAX,
                                       description="更新最大迭代次数")
    enable_planning: bool | None = Field(None, description="更新计划模式开关")
    enable_rag: bool | None = Field(None, description="更新 RAG 开关")
    enable_reflection: bool | None = Field(None, description="更新反思模式开关")


async def _persist_agent_to_db(
    name: str, model: str, provider: str, skills: list[str], description: str,
    system_prompt: str = "", max_iterations: int = 15,
    enable_planning: bool = False, enable_rag: bool = True,
    enable_reflection: bool = False,
):
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

    # ── system_prompt 长度限制 ──
    sp = (system_prompt or "").strip()
    if len(sp) > SYSTEM_PROMPT_MAX_LEN:
        sp = sp[:SYSTEM_PROMPT_MAX_LEN]
        _log.warning(f"Agent '{name}' 的 system_prompt 超过 {SYSTEM_PROMPT_MAX_LEN} 字符，已截断")

    from src.infrastructure.models import AgentConfigModel
    try:
        async with _session_factory() as session:
            existing = await session.get(AgentConfigModel, name)
            if existing:
                existing.model = model
                existing.provider = provider
                existing.skills = skills
                existing.description = description
                existing.system_prompt = sp
                existing.max_iterations = max_iterations
                existing.enable_planning = enable_planning
                existing.enable_rag = enable_rag
                existing.enable_reflection = enable_reflection
            else:
                cfg = AgentConfigModel(
                    name=name, model=model, provider=provider,
                    skills=skills, description=description,
                    system_prompt=sp, max_iterations=max_iterations,
                    enable_planning=enable_planning, enable_rag=enable_rag,
                    enable_reflection=enable_reflection,
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


@router.get("/{name}/config")
async def api_get_agent_config(name: str, current_user = Depends(get_current_user)):
    """获取 Agent 完整配置（含 system_prompt，供编辑表单使用）"""
    import logging as _logging
    _log = _logging.getLogger("smart_agent.web")

    from src.infrastructure.database import _session_factory
    from src.infrastructure.models import AgentConfigModel

    if _session_factory is None:
        return JSONResponse({"ok": False, "error": "数据库未连接"}, status_code=503)

    try:
        async with _session_factory() as session:
            cfg = await session.get(AgentConfigModel, name)
            if cfg is None:
                return JSONResponse({"ok": False, "error": "Agent 配置未找到"}, status_code=404)
            return {"ok": True, "config": cfg.to_dict()}
    except Exception as e:
        _log.error(f"获取 Agent '{name}' 配置失败: {e}")
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)


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


def _build_default_system_prompt(name: str, skills: list[str]) -> str:
    """生成默认 system_prompt（用户未自定义时使用）"""
    skill_desc = f"专注于{'、'.join(skills)}" if skills else "通用"
    return (
        f"你是 {name}，一个{skill_desc}的 AI 助手。"
        f"请用你的专业知识高效完成用户的任务。"
    )


@router.post("/create")
async def api_create_agent(req: CreateAgentRequest, current_user = Depends(get_current_user)):
    from src.core.llm import LLMConfig
    from src.tools.builtin_tools import register_all
    from src.core.agent import Agent

    if not req.name or not req.name.strip():
        return JSONResponse({"ok": False, "error": "Agent 名称不能为空"}, status_code=400)

    # ── system_prompt: 优先用户自定义，否则自动生成 ──
    sp = (req.system_prompt or "").strip()
    if sp:
        if len(sp) > SYSTEM_PROMPT_MAX_LEN:
            sp = sp[:SYSTEM_PROMPT_MAX_LEN]
    else:
        sp = _build_default_system_prompt(req.name, req.skills or [])

    config = LLMConfig(provider=req.provider, model=req.model)
    new_agent = Agent()
    new_agent.name = req.name
    new_agent.system_prompt = sp
    new_agent.max_iterations = req.max_iterations
    new_agent.enable_planning = req.enable_planning
    new_agent.enable_rag = req.enable_rag
    new_agent.enable_reflection = req.enable_reflection
    new_agent.init(config)
    register_all(new_agent.tools)
    if hasattr(new_agent, '_rebuild_graph'):
        new_agent._rebuild_graph()

    tm = get_task_manager()
    skill_desc = f"专注于{'、'.join(req.skills)}" if req.skills else "通用"
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
        system_prompt=sp, max_iterations=req.max_iterations,
        enable_planning=req.enable_planning, enable_rag=req.enable_rag,
        enable_reflection=req.enable_reflection,
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

    # ── 更新 Agent runtime 属性 ──
    agent_obj = proxy.agent
    if req.system_prompt is not None:
        sp = req.system_prompt.strip()
        if sp and len(sp) > SYSTEM_PROMPT_MAX_LEN:
            sp = sp[:SYSTEM_PROMPT_MAX_LEN]
        if not sp:
            sp = _build_default_system_prompt(name, proxy.skills or [])
        agent_obj.system_prompt = sp
        if hasattr(agent_obj, '_rebuild_graph'):
            agent_obj._rebuild_graph()  # prompt 变更需重建 graph
    if req.max_iterations is not None:
        agent_obj.max_iterations = req.max_iterations
    if req.enable_planning is not None:
        agent_obj.enable_planning = req.enable_planning
    if req.enable_rag is not None:
        agent_obj.enable_rag = req.enable_rag
    if req.enable_reflection is not None:
        agent_obj.enable_reflection = req.enable_reflection

    # 持久化
    await _persist_agent_to_db(
        name=name,
        model=agent_obj.llm.config.model if agent_obj.llm else "deepseek-chat",
        provider=agent_obj.llm.config.provider if agent_obj.llm else "deepseek",
        skills=proxy.skills,
        description=proxy.description,
        system_prompt=agent_obj.system_prompt,
        max_iterations=agent_obj.max_iterations,
        enable_planning=agent_obj.enable_planning,
        enable_rag=agent_obj.enable_rag,
        enable_reflection=agent_obj.enable_reflection,
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
