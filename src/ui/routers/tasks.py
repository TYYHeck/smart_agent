# -*- coding: utf-8 -*-
"""任务管理路由 —— 发布/编排/查询任务"""

from __future__ import annotations
import json
import asyncio
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from src.auth.dependencies import get_current_user
from src.core.task_manager import get_task_manager, AgentProxy
from src.core.orchestrator import ExecutionMode, patch_task_manager

router = APIRouter(prefix="/api/tasks", tags=["任务管理"])

# 编排器线程池
_orch_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="orchestrator")


class PublishTaskRequest(BaseModel):
    description: str = Field(..., min_length=1, description="任务描述")
    title: str = Field("", description="任务标题（可选）")
    priority: int = Field(0, ge=0, le=10, description="优先级 0-10")
    tags: list[str] = Field(default_factory=list, description="标签列表")
    target_agent: str = Field("", description="指定 Agent 名称")


class OrchestrateTaskRequest(BaseModel):
    description: str = Field(..., min_length=1, description="任务描述")
    title: str = Field("", description="任务标题")
    mode: str = Field("auto", description="执行模式: single/parallel/pipeline/collaborative/auto")
    agent_names: list[str] | None = Field(None, description="指定 Agent 列表")


class UpdateTaskRequest(BaseModel):
    description: str | None = Field(None, description="新任务描述")
    title: str | None = Field(None, description="新标题")
    priority: int | None = Field(None, ge=0, le=10, description="新优先级")


@router.post("/publish")
async def api_publish_task(req: PublishTaskRequest, current_user = Depends(get_current_user)):
    tm = get_task_manager()
    task_id = tm.publish(
        description=req.description,
        title=req.title,
        priority=req.priority,
        tags=req.tags,
        target_agent=req.target_agent,
    )
    return {"ok": True, "task_id": task_id}


@router.post("/orchestrate")
async def api_orchestrate_task(req: OrchestrateTaskRequest, current_user = Depends(get_current_user)):
    from ..web_server import get_agent

    agent = get_agent()
    tm = get_task_manager()

    # 确保主 Agent 已注册
    if agent.name not in list(tm._agents.keys()):
        tm.register_agent(AgentProxy(name=agent.name, agent=agent))

    result = tm.execute_orchestrated(
        description=req.description,
        title=req.title,
        mode=req.mode,
        agent_names=req.agent_names,
    )
    return {"ok": True, "result": result.to_dict()}


@router.post("/orchestrate/stream")
async def api_orchestrate_task_stream(
    request: Request,
    req: OrchestrateTaskRequest,
    current_user = Depends(get_current_user),
):
    from ..web_server import get_agent

    agent = get_agent()
    tm = get_task_manager()

    if agent.name not in list(tm._agents.keys()):
        tm.register_agent(AgentProxy(name=agent.name, agent=agent))

    async def generate():
        progress_queue: asyncio.Queue = asyncio.Queue()

        def _on_progress(stage: str, info: dict):
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    asyncio.run_coroutine_threadsafe(
                        progress_queue.put({"stage": stage, **info}),
                        loop,
                    )
            except Exception:
                pass

        def _run():
            try:
                result = tm.execute_orchestrated(
                    description=req.description,
                    title=req.title,
                    mode=req.mode,
                    agent_names=req.agent_names,
                    on_progress=_on_progress,
                )
                asyncio.run_coroutine_threadsafe(
                    progress_queue.put({"type": "done", "result": result.to_dict()}),
                    asyncio.get_event_loop(),
                )
            except Exception as e:
                asyncio.run_coroutine_threadsafe(
                    progress_queue.put({"type": "error", "content": str(e)}),
                    asyncio.get_event_loop(),
                )

        _orch_executor.submit(_run)

        while True:
            if await request.is_disconnected():
                break
            try:
                event = await asyncio.wait_for(progress_queue.get(), timeout=0.5)
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                if event.get("type") in ("done", "error"):
                    break
            except asyncio.TimeoutError:
                continue

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/detect-mode")
async def api_detect_mode(req: OrchestrateTaskRequest, current_user = Depends(get_current_user)):
    tm = get_task_manager()
    return {"ok": True, **tm.detect_best_mode(req.description)}


@router.get("/orchestrate/modes")
async def api_list_modes(current_user = Depends(get_current_user)):
    return {
        "ok": True,
        "modes": [
            {"id": "auto", "name": "自动", "desc": "系统分析任务自动选择模式"},
            {"id": "single", "name": "单 Agent", "desc": "一个 Agent 独立执行"},
            {"id": "parallel", "name": "并行", "desc": "多 Agent 同时执行，汇总结果"},
            {"id": "pipeline", "name": "流水线", "desc": "Agent 串行接力"},
            {"id": "collaborative", "name": "协作讨论", "desc": "团队讨论，互审达成共识"},
        ],
    }


@router.get("/list")
async def api_list_tasks(
    status: str = "",
    limit: int = 20,
    current_user = Depends(get_current_user),
):
    tm = get_task_manager()
    tasks = tm.list_tasks(status=status, limit=limit)
    return {"ok": True, "tasks": tasks, "queue": tm.queue_status()}


@router.get("/{task_id}")
async def api_get_task(task_id: str, current_user = Depends(get_current_user)):
    tm = get_task_manager()
    task = tm.get_task(task_id)
    if task is None:
        return JSONResponse({"ok": False, "error": "任务未找到"}, status_code=404)
    return {"ok": True, "task": task}


@router.post("/{task_id}/cancel")
async def api_cancel_task(task_id: str, current_user = Depends(get_current_user)):
    tm = get_task_manager()
    tm.cancel_task(task_id)
    return {"ok": True}


@router.get("/queue/status")
async def api_queue_status(current_user = Depends(get_current_user)):
    tm = get_task_manager()
    return {"ok": True, **tm.queue_status()}


@router.get("/{task_id}/watch")
async def api_watch_task(task_id: str, request: Request, current_user = Depends(get_current_user)):
    """SSE 实时监听任务状态变更"""
    tm = get_task_manager()
    task = tm.get_task(task_id)
    if task is None:
        return JSONResponse({"ok": False, "error": "任务未找到"}, status_code=404)

    async def generate():
        last_event_count = 0
        # 先推送当前状态
        current_task = tm.get_task(task_id) or {}
        yield f"data: {json.dumps({'type': 'status', 'task': current_task}, ensure_ascii=False)}\n\n"

        while True:
            if await request.is_disconnected():
                break
            await asyncio.sleep(1)
            current_task = tm.get_task(task_id)
            if current_task is None:
                yield f"data: {json.dumps({'type': 'gone', 'task_id': task_id}, ensure_ascii=False)}\n\n"
                break

            # 检测状态变化
            status = current_task.get("status", "")
            if status in ("completed", "failed", "cancelled"):
                yield f"data: {json.dumps({'type': 'done', 'task': current_task}, ensure_ascii=False)}\n\n"
                break

            # 检测事件日志变化
            event_log = current_task.get("event_log", [])
            new_count = len(event_log)
            if new_count > last_event_count:
                new_events = event_log[last_event_count:]
                for evt in new_events:
                    yield f"data: {json.dumps({'type': 'event', 'data': evt}, ensure_ascii=False)}\n\n"
                last_event_count = new_count

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.post("/{task_id}/update")
async def api_update_task(
    task_id: str,
    req: UpdateTaskRequest,
    current_user = Depends(get_current_user),
):
    tm = get_task_manager()
    task = tm.get_task(task_id)
    if task is None:
        return JSONResponse({"ok": False, "error": "任务未找到"}, status_code=404)
    if req.description is not None:
        task["description"] = req.description
    if req.title is not None:
        task["title"] = req.title
    if req.priority is not None:
        task["priority"] = req.priority
    return {"ok": True, "task": task}
