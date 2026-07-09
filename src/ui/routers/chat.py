# -*- coding: utf-8 -*-
"""聊天路由 —— SSE 流式对话"""

from __future__ import annotations
import json
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from src.auth.dependencies import get_current_user

router = APIRouter(tags=["对话"])


class ChatRequest(BaseModel):
    message: str = Field(..., min_length=1, description="用户消息内容")


@router.post("/api/chat")
async def api_chat(req: ChatRequest, current_user = Depends(get_current_user)):
    from ..web_server import get_agent

    agent = get_agent()

    async def generate():
        async for event in agent.stream_events(req.message):
            yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )
