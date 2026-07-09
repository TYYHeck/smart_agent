# -*- coding: utf-8 -*-
"""知识库管理路由 —— 文件上传/列表/检索/清空"""

from __future__ import annotations
import os
import shutil
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse

from src.auth.dependencies import get_current_user

router = APIRouter(prefix="/api/knowledge", tags=["知识库"])

# 上传文件存储目录
_UPLOAD_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
    "data", "uploads",
)
os.makedirs(_UPLOAD_DIR, exist_ok=True)

# 支持的文件类型
ALLOWED_EXTENSIONS = {
    ".txt", ".md", ".py", ".js", ".ts",
    ".json", ".yaml", ".yml", ".html", ".css", ".pdf",
}

MAX_FILE_SIZE = 20 * 1024 * 1024  # 20MB


def _get_kb():
    """获取全局知识库实例"""
    from src.ui.web_server import get_agent
    agent = get_agent()
    if agent.knowledge is None:
        raise HTTPException(status_code=503, detail="知识库未初始化，请检查 RAG 配置")
    return agent.knowledge


def _validate_ext(filename: str) -> str:
    """校验扩展名，返回小写扩展名"""
    ext = os.path.splitext(filename)[1].lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的文件类型: {ext}。支持: {', '.join(sorted(ALLOWED_EXTENSIONS))}",
        )
    return ext


# ============================================================
# 文件上传
# ============================================================

@router.post("/upload")
async def api_upload_knowledge(
    files: list[UploadFile] = File(...),
    current_user = Depends(get_current_user),
):
    """上传文档到知识库  (支持多文件同时上传)

    支持类型: txt / md / py / js / ts / json / yaml / html / css / pdf
    文件大小限制: 单文件 ≤ 20MB

    上传后自动: 加载文本 → 语义分块 → 向量嵌入 → 存入 ChromaDB
    """
    kb = _get_kb()
    os.makedirs(_UPLOAD_DIR, exist_ok=True)

    results = []
    for f in files:
        if not f.filename:
            continue

        ext = _validate_ext(f.filename)

        # 保存到本地
        safe_name = f.filename.replace("\\", "_").replace("/", "_")
        filepath = os.path.join(_UPLOAD_DIR, safe_name)
        content_bytes = await f.read()

        if len(content_bytes) > MAX_FILE_SIZE:
            results.append({"file": f.filename, "ok": False, "error": f"文件超过 20MB 限制"})
            continue

        with open(filepath, "wb") as out:
            out.write(content_bytes)

        # 加载文本内容
        try:
            from src.rag.knowledge_base import DocumentLoader
            content = DocumentLoader.load_file(filepath)
        except Exception as e:
            results.append({"file": f.filename, "ok": False, "error": f"文件加载失败: {e}"})
            continue

        # 添加到知识库
        try:
            kb.add_document(
                source_id=safe_name,
                content=content,
                metadata={"filename": f.filename, "ext": ext, "size": len(content_bytes)},
            )
            results.append({
                "file": f.filename,
                "ok": True,
                "size": len(content_bytes),
                "chars": len(content),
                "saved_path": filepath,
            })
        except Exception as e:
            results.append({"file": f.filename, "ok": False, "error": f"向量化失败: {e}"})

    ok_count = sum(1 for r in results if r.get("ok"))
    return {
        "ok": True,
        "uploaded": ok_count,
        "total": len(files),
        "results": results,
    }


# ============================================================
# 知识库文件列表
# ============================================================

@router.get("/files")
async def api_knowledge_files(current_user = Depends(get_current_user)):
    """列出知识库中已添加的所有文件源及切片数"""
    kb = _get_kb()
    stats = kb.stats()

    sources = []
    try:
        all_data = kb.collection.get()
        if all_data and all_data.get("metadatas"):
            # 按 source_id 聚合
            source_map: dict[str, dict] = {}
            for meta in all_data["metadatas"]:
                sid = meta.get("source_id", "unknown")
                if sid not in source_map:
                    source_map[sid] = {
                        "source_id": sid,
                        "filename": meta.get("filename", sid),
                        "ext": meta.get("ext", ""),
                        "size": meta.get("size", 0),
                        "chunks": 0,
                    }
                source_map[sid]["chunks"] += 1
            sources = sorted(source_map.values(), key=lambda s: s["chunks"], reverse=True)
    except Exception:
        pass

    return {
        "ok": True,
        "total_chunks": stats.get("chunks", 0),
        "total_sources": stats.get("sources", 0),
        "sources": sources,
    }


# ============================================================
# 知识库统计
# ============================================================

@router.get("/stats")
async def api_knowledge_stats(current_user = Depends(get_current_user)):
    """知识库统计信息 (总切片数 / 来源数)"""
    kb = _get_kb()
    return {"ok": True, **kb.stats()}


# ============================================================
# 检索测试
# ============================================================

@router.get("/search")
async def api_knowledge_search(
    q: str,
    top_k: int = 5,
    current_user = Depends(get_current_user),
):
    """检索知识库 (测试用)"""
    kb = _get_kb()
    results = kb.search(q, top_k=top_k)
    return {"ok": True, "query": q, "results": results, "count": len(results)}


# ============================================================
# 清空知识库
# ============================================================

@router.delete("/clear")
async def api_clear_knowledge(current_user = Depends(get_current_user)):
    """清空整个知识库 (所有切片 + 本地上传文件)"""
    kb = _get_kb()
    kb.clear()

    # 清空上传目录
    if os.path.isdir(_UPLOAD_DIR):
        for fname in os.listdir(_UPLOAD_DIR):
            fpath = os.path.join(_UPLOAD_DIR, fname)
            try:
                if os.path.isfile(fpath):
                    os.remove(fpath)
            except Exception:
                pass

    return {"ok": True, "message": "知识库已清空"}


# ============================================================
# 删除指定来源
# ============================================================

@router.delete("/{source_id}")
async def api_delete_source(source_id: str, current_user = Depends(get_current_user)):
    """从知识库中删除指定来源的所有切片"""
    kb = _get_kb()
    try:
        kb.collection.delete(where={"source_id": source_id})
        # 同步删除本地文件
        local_path = os.path.join(_UPLOAD_DIR, source_id)
        if os.path.isfile(local_path):
            os.remove(local_path)
        return {"ok": True, "deleted": source_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除失败: {e}")
