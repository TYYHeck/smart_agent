# -*- coding: utf-8 -*-
"""文件管理路由 —— 上传/列表/下载/预览"""

from __future__ import annotations
import os
import mimetypes
from fastapi import APIRouter, Depends, HTTPException, File, UploadFile
from fastapi.responses import FileResponse, JSONResponse

from src.auth.dependencies import get_current_user

router = APIRouter(prefix="/api/files", tags=["文件管理"])

# 安全限制：只允许访问工作目录下的文件
_WORK_DIR = os.path.abspath(os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
))

# 上传目录
_UPLOAD_DIR = os.path.join(_WORK_DIR, "output")
os.makedirs(_UPLOAD_DIR, exist_ok=True)

_MAX_UPLOAD_SIZE = 20 * 1024 * 1024  # 20MB


def _resolve_file_path(file: str) -> str:
    """解析文件路径，安全检查"""
    # 支持绝对路径和相对路径
    if os.path.isabs(file):
        real_path = os.path.realpath(file)
    else:
        real_path = os.path.realpath(os.path.join(_WORK_DIR, file))

    # 安全检查：确保在工作目录下
    if not real_path.startswith(_WORK_DIR):
        raise HTTPException(status_code=403, detail="禁止访问工作目录外的文件")
    if not os.path.isfile(real_path):
        raise HTTPException(status_code=404, detail="文件不存在")
    return real_path


@router.post("/upload")
async def api_upload_file(
    file: UploadFile = File(...),
    current_user = Depends(get_current_user),
):
    """上传文件（供对话/任务使用，保存到 output 目录）"""
    if not file or not file.filename:
        raise HTTPException(status_code=400, detail="未选择文件")

    # 安全检查：限制文件大小
    content = await file.read()
    if len(content) > _MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=400, detail=f"文件超过 {_MAX_UPLOAD_SIZE // 1024 // 1024}MB 限制")

    # 安全文件名
    safe_name = file.filename.replace("\\", "_").replace("/", "_")
    filepath = os.path.join(_UPLOAD_DIR, safe_name)

    # 避免覆盖：同名文件加序号
    base, ext = os.path.splitext(safe_name)
    counter = 1
    while os.path.exists(filepath):
        filepath = os.path.join(_UPLOAD_DIR, f"{base}_{counter}{ext}")
        counter += 1

    with open(filepath, "wb") as f:
        f.write(content)

    return {
        "ok": True,
        "filename": file.filename,
        "path": f"output/{os.path.basename(filepath)}",
        "size": len(content),
    }


@router.get("/list")
async def api_list_files(task_id: str = "", current_user = Depends(get_current_user)):
    """列出输出文件"""
    output_dir = os.path.join(_WORK_DIR, "output")
    files = []
    if os.path.isdir(output_dir):
        for fname in os.listdir(output_dir):
            fpath = os.path.join(output_dir, fname)
            if os.path.isfile(fpath):
                stat = os.stat(fpath)
                files.append({
                    "name": fname,
                    "path": f"output/{fname}",
                    "size": stat.st_size,
                    "modified": os.path.getmtime(fpath) if hasattr(os.path, 'getmtime') else None,
                })

    # 过滤特定 task 的文件（如果指定了 task_id）
    if task_id and files:
        files = [f for f in files if task_id in f["name"]]

    return {"ok": True, "files": files, "task_id": task_id}


@router.get("/download")
async def api_download_file(file: str, current_user = Depends(get_current_user)):
    """下载文件"""
    real_path = _resolve_file_path(file)
    mime_type, _ = mimetypes.guess_type(real_path)
    return FileResponse(
        real_path,
        media_type=mime_type or "application/octet-stream",
        filename=os.path.basename(real_path),
    )


@router.get("/preview")
async def api_preview_file(file: str, current_user = Depends(get_current_user)):
    """预览文本文件内容"""
    real_path = _resolve_file_path(file)
    mime_type, _ = mimetypes.guess_type(real_path)

    # 只预览文本类型文件
    if mime_type and not mime_type.startswith("text/") and mime_type not in (
        "application/json", "application/xml", "application/javascript",
    ):
        content_type = mime_type
    else:
        content_type = "text/plain"

    try:
        with open(real_path, encoding="utf-8") as f:
            content = f.read()
        if len(content) > 50000:
            content = content[:50000] + "\n\n... (文件过长，已截断到 50000 字符)"
        return {
            "ok": True,
            "content": content,
            "file": file,
            "size": len(content),
            "content_type": content_type or "text/plain",
        }
    except UnicodeDecodeError:
        raise HTTPException(status_code=400, detail="二进制文件不支持预览")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete")
async def api_delete_file(file: str, current_user = Depends(get_current_user)):
    """删除输出文件（从磁盘永久删除）"""
    real_path = _resolve_file_path(file)

    # 额外安全检查：只允许删除 output 目录下的文件
    output_dir = os.path.abspath(os.path.join(_WORK_DIR, "output"))
    if not os.path.abspath(real_path).startswith(output_dir):
        raise HTTPException(status_code=403, detail="只允许删除 output 目录下的文件")

    try:
        os.remove(real_path)
        return {"ok": True, "file": file}
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"删除失败: {e}")
