# -*- coding: utf-8 -*-
"""
Web 服务 —— 给 Agent 提供网页可视化交互界面

后端: FastAPI + SSE 流式推送
前端: 纯 HTML/JS 单页面 (暗色主题)

功能标签页: 仪表盘 | 对话 | 任务管理 | Agent 管理 | 配置
"""

from __future__ import annotations
from typing import Optional
import sys
import os
import json
import asyncio
import uuid
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import StreamingResponse, HTMLResponse, JSONResponse, Response, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
import uvicorn

from src.core.agent import Agent, AgentEvent, create_agent
from src.core.llm import LLMConfig
from src.core.task_manager import get_task_manager, AgentProxy
from src.core.orchestrator import ExecutionMode, patch_task_manager
from src.tools.builtin_tools import register_all

# 编排器线程池（用于 SSE 流式推送）
_orch_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="orchestrator")


# ============================================================
# 前端 HTML 页面 (内嵌)
# ============================================================

CHAT_PAGE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SmartAgent - 智能AI助手</title>
<style>
:root {
  --bg: #0d1117; --sidebar: #161b22; --card: #21262d;
  --primary: #58a6ff; --primary-hover: #79c0ff;
  --text: #c9d1d9; --text-bright: #f0f6fc; --muted: #8b949e;
  --user-bg: #1f6feb; --agent-bg: #21262d; --border: #30363d;
  --code-bg: #161b22; --success: #3fb950; --warn: #d2991d;
  --error: #f85149; --purple: #a371f7; --teal: #39d353;
}
* { margin:0; padding:0; box-sizing:border-box; }
body { font-family:'Segoe UI',system-ui,-apple-system,sans-serif; background:var(--bg); color:var(--text); height:100vh; display:flex; }
/* 侧边栏 */
.sidebar { width:260px; background:var(--sidebar); display:flex; flex-direction:column; border-right:1px solid var(--border); flex-shrink:0; overflow-y:auto; }
.sidebar-header { padding:16px; border-bottom:1px solid var(--border); }
.sidebar-header h1 { font-size:18px; color:var(--primary); letter-spacing:1px; }
.sidebar-header p { font-size:11px; color:var(--muted); margin-top:2px; }
.sidebar-section { padding:10px 16px; border-bottom:1px solid var(--border); }
.sidebar-section label.title { font-size:10px; color:var(--muted); display:block; margin-bottom:6px; text-transform:uppercase; letter-spacing:1px; }
.mode-row { display:flex; align-items:center; justify-content:space-between; padding:4px 0; font-size:12px; }
.toggle-switch { position:relative; width:36px; height:20px; }
.toggle-switch input { opacity:0; width:0; height:0; }
.toggle-slider { position:absolute; cursor:pointer; top:0;left:0;right:0;bottom:0; background:#444; border-radius:20px; transition:.3s; }
.toggle-slider:before { content:''; position:absolute; height:14px; width:14px; left:3px; bottom:3px; background:white; border-radius:50%; transition:.3s; }
.toggle-switch input:checked+.toggle-slider { background:var(--primary); }
.toggle-switch input:checked+.toggle-slider:before { transform:translateX(16px); }
.sidebar-stats { padding:10px 16px; font-size:11px; color:var(--muted); border-bottom:1px solid var(--border); }
.sidebar-stats div { margin:4px 0; }
.sidebar-stats span { color:var(--primary); font-weight:bold; }
.sidebar-footer { margin-top:auto; padding:12px 16px; border-top:1px solid var(--border); font-size:10px; color:var(--muted); }
/* 标签页 */
.tab-bar { display:flex; border-bottom:1px solid var(--border); background:var(--sidebar); flex-shrink:0; }
.tab-btn { padding:10px 16px; background:none; border:none; color:var(--muted); font-size:13px; cursor:pointer; border-bottom:2px solid transparent; transition:.2s; white-space:nowrap; }
.tab-btn.active { color:var(--primary); border-bottom-color:var(--primary); }
.tab-btn:hover { color:var(--text); }
.tab-btn .tab-icon { margin-right:4px; }
/* 主区域 */
.main { flex:1; display:flex; flex-direction:column; min-width:0; }
.tab-content { flex:1; overflow-y:auto; display:none; }
.tab-content.active { display:flex; flex-direction:column; }
/* ---- 仪表盘 ---- */
.dashboard { padding:24px; gap:20px; }
.stat-cards { display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:12px; margin-bottom:20px; }
.stat-card { background:var(--card); border:1px solid var(--border); border-radius:10px; padding:16px; text-align:center; }
.stat-card .stat-val { font-size:28px; font-weight:bold; color:var(--text-bright); }
.stat-card .stat-label { font-size:11px; color:var(--muted); margin-top:4px; text-transform:uppercase; }
.stat-card.accent-blue { border-top:3px solid var(--primary); }
.stat-card.accent-green { border-top:3px solid var(--success); }
.stat-card.accent-yellow { border-top:3px solid var(--warn); }
.stat-card.accent-red { border-top:3px solid var(--error); }
.stat-card.accent-purple { border-top:3px solid var(--purple); }
.stat-card.accent-teal { border-top:3px solid var(--teal); }
.dash-section { margin-bottom:20px; }
.dash-section h3 { font-size:14px; color:var(--text-bright); margin-bottom:10px; border-bottom:1px solid var(--border); padding-bottom:8px; }
.dash-grid { display:grid; grid-template-columns:1fr 1fr; gap:16px; }
/* 数据表格 */
.data-table { width:100%; border-collapse:collapse; font-size:13px; }
.data-table th { text-align:left; padding:8px 12px; border-bottom:2px solid var(--border); color:var(--muted); font-size:11px; text-transform:uppercase; }
.data-table td { padding:8px 12px; border-bottom:1px solid var(--border); }
.data-table tr:hover td { background:var(--card); }
.table-actions { display:flex; gap:6px; }
/* 按钮 */
.btn { padding:6px 14px; border-radius:6px; border:1px solid var(--border); font-size:12px; cursor:pointer; transition:.2s; }
.btn-primary { background:var(--primary); color:#fff; border-color:var(--primary); }
.btn-primary:hover { opacity:.85; }
.btn-danger { background:var(--error); color:#fff; border-color:var(--error); }
.btn-danger:hover { opacity:.85; }
.btn-sm { padding:4px 10px; font-size:11px; }
.btn-outline { background:transparent; color:var(--text); }
.btn-outline:hover { background:var(--card); }
.btn-outline.active, .task-filter.active { background:var(--primary); color:#fff; border-color:var(--primary); }
/* 标签 / 徽章 */
.skill-tag { display:inline-block; padding:2px 8px; border-radius:10px; font-size:10px; margin:1px 2px; background:rgba(88,166,255,.15); color:var(--primary); }
.status-badge { font-size:11px; padding:3px 8px; border-radius:10px; font-weight:bold; }
.status-badge.pending { background:#2a3a1a; color:var(--warn); }
.status-badge.running { background:#1a2740; color:var(--primary); }
.status-badge.completed { background:#1a2a1a; color:var(--success); }
.status-badge.failed { background:#3a1a1a; color:var(--error); }
.status-badge.idle { background:#1a2a1a; color:var(--success); }
.status-badge.busy { background:#1a2740; color:var(--primary); }
/* 模态框 */
.modal-overlay { display:none; position:fixed; top:0;left:0;right:0;bottom:0; background:rgba(0,0,0,.6); z-index:1000; align-items:center; justify-content:center; }
.modal-overlay.show { display:flex; }
.modal { background:var(--sidebar); border:1px solid var(--border); border-radius:12px; padding:24px; min-width:420px; max-width:560px; max-height:80vh; overflow-y:auto; }
.modal.wide { max-width:700px; }
.modal h3 { color:var(--text-bright); margin-bottom:16px; font-size:16px; }
.modal-actions { display:flex; gap:8px; justify-content:flex-end; margin-top:16px; }
/* 任务详情 */
.task-detail { font-size:13px; }
.detail-section { margin-bottom:14px; padding-bottom:12px; border-bottom:1px solid var(--border); }
.detail-section:last-child { border-bottom:none; }
.detail-label { font-size:10px; color:var(--muted); text-transform:uppercase; letter-spacing:0.5px; margin-bottom:4px; }
.detail-value { color:var(--text); line-height:1.6; }
.detail-value.mono { font-family:'Cascadia Code',Consolas,monospace; color:var(--primary); }
.result-box { background:var(--code-bg); border:1px solid var(--border); border-radius:6px; padding:10px 12px; max-height:200px; overflow-y:auto; white-space:pre-wrap; font-size:12px; }
.error-box { background:#3a1a1a; border:1px solid var(--error); border-radius:6px; padding:10px 12px; max-height:200px; overflow-y:auto; white-space:pre-wrap; font-size:12px; color:#ffaaaa; }
.event-log { max-height:260px; overflow-y:auto; }
.event-item { padding:5px 0; border-bottom:1px solid rgba(48,54,61,.5); font-size:12px; }
.event-item:last-child { border-bottom:none; }
.event-icon { margin-right:6px; }
.event-time { color:var(--muted); margin-right:8px; font-family:monospace; font-size:11px; }
.event-name { color:var(--text-bright); font-weight:500; }
.event-data { color:var(--muted); font-size:11px; margin-top:2px; margin-left:38px; word-break:break-all; }
.output-files-list { max-height:240px; overflow-y:auto; }
.output-file-row { display:flex; align-items:center; gap:8px; padding:6px 0; border-bottom:1px solid rgba(48,54,61,.4); flex-wrap:wrap; }
.output-file-row:last-child { border-bottom:none; }
.output-file-row .file-icon { font-size:14px; flex-shrink:0; }
.output-file-row .file-name { color:var(--text-bright); font-weight:500; font-size:12px; flex-shrink:0; }
.output-file-row .file-path-muted { color:var(--muted); font-size:10px; font-family:monospace; flex:1; min-width:0; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
.output-file-row .btn-sm { padding:2px 10px; font-size:11px; }
.code-block { background:var(--code-bg); border:1px solid var(--border); border-radius:6px; padding:12px 16px; overflow:auto; font-size:12px; line-height:1.6; white-space:pre; margin:0; }
.form-group { margin-bottom:12px; }
.form-group label { display:block; font-size:12px; color:var(--muted); margin-bottom:4px; }
.form-input, .form-textarea, .form-select { width:100%; background:var(--card); border:1px solid var(--border); border-radius:6px; padding:8px 10px; color:var(--text); font-size:13px; outline:none; font-family:inherit; }
.form-input:focus, .form-textarea:focus, .form-select:focus { border-color:var(--primary); }
.form-textarea { resize:vertical; min-height:60px; }
.form-help { font-size:11px; color:var(--muted); margin-top:2px; }
/* ---- 聊天 ---- */
.chat-area { flex:1; overflow-y:auto; padding:24px; display:flex; flex-direction:column; gap:16px; }
.message { display:flex; gap:12px; animation:fadeIn .3s; max-width:85%; }
.message.user { align-self:flex-end; flex-direction:row-reverse; }
.message.agent { align-self:flex-start; }
.avatar { width:36px; height:36px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-size:16px; flex-shrink:0; }
.message.user .avatar { background:var(--primary); }
.message.agent .avatar { background:var(--card); }
.bubble { padding:12px 16px; border-radius:12px; font-size:14px; line-height:1.6; word-break:break-word; }
.message.user .bubble { background:var(--user-bg); color:white; border-bottom-right-radius:4px; }
.message.agent .bubble { background:var(--agent-bg); color:var(--text); border-bottom-left-radius:4px; border:1px solid var(--border); }
.bubble pre { background:var(--code-bg); padding:12px; border-radius:8px; overflow-x:auto; font-size:13px; margin:8px 0; border:1px solid var(--border); }
.bubble code { background:var(--code-bg); padding:2px 6px; border-radius:4px; font-size:13px; }
.bubble pre code { padding:0; background:none; }
.bubble ul,.bubble ol { padding-left:20px; margin:6px 0; }
.bubble li { margin:3px 0; line-height:1.5; }
.bubble h1,.bubble h2,.bubble h3 { margin:8px 0 4px; color:var(--text-bright); }
.bubble h1 { font-size:1.3em; } .bubble h2 { font-size:1.15em; } .bubble h3 { font-size:1.05em; }
.bubble p { margin:4px 0; }
.bubble strong { color:var(--text-bright); }
.bubble table { border-collapse:collapse; margin:8px 0; font-size:13px; width:100%; }
.bubble th,.bubble td { border:1px solid var(--border); padding:6px 10px; text-align:left; }
.bubble th { background:var(--code-bg); color:var(--text-bright); font-weight:600; }
.bubble hr { border:none; border-top:1px solid var(--border); margin:12px 0; }
.bubble blockquote { border-left:3px solid var(--primary); padding-left:12px; margin:8px 0; color:var(--muted); }
.tool-call-card { background:#1a2740; border-left:3px solid var(--warn); padding:10px 14px; border-radius:8px; margin:8px 0; font-size:13px; color:#ccc; }
.tool-call-card .tool-name { color:var(--warn); font-weight:bold; }
.tool-call-card .tool-args { color:var(--muted); font-size:12px; margin-top:4px; }
.tool-call-card.success { border-left-color:var(--success); }
.tool-call-card .result-preview { font-size:12px; color:var(--muted); margin-top:4px; max-height:80px; overflow-y:auto; }
/* 输入区 */
.input-area { padding:16px 24px; border-top:1px solid var(--border); display:flex; gap:12px; align-items:flex-end; position:relative; }
.input-area textarea { flex:1; background:var(--card); border:1px solid var(--border); border-radius:12px; padding:12px 16px; color:var(--text); font-size:14px; resize:none; outline:none; font-family:inherit; min-height:44px; max-height:150px; }
.input-area textarea:focus { border-color:var(--primary); }
.input-area textarea::placeholder { color:var(--muted); }
.input-area button { background:var(--primary); color:white; border:none; border-radius:10px; padding:10px 20px; font-size:14px; cursor:pointer; transition:opacity .2s; white-space:nowrap; }
.input-area button:hover { opacity:.85; }
.input-area button:disabled { opacity:.5; cursor:not-allowed; }
/* 任务面板 */
.task-panel { padding:24px; }
.task-panel h2 { color:var(--text-bright); margin-bottom:16px; font-size:18px; }
.task-card { background:var(--card); border:1px solid var(--border); border-radius:8px; padding:12px 16px; margin-bottom:10px; display:flex; justify-content:space-between; align-items:center; }
.task-info { flex:1; }
.task-title { font-size:14px; color:var(--text-bright); }
.task-meta { font-size:12px; color:var(--muted); margin-top:4px; }
.publish-form { display:flex; gap:10px; margin-bottom:20px; flex-wrap:wrap; }
.publish-form input { flex:1; min-width:200px; background:var(--card); border:1px solid var(--border); border-radius:8px; padding:10px 14px; color:var(--text); font-size:14px; outline:none; }
.publish-form input:focus { border-color:var(--primary); }
.publish-form button { background:var(--primary); color:white; border:none; border-radius:8px; padding:10px 18px; font-size:14px; cursor:pointer; }
/* Combobox */
.combo-wrapper { position:relative; }
.combo-input { width:100%; background:var(--card); color:var(--text); border:1px solid var(--border); border-radius:6px; padding:8px 28px 8px 10px; font-size:13px; outline:none; }
.combo-input:focus { border-color:var(--primary); }
.combo-arrow { position:absolute; right:10px; top:50%; transform:translateY(-50%); pointer-events:none; font-size:10px; color:var(--muted); }
.combo-dropdown { display:none; position:absolute; top:100%; left:0; right:0; background:var(--sidebar); border:1px solid var(--border); border-radius:6px; max-height:200px; overflow-y:auto; z-index:100; margin-top:2px; box-shadow:0 4px 12px rgba(0,0,0,.4); }
.combo-dropdown.show { display:block; }
.combo-item { padding:8px 12px; font-size:13px; cursor:pointer; color:var(--text); display:flex; align-items:center; gap:6px; }
.combo-item:hover,.combo-item.active { background:var(--card); color:var(--primary); }
.combo-item .combo-hint { font-size:11px; color:var(--muted); margin-left:auto; }
.combo-item .combo-desc { font-size:11px; color:var(--muted); }
/* 执行模式切换 — 分段胶囊控件 */
.mode-toggle {
  display: flex;
  background: var(--bg);
  border: 1px solid var(--border);
  border-radius: 10px;
  padding: 3px;
  gap: 3px;
}
.mode-btn {
  flex: 1;
  padding: 7px 12px;
  font-size: 13px;
  background: transparent;
  border: none;
  border-radius: 8px;
  color: var(--muted);
  cursor: pointer;
  transition: all 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  white-space: nowrap;
}
.mode-btn:hover { color: var(--text); }
.mode-btn.active {
  background: var(--primary);
  color: #fff;
  font-weight: 600;
  box-shadow: 0 1px 4px rgba(88,166,255,.35);
}
/* 编排实时面板 */
.orch-live-container { margin:0 0 12px; }
.orch-flow { display:flex; flex-direction:column; gap:8px; }
.orch-stage-card { background:var(--bg); border:1px solid var(--border); border-radius:8px; padding:10px 14px; animation:fadeIn .3s; }
.orch-stage-card.stage-active { border-color:var(--primary); box-shadow:0 0 8px rgba(88,166,255,.15); }
.orch-stage-card.stage-done { border-color:var(--success); opacity:.85; }
.orch-stage-card.stage-error { border-color:var(--error); }
.orch-stage-card .stage-header { display:flex; align-items:center; gap:8px; margin-bottom:4px; }
.orch-stage-card .stage-icon { font-size:18px; }
.orch-stage-card .stage-title { font-size:13px; color:var(--text-bright); font-weight:bold; }
.orch-stage-card .stage-time { font-size:11px; color:var(--muted); margin-left:auto; }
.orch-stage-card .stage-agent { font-size:12px; color:var(--primary); }
.orch-stage-card .stage-detail { font-size:12px; color:var(--muted); margin-top:4px; line-height:1.5; }
.orch-agent-tag { display:inline-block; padding:2px 8px; border-radius:12px; font-size:11px; background:rgba(163,113,247,.2); color:var(--purple); margin:2px 4px 2px 0; }
.orch-agent-tag.active { background:rgba(88,166,255,.2); color:var(--primary); animation:pulse 1.5s infinite; }
.orch-agent-tag.done { background:rgba(63,185,80,.2); color:var(--success); }
.orch-agent-tag.error { background:rgba(248,81,73,.2); color:var(--error); }
.orch-agent-tag.agent-done { background:rgba(63,185,80,.2); color:var(--success); }
.orch-agent-tag.agent-running { background:rgba(88,166,255,.2); color:var(--primary); animation:pulse 1.5s infinite; }
.orch-agent-tag.agent-failed { background:rgba(248,81,73,.2); color:var(--error); }
.orch-agent-tag.agent-pending { background:rgba(139,148,158,.15); color:var(--muted); }
@keyframes pulse { 0%,100% { opacity:1; } 50% { opacity:.5; } }
/* 模式徽章 */
.orch-mode-badge { display:inline-block; padding:4px 12px; border-radius:6px; font-size:13px; font-weight:bold; letter-spacing:1px; }
.orch-mode-badge.auto { background:rgba(163,113,247,.2); color:var(--purple); }
.orch-mode-badge.single { background:rgba(88,166,255,.2); color:var(--primary); }
.orch-mode-badge.parallel { background:rgba(210,153,29,.2); color:var(--warn); }
.orch-mode-badge.pipeline { background:rgba(57,211,83,.2); color:var(--success); }
.orch-mode-badge.collaborative { background:rgba(248,81,73,.2); color:var(--error); }
/* Agent 状态网格（任务详情弹窗内） */
.agent-status-grid { display:flex; flex-direction:column; gap:6px; }
.agent-status-row { display:flex; align-items:center; gap:8px; padding:4px 0; }
.agent-status-name { font-size:13px; color:var(--text); min-width:80px; }
.agent-status-state { font-size:11px; padding:1px 8px; border-radius:10px; }
.agent-status-state.agent-done { color:var(--success); background:rgba(63,185,80,.15); }
.agent-status-state.agent-running { color:var(--primary); background:rgba(88,166,255,.15); }
.agent-status-state.agent-failed { color:var(--error); background:rgba(248,81,73,.15); }
.agent-status-state.agent-pending { color:var(--muted); background:rgba(139,148,158,.1); }
/* 编排结果展示 */
.orch-result { margin-top:12px; }
.orch-result .result-header { font-size:14px; color:var(--text-bright); margin-bottom:8px; padding-bottom:6px; border-bottom:1px solid var(--border); }
.orch-result .result-item { background:var(--bg); border:1px solid var(--border); border-radius:6px; padding:10px; margin-bottom:8px; }
.orch-result .result-item-title { font-size:12px; color:var(--primary); margin-bottom:4px; font-weight:bold; }
.orch-result .result-item-text { font-size:12px; color:var(--muted); line-height:1.5; white-space:pre-wrap; max-height:120px; overflow-y:auto; }
.orch-summary-box { background:var(--bg); border:2px solid var(--purple); border-radius:8px; padding:12px; margin-top:8px; }
.orch-summary-box .summary-title { font-size:14px; color:var(--purple); font-weight:bold; margin-bottom:8px; }
.orch-summary-box .summary-text { font-size:13px; color:var(--text); line-height:1.6; white-space:pre-wrap; }
/* 编排流程指示器 */
.orch-flow-indicator { display:flex; align-items:center; gap:6px; padding:8px 0; flex-wrap:wrap; }
.orch-flow-node { padding:6px 12px; border-radius:16px; font-size:12px; background:var(--card); border:1px solid var(--border); color:var(--muted); white-space:nowrap; }
.orch-flow-node.active { background:rgba(88,166,255,.15); border-color:var(--primary); color:var(--primary); }
.orch-flow-node.done { background:rgba(63,185,80,.15); border-color:var(--success); color:var(--success); }
.orch-flow-arrow { color:var(--border); font-size:12px; }
/* 输出文件 */
.orch-output-files { margin-top:8px; }
.orch-output-files a { color:var(--primary); font-size:12px; text-decoration:none; margin-right:12px; }
.orch-output-files a:hover { text-decoration:underline; }
/* 编排结果内嵌到任务卡片 */
.task-card-orch { margin-top:8px; padding:8px 12px; background:rgba(88,166,255,.04); border-radius:6px; border-left:3px solid var(--purple); }
.task-card-orch .orch-mode-badge { font-size:11px; padding:2px 8px; margin-right:6px; }
.slash-dropdown { display:none; position:absolute; bottom:100%; left:0; min-width:240px; background:var(--sidebar); border:1px solid var(--border); border-radius:8px; max-height:240px; overflow-y:auto; z-index:200; margin-bottom:4px; box-shadow:0 -2px 12px rgba(0,0,0,.4); }
.slash-dropdown.show { display:block; }
.slash-item { padding:10px 14px; cursor:pointer; display:flex; align-items:center; gap:10px; color:var(--text); }
.slash-item:hover,.slash-item.active { background:var(--card); color:var(--primary); }
.slash-item .slash-cmd { font-weight:bold; font-size:14px; min-width:70px; }
.slash-item .slash-desc { font-size:12px; color:var(--muted); }
.slash-item .slash-args { font-size:11px; color:var(--muted); margin-left:auto; }
/* 动画 */
.typing-indicator { display:flex; gap:4px; padding:8px 0; }
.typing-indicator span { width:8px; height:8px; border-radius:50%; background:var(--muted); animation:bounce 1.2s infinite; }
.typing-indicator span:nth-child(2) { animation-delay:.2s; }
.typing-indicator span:nth-child(3) { animation-delay:.4s; }
@keyframes bounce { 0%,60%,100% { transform:translateY(0); } 30% { transform:translateY(-8px); } }
@keyframes fadeIn { from { opacity:0; transform:translateY(8px); } to { opacity:1; transform:translateY(0); } }
::-webkit-scrollbar { width:6px; }
::-webkit-scrollbar-track { background:transparent; }
::-webkit-scrollbar-thumb { background:var(--border); border-radius:3px; }
/* 响应式 */
@media(max-width:768px) { .sidebar { width:200px; } .dash-grid { grid-template-columns:1fr; } .stat-cards { grid-template-columns:repeat(auto-fit,minmax(120px,1fr)); } }
</style>
</head>
<body>

<div class="sidebar">
  <div class="sidebar-header"><h1>SmartAgent</h1><p>智能 AI 助手</p></div>
  <div class="sidebar-section model-selector">
    <label class="title">模型</label>
    <div class="combo-wrapper" id="modelComboWrapper">
      <input class="combo-input" id="modelComboInput" placeholder="搜索模型..." autocomplete="off">
      <span class="combo-arrow">▼</span><div class="combo-dropdown" id="modelComboDropdown"></div>
    </div>
  </div>
  <div class="sidebar-section">
    <label class="title">Agent 模式</label>
    <div class="mode-row"><span>任务计划</span><label class="toggle-switch"><input type="checkbox" id="togglePlan" onchange="toggleMode('planning')"><span class="toggle-slider"></span></label></div>
    <div class="mode-row"><span>知识库 RAG</span><label class="toggle-switch"><input type="checkbox" id="toggleRag" onchange="toggleMode('rag')" checked><span class="toggle-slider"></span></label></div>
    <div class="mode-row"><span>自我反思</span><label class="toggle-switch"><input type="checkbox" id="toggleReflect" onchange="toggleMode('reflection')"><span class="toggle-slider"></span></label></div>
  </div>
  <div class="sidebar-stats">
    <div>工具: <span id="stat-tools">0</span> 个</div>
    <div>对话: <span id="stat-turns">0</span> 轮</div>
    <div>LangChain: <span id="stat-lc">检测中</span></div>
  </div>
  <div class="sidebar-footer">基于 LangChain 构建<br>ReAct · 思考→行动→观察</div>
</div>

<div class="main">
  <div class="tab-bar">
    <button class="tab-btn active" onclick="switchTab('dashboard')"><span class="tab-icon">📊</span>仪表盘</button>
    <button class="tab-btn" onclick="switchTab('chat')"><span class="tab-icon">💬</span>对话</button>
    <button class="tab-btn" onclick="switchTab('tasks')"><span class="tab-icon">📋</span>任务管理</button>
    <button class="tab-btn" onclick="switchTab('agents')"><span class="tab-icon">🤖</span>Agent管理</button>
    <button class="tab-btn" onclick="switchTab('files')"><span class="tab-icon">📁</span>输出文件</button>
    <button class="tab-btn" onclick="switchTab('config')"><span class="tab-icon">⚙️</span>配置</button>
  </div>

  <!-- ===== 仪表盘 ===== -->
  <div id="tab-dashboard" class="tab-content active">
    <div class="dashboard">
      <div class="stat-cards" id="statCards"></div>
      <div class="dash-grid">
        <div class="dash-section">
          <h3>近期任务</h3>
          <div id="dashRecentTasks"></div>
        </div>
        <div class="dash-section">
          <h3>Agent 状态</h3>
          <div id="dashAgents"></div>
        </div>
      </div>
    </div>
  </div>

  <!-- ===== 对话 ===== -->
  <div id="tab-chat" class="tab-content">
    <div class="chat-area" id="chat"></div>
    <div class="input-area">
      <div class="slash-dropdown" id="slashDropdown"></div>
      <textarea id="input" placeholder="输入消息，/ 查看命令，Enter 发送，Shift+Enter 换行" rows="1" oninput="autoResize(this)"></textarea>
      <button id="sendBtn" onclick="sendMessage()">发送</button>
    </div>
  </div>

  <!-- ===== 任务管理 ===== -->
  <div id="tab-tasks" class="tab-content">
    <div class="task-panel">
      <h2>任务管理</h2>
      <!-- 统一发布区 -->
      <div class="publish-form">
        <input id="taskInput" placeholder="输入任务描述..." onkeydown="if(event.key==='Enter')publishTask()">
        <div class="combo-wrapper" id="agentComboWrapper" style="min-width:140px;">
          <input class="combo-input" id="agentComboInput" placeholder="自动分配" autocomplete="off" style="border-radius:8px;padding:10px 28px 10px 12px;">
          <span class="combo-arrow">▼</span><div class="combo-dropdown" id="agentComboDropdown"></div>
        </div>
        <!-- 执行模式切换 -->
        <div class="mode-toggle" id="modeToggle">
          <button class="mode-btn active" onclick="setExecMode('single',this)">👤 单 Agent</button>
          <button class="mode-btn" onclick="setExecMode('orchestrated',this)">🤖 多 Agent 编排</button>
        </div>
        <select id="orchModeSelect" class="form-select" style="min-width:130px;display:none;" onchange="onOrchModeChange()">
          <option value="auto">自动选择策略</option>
          <option value="parallel">⚡ 并行执行</option>
          <option value="pipeline">🔗 流水线</option>
          <option value="collaborative">🤝 协作讨论</option>
        </select>
        <button onclick="publishTask()" id="publishBtn">发布任务</button>
      </div>
      <!-- 编排实时面板 -->
      <div id="orchLivePanel" style="display:none;">
        <div id="orchLiveContent"></div>
      </div>
      <div style="margin-bottom:12px;" id="taskFilterBar">
        <button onclick="filterTasks('', this)" class="btn btn-outline btn-sm task-filter active">全部</button>
        <button onclick="filterTasks('pending', this)" class="btn btn-outline btn-sm task-filter">待处理</button>
        <button onclick="filterTasks('running', this)" class="btn btn-outline btn-sm task-filter">执行中</button>
        <button onclick="filterTasks('completed', this)" class="btn btn-outline btn-sm task-filter">已完成</button>
      </div>
      <div id="taskList"></div>
    </div>
  </div>

  <!-- ===== Agent 管理 ===== -->
  <div id="tab-agents" class="tab-content">
    <div class="task-panel">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
        <h2 style="margin:0;">Agent 管理</h2>
        <button class="btn btn-primary" onclick="showCreateAgentModal()">+ 创建 Agent</button>
      </div>
      <div id="agentTable"></div>
    </div>
  </div>

  <!-- ===== 配置 ===== -->
  <div id="tab-config" class="tab-content">
    <div class="task-panel" id="configPanel">
      <h2>系统配置</h2>
      <p style="color:var(--muted);margin-bottom:16px;">修改后点击「保存」写入 config.yaml，需重启服务生效。</p>
      <div id="configSections"></div>
    </div>
  </div>

  <!-- ===== 输出文件 ===== -->
  <div id="tab-files" class="tab-content">
    <div class="task-panel">
      <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:16px;">
        <h2 style="margin:0;">📁 输出文件</h2>
        <button class="btn btn-outline btn-sm" onclick="loadOutputFiles()">🔄 刷新</button>
      </div>
      <p style="color:var(--muted);margin-bottom:12px;font-size:12px;">
        Agent 执行任务时生成的文件列表。点击下载或预览。
      </p>
      <div id="outputFilesList" style="margin-bottom:12px;">
        <div style="color:var(--muted);padding:12px 0;">加载中...</div>
      </div>
    </div>
  </div>
</div>

<!-- 通用模态框 -->
<div class="modal-overlay" id="modalOverlay" onclick="if(event.target===this)closeModal()">
  <div class="modal" id="modalContent"></div>
</div>

<script>
// ==================== 全局工具 ====================
const $ = id => document.getElementById(id);
const api = (url, opts) => fetch(url, opts).then(r => r.json());

function formatTime(ts) { if(!ts) return '-'; const d=new Date(ts); return d.toLocaleString('zh-CN'); }
function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;'); }
function escAttr(s) { return String(s).replace(/"/g,'&quot;').replace(/'/g,'&#39;'); }

// ==================== 文件下载 / 预览 ====================
function downloadFile(filepath) {
  const url = '/api/files/download?file=' + encodeURIComponent(filepath);
  const a = document.createElement('a');
  a.href = url;
  a.download = '';
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
}

async function previewFile(filepath) {
  try {
    const url = '/api/files/preview?file=' + encodeURIComponent(filepath);
    const data = await api(url);
    if(!data.ok) return alert(data.detail || '预览失败');
    const fname = filepath.split('/').pop() || filepath.split('\\').pop() || filepath;
    const langClass = getCodeLang(filepath);
    openModal('📄 预览: ' + escHtml(fname),
      `<div style="max-height:70vh;overflow:auto;">
        <pre class="code-block ${langClass}"><code>${escHtml(data.content)}</code></pre>
        <p style="margin-top:8px;color:var(--muted);font-size:12px;">
          文件: ${escHtml(filepath)} &nbsp;|&nbsp; ${data.size||0} 字符
          <a href="/api/files/download?file=${encodeURIComponent(filepath)}" class="btn btn-sm btn-outline" style="margin-left:8px;">⬇ 下载</a>
        </p>
      </div>`, null, null);
    const saveBtn = $('modalSaveBtn');
    if(saveBtn) saveBtn.style.display = 'none';
    $('modalContent').classList.add('wide');
  } catch(e) { console.error(e); alert('预览失败: ' + e.message); }
}

function getCodeLang(filepath) {
  const ext = (filepath||'').toLowerCase().split('.').pop();
  const map = {py:'language-python',js:'language-javascript',ts:'language-typescript',
               html:'language-html',css:'language-css',json:'language-json',md:'language-markdown',
               sql:'language-sql',yaml:'language-yaml',yml:'language-yaml',sh:'language-bash',
               bash:'language-bash',xml:'language-xml',java:'language-java',go:'language-go',
               rs:'language-rust',cpp:'language-cpp',c:'language-c',rb:'language-ruby',
               php:'language-php',swift:'language-swift',kt:'language-kotlin'};
  return map[ext] || '';
}

// ==================== 模态框 ====================
function openModal(title, bodyHtml, onSave, saveLabel) {
  saveLabel = saveLabel || '保存';
  $('modalContent').innerHTML = `<h3>${title}</h3>${bodyHtml}
    <div class="modal-actions">
      <button class="btn btn-outline" onclick="closeModal()">取消</button>
      <button class="btn btn-primary" id="modalSaveBtn">${saveLabel}</button>
    </div>`;
  $('modalOverlay').classList.add('show');
  if(onSave) $('modalSaveBtn').onclick = () => { onSave(); closeModal(); };
}
function closeModal() { 
  $('modalOverlay').classList.remove('show'); 
  $('modalContent').classList.remove('wide');
}

// ==================== 标签页 ====================
function switchTab(tab) {
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
  const btn = document.querySelector(`[onclick="switchTab('${tab}')"]`);
  if(btn) btn.classList.add('active');
  const content = $('tab-' + tab);
  if(content) content.classList.add('active');
  
  if(tab === 'dashboard') loadDashboard();
  else if(tab === 'tasks') { refreshTasks(''); refreshAgentCombo(); }
  else if(tab === 'agents') loadAgents();
  else if(tab === 'files') loadOutputFiles();
  else if(tab === 'config') loadConfig();
}

// ==================== 仪表盘 ====================
async function loadDashboard() {
  try {
    const data = await api('/api/dashboard/stats');
    if(!data.ok) return;
    const s = data.stats;

    // 统计卡片
    $('statCards').innerHTML = [
      {v:s.pending, l:'待处理', c:'accent-yellow'},
      {v:s.running, l:'执行中', c:'accent-blue'},
      {v:s.completed, l:'已完成', c:'accent-green'},
      {v:s.failed, l:'失败', c:'accent-red'},
      {v:s.agents_idle, l:'空闲 Agent', c:'accent-teal'},
      {v:s.tools, l:'工具数', c:'accent-purple'},
    ].map(c => `<div class="stat-card ${c.c}"><div class="stat-val">${c.v}</div><div class="stat-label">${c.l}</div></div>`).join('');

    // 近期任务
    let taskHtml = '';
    if(!data.recent_tasks || data.recent_tasks.length===0) {
      taskHtml = '<div style="color:var(--muted);padding:8px;">暂无任务</div>';
    } else {
      taskHtml = '<table class="data-table"><tr><th>ID</th><th>标题</th><th>状态</th><th>时间</th></tr>' +
        data.recent_tasks.map(t => `<tr>
          <td style="font-family:monospace;">${t.id}</td>
          <td>${escHtml(t.title||t.description||'').slice(0,40)}</td>
          <td><span class="status-badge ${t.status}">${t.status}</span></td>
          <td style="font-size:11px;color:var(--muted);">${(t.created_at||'').slice(0,16)}</td>
        </tr>`).join('') + '</table>';
    }
    $('dashRecentTasks').innerHTML = taskHtml;

    // Agent 列表
    let agentHtml = '';
    if(!data.agents || data.agents.length===0) {
      agentHtml = '<div style="color:var(--muted);padding:8px;">暂无 Agent，请先在 Agent 管理页创建</div>';
    } else {
      agentHtml = '<table class="data-table"><tr><th>名称</th><th>状态</th><th>技能</th></tr>' +
        data.agents.map(a => `<tr>
          <td><b>${escHtml(a.name)}</b></td>
          <td><span class="status-badge ${a.status}">${a.status}</span></td>
          <td>${(a.skills||[]).map(s => `<span class="skill-tag">${escHtml(s)}</span>`).join('')||'-'}</td>
        </tr>`).join('') + '</table>';
    }
    $('dashAgents').innerHTML = agentHtml;

    // 更新侧边栏
    $('stat-tools').textContent = s.tools;
  } catch(e) { console.error('仪表盘加载失败:', e); }
}

// ==================== Combobox ====================
function initCombo(wrapperId, inputId, dropdownId, items, onSelect, renderItem) {
  const wrapper = $(wrapperId), input = $(inputId), dropdown = $(dropdownId);
  let activeIdx = -1;

  function showDropdown() {
    const query = input.value.toLowerCase();
    const filtered = items.filter(item => {
      const label = (item.label||item.name||item.cmd||'').toLowerCase();
      const desc = (item.desc||item.description||'').toLowerCase();
      return label.includes(query) || desc.includes(query);
    });
    if(filtered.length===0) { dropdown.classList.remove('show'); return; }
    dropdown.innerHTML = filtered.map((item,i) =>
      `<div class="combo-item" data-idx="${i}">${renderItem(item,query)}</div>`).join('');
    dropdown.classList.add('show'); activeIdx = -1;
    dropdown.querySelectorAll('.combo-item').forEach(el => {
      el.addEventListener('mousedown', e => {
        e.preventDefault();
        onSelect(filtered[parseInt(el.dataset.idx)]);
        dropdown.classList.remove('show'); input.focus();
      });
    });
  }

  input.addEventListener('focus', showDropdown);
  input.addEventListener('input', showDropdown);
  input.addEventListener('keydown', e => {
    const itemEls = dropdown.querySelectorAll('.combo-item');
    if(e.key==='ArrowDown') {
      e.preventDefault();
      if(!dropdown.classList.contains('show')) { showDropdown(); return; }
      activeIdx = Math.min(activeIdx+1, itemEls.length-1);
      itemEls.forEach((el,i) => el.classList.toggle('active', i===activeIdx));
      if(itemEls[activeIdx]) itemEls[activeIdx].scrollIntoView({block:'nearest'});
    } else if(e.key==='ArrowUp') {
      e.preventDefault();
      activeIdx = Math.max(activeIdx-1, -1);
      itemEls.forEach((el,i) => el.classList.toggle('active', i===activeIdx));
      if(itemEls[activeIdx]) itemEls[activeIdx].scrollIntoView({block:'nearest'});
    } else if(e.key==='Enter') {
      if(activeIdx>=0 && itemEls[activeIdx]) { e.preventDefault(); itemEls[activeIdx].click(); return; }
      dropdown.classList.remove('show');
    } else if(e.key==='Escape') { dropdown.classList.remove('show'); activeIdx = -1; }
  });
  document.addEventListener('click', e => { if(!wrapper.contains(e.target)) dropdown.classList.remove('show'); });
  return { input, dropdown, showDropdown };
}

function highlightMatch(text, query) {
  if(!query) return text;
  const i = text.toLowerCase().indexOf(query.toLowerCase());
  if(i===-1) return text;
  return text.slice(0,i) + '<b style="color:var(--primary)">' + text.slice(i,i+query.length) + '</b>' + text.slice(i+query.length);
}

// ---- 模型 Combobox ----
let currentModelId = '';
function renderModelItem(m, q) { return highlightMatch(m.name, q) + (m.id!==m.name?` <span class="combo-hint">${m.id}</span>`:''); }
function initModelCombo(models, cur) {
  currentModelId = cur;
  initCombo('modelComboWrapper','modelComboInput','modelComboDropdown',
    models.map(m=>({label:m.name+' '+m.id, id:m.id, name:m.name, provider:m.provider})),
    item => { $('modelComboInput').value = item.name; currentModelId = item.id; switchModelById(item.id, item.provider); },
    renderModelItem
  );
  const c = models.find(m=>m.id===cur);
  if(c) $('modelComboInput').value = c.name;
}
async function switchModelById(modelId, provider) {
  try { await api('/api/switch_model', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({model:modelId,provider})}); }
  catch(e) { console.error(e); }
}

// ---- Agent Combobox ----
let selectedAgent = '';
function renderAgentItem(a, q) {
  const skills = a.skills&&a.skills.length ? `<span class="combo-hint">${a.skills.join(',')}</span>` : '';
  return highlightMatch(a.name, q) + ' ' + skills;
}
function initAgentCombo() {
  initCombo('agentComboWrapper','agentComboInput','agentComboDropdown',
    [{label:'自动分配',name:'',description:'由系统智能分配'}],
    item => { $('agentComboInput').value = item.name||'自动分配'; selectedAgent = item.name||''; },
    (item,q) => item.name==='' ? '<b>自动分配</b> <span class="combo-hint">系统智能选择</span>' : renderAgentItem(item,q)
  );
}
async function refreshAgentCombo() {
  try {
    const data = await api('/api/agents/list');
    const items = [{label:'自动分配',name:'',description:'由系统智能分配'}];
    (data.agents||[]).forEach(a => items.push({
      label:a.name, name:a.name, skills:a.skills||[], description:a.description||'', status:a.status
    }));
    initCombo('agentComboWrapper','agentComboInput','agentComboDropdown', items,
      item => { $('agentComboInput').value=item.name||'自动分配'; selectedAgent=item.name||''; },
      (item,q) => item.name==='' ? '<b>自动分配</b> <span class="combo-hint">系统智能选择</span>' : renderAgentItem(item,q)
    );
  } catch(e) {}
}

// ==================== 斜杠命令 ====================
const chatEl = $('chat'), inputEl = $('input'), sendBtn = $('sendBtn'), slashDropdown = $('slashDropdown');
let isStreaming = false, currentAgentBubble = null, currentToolCards = {};
let commandsCache = [], slashActiveIdx = -1;

async function loadCommands() {
  try { const d = await api('/api/commands'); commandsCache = d.commands||[]; } catch(e) { commandsCache=[]; }
}

function showSlashDropdown() {
  const text = inputEl.value;
  if(!text.startsWith('/')) { slashDropdown.classList.remove('show'); return; }
  const prefix = text.split(' ')[0].toLowerCase();
  const filtered = commandsCache.filter(c => c.cmd.toLowerCase().startsWith(prefix));
  if(filtered.length===0) { slashDropdown.classList.remove('show'); return; }
  slashDropdown.innerHTML = filtered.map((c,i) =>
    `<div class="slash-item" data-idx="${i}"><span class="slash-cmd">${c.cmd}</span><span class="slash-desc">${c.desc}</span>${c.args?`<span class="slash-args">${c.args}</span>`:''}</div>`
  ).join('');
  slashDropdown.classList.add('show'); slashActiveIdx = -1;
  slashDropdown.querySelectorAll('.slash-item').forEach(el => {
    el.addEventListener('mousedown', e => {
      e.preventDefault();
      const cmd = filtered[parseInt(el.dataset.idx)];
      inputEl.value = cmd.cmd + (cmd.args?' ':'');
      slashDropdown.classList.remove('show'); inputEl.focus(); autoResize(inputEl);
    });
  });
}
function navSlash(key) {
  const items = slashDropdown.querySelectorAll('.slash-item');
  if(!items.length) return;
  slashActiveIdx = key==='ArrowDown' ? Math.min(slashActiveIdx+1,items.length-1) : Math.max(slashActiveIdx-1,-1);
  items.forEach((el,i) => el.classList.toggle('active',i===slashActiveIdx));
  if(slashActiveIdx>=0&&items[slashActiveIdx]) items[slashActiveIdx].scrollIntoView({block:'nearest'});
}
function selectSlash() {
  const items = slashDropdown.querySelectorAll('.slash-item');
  if(slashActiveIdx>=0&&items[slashActiveIdx]) { items[slashActiveIdx].click(); return true; }
  return false;
}

// ==================== 聊天 ====================
async function toggleMode(mode) {
  try {
    const data = await api('/api/toggle_mode', {method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({mode})});
    if(mode==='planning') $('togglePlan').checked = data.enabled;
    if(mode==='rag') $('toggleRag').checked = data.enabled;
    if(mode==='reflection') $('toggleReflect').checked = data.enabled;
  } catch(e) {}
}

function autoResize(el) { el.style.height='auto'; el.style.height=Math.min(el.scrollHeight,150)+'px'; }

inputEl.addEventListener('keydown', e => {
  if(slashDropdown.classList.contains('show')) {
    if(e.key==='ArrowDown'||e.key==='ArrowUp') { e.preventDefault(); navSlash(e.key); return; }
    if(e.key==='Enter') { if(selectSlash()) { e.preventDefault(); return; } slashDropdown.classList.remove('show'); }
    if(e.key==='Escape') { slashDropdown.classList.remove('show'); return; }
  }
  if(e.key==='Enter'&&!e.shiftKey) { e.preventDefault(); sendMessage(); }
});
inputEl.addEventListener('input', () => {
  if(inputEl.value.startsWith('/')) showSlashDropdown(); else slashDropdown.classList.remove('show');
});

function scrollBottom() { chatEl.scrollTop = chatEl.scrollHeight; }

function addBubble(role, content, id) {
  const div = document.createElement('div');
  div.className = 'message ' + role;
  div.id = id || '';
  div.innerHTML = `<div class="avatar">${role==='user'?'U':'AI'}</div><div class="bubble">${content}</div>`;
  chatEl.appendChild(div); scrollBottom(); return div;
}
function updateBubble(el, html) { el.querySelector('.bubble').innerHTML = html; scrollBottom(); }

function renderMarkdown(text) {
  var html = text
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
    .replace(/```(\w*)\n([\s\S]*?)```/g,'<pre><code>$2</code></pre>')
    .replace(/`([^`]+)`/g,'<code>$1</code>')
    .replace(/\*\*(.+?)\*\*/g,'<strong>$1</strong>')
    .replace(/^### (.+)$/gm,'<h3>$1</h3>').replace(/^## (.+)$/gm,'<h2>$1</h2>').replace(/^# (.+)$/gm,'<h1>$1</h1>')
    .replace(/^---$/gm,'<hr>').replace(/^&gt; (.+)$/gm,'<blockquote>$1</blockquote>');
  html = html.replace(/((?:\|.+\|\n)+)/g, function(match) {
    var lines = match.trim().split('\n');
    if(lines.length<2) return match;
    if(!lines.every(function(l){return /^\|.+\\|$/.test(l);})) return match;
    var t='<table>';
    lines.forEach(function(line,i){
      var cells=line.split('|').filter(function(c){return c.trim()!=='';});
      if(i===1&&/^[\s\-:]+$/.test(cells.join(''))) return;
      var tag=i===0?'th':'td';
      t+='<tr>'+cells.map(function(c){return '<'+tag+'>'+c.trim()+'</'+tag+'>';}).join('')+'</tr>';
    });
    return t+'</table>';
  });
  html = html.replace(/^- (.+)$/gm,'<li>$1</li>').replace(/((?:<li>.*<\/li>\n?)+)/g,'<ul>$1</ul>');
  html = html.replace(/\n\n/g,'<br><br>').replace(/\n/g,'<br>');
  return html;
}

function addToolCard(callId, name, args) {
  const card = document.createElement('div');
  card.className = 'tool-call-card'; card.id = 'tool-'+callId;
  card.innerHTML = `<div class="tool-name">调用工具: ${name}</div><div class="tool-args">参数: ${args}</div><div class="result-preview" style="display:none"></div>`;
  if(currentAgentBubble) currentAgentBubble.querySelector('.bubble').appendChild(card);
  currentToolCards[callId] = card; scrollBottom();
}
function updateToolResult(callId, success, result) {
  const card = currentToolCards[callId]; if(!card) return;
  card.classList.add(success?'success':'');
  const p = card.querySelector('.result-preview'); p.style.display='block';
  p.textContent = (success?'OK: ':'FAIL: ') + result; scrollBottom();
}

async function sendMessage() {
  const text = inputEl.value.trim();
  if(!text||isStreaming) return;
  addBubble('user', text); inputEl.value=''; inputEl.style.height='auto'; slashDropdown.classList.remove('show');
  isStreaming=true; sendBtn.disabled=true;
  currentAgentBubble = addBubble('agent','<div class="typing-indicator"><span></span><span></span><span></span></div>','agent-msg');
  currentToolCards = {}; let fullText='';
  try {
    const resp = await fetch('/api/chat',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:text})});
    const reader = resp.body.getReader(); const decoder = new TextDecoder();
    while(true) {
      const {done,value} = await reader.read(); if(done) break;
      for(const line of decoder.decode(value,{stream:true}).split('\n')) {
        if(!line.startsWith('data: ')) continue;
        const data = JSON.parse(line.slice(6));
        if(data.type==='text') { fullText+=data.content; updateBubble(currentAgentBubble,renderMarkdown(fullText)); }
        else if(data.type==='tool_call') addToolCard(data.call_id,data.name,JSON.stringify(data.arguments,null,2));
        else if(data.type==='tool_result') updateToolResult(data.call_id,data.success,String(data.result||data.error||'').slice(0,300));
        else if(data.type==='done') $('stat-turns').textContent = parseInt($('stat-turns').textContent)+1;
      }
    }
  } catch(err) { updateBubble(currentAgentBubble,'<span style="color:#e74c3c">请求失败: '+err.message+'</span>'); }
  isStreaming=false; sendBtn.disabled=false; inputEl.focus();
}

// ==================== 任务管理 ====================
let execMode = 'single';          // 'single' | 'orchestrated'
let orchAbortController = null;
let orchIsExecuting = false;

function setExecMode(mode, btn) {
  execMode = mode;
  document.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
  btn.classList.add('active');
  const orchSelect = $('orchModeSelect');
  orchSelect.style.display = mode === 'orchestrated' ? '' : 'none';
  // 更新按钮文字
  const pubBtn = $('publishBtn');
  pubBtn.textContent = mode === 'orchestrated' ? '编排执行' : '发布任务';
  pubBtn.style.background = mode === 'orchestrated' ? 'var(--purple)' : '';
}

function onOrchModeChange() {}  // placeholder, hint 已集成到模式徽章

async function publishTask() {
  const input = $('taskInput'), desc = input.value.trim();
  if (!desc) return;
  if (orchIsExecuting) return;

  if (execMode === 'orchestrated') {
    await executeOrchestrated(desc);
  } else {
    try {
      await api('/api/tasks/publish', {
        method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({description:desc,target_agent:selectedAgent})
      });
      input.value=''; refreshTasks('');
    } catch(e) { console.error(e); }
  }
}

async function filterTasks(status, btn) {
  // 高亮当前按钮
  document.querySelectorAll('.task-filter').forEach(b => b.classList.remove('active'));
  if (btn) btn.classList.add('active');
  currentTaskFilter = status;
  refreshTasks(status);
}

async function refreshTasks(status) {
  try {
    const url = '/api/tasks/list'+(status?'?status='+status:'');
    const data = await api(url);
    const list = $('taskList');
    if(!data.tasks||data.tasks.length===0) { list.innerHTML='<div style="color:var(--muted);padding:20px;">暂无任务</div>'; return; }
    const statusMap = {pending:'待处理',running:'执行中',completed:'已完成',failed:'失败',cancelled:'已取消'};
    list.innerHTML = data.tasks.map(t => {
      // 编排任务标记
      const meta = t.metadata_ || {};
      const isOrch = meta.orchestration_mode;
      const orchBadge = isOrch
        ? `<span class="orch-mode-badge ${isOrch}" style="font-size:10px;padding:1px 6px;margin-left:6px;">${isOrch.toUpperCase()}</span>`
        : '';

      // 编排子Agent状态（显示在卡片内）
      let orchSubStatus = '';
      if (isOrch && meta.orchestration_agents) {
        const agentStatuses = meta.orchestration_agent_statuses || {};
        orchSubStatus = `<div class="task-card-orch">
          <span style="font-size:10px;color:var(--muted);">参与:</span>
          ${meta.orchestration_agents.map(a => {
            const st = agentStatuses[a];
            const stIcon = st==='done'?'✅':st==='running'?'🔄':st==='failed'?'❌':'⏳';
            const stCls = st==='done'?'agent-done':st==='running'?'agent-running':st==='failed'?'agent-failed':'agent-pending';
            return `<span class="orch-agent-tag ${stCls}" title="${a}: ${st||'等待中'}">${stIcon} ${escHtml(a)}</span>`;
          }).join(' ')}
        </div>`;
      }

      return `
      <div class="task-card">
        <div class="task-info" style="cursor:pointer;" onclick="showTaskDetailModal('${t.id}')">
          <div class="task-title">${escHtml(t.title||t.description||'').slice(0,60)}${orchBadge}</div>
          <div class="task-meta">ID: ${t.id} | ${(t.created_at||'').slice(0,16)} | Agent: ${t.assigned_agent||'-'}${isOrch ? ' | 多Agent协作' : ''}</div>
          ${orchSubStatus}
        </div>
        <div style="display:flex;align-items:center;gap:8px;">
          <span class="status-badge ${t.status}">${statusMap[t.status]||t.status}</span>
          <button class="btn btn-outline btn-sm" onclick="showTaskDetailModal('${t.id}')">详情</button>
          ${t.status==='pending'?`<button class="btn btn-outline btn-sm" onclick="showEditTaskModal('${t.id}')">编辑</button>`:''}
          ${t.status==='pending'||t.status==='running'?`<button class="btn btn-danger btn-sm" onclick="cancelTask('${t.id}')">取消</button>`:''}
        </div>
      </div>`;
    }).join('');
  } catch(e) { console.error(e); }
}

async function showEditTaskModal(taskId) {
  try {
    const data = await api('/api/tasks/'+taskId);
    if(!data.ok) return alert('加载任务失败');
    const t = data.task;
    openModal('编辑任务: '+t.id,
      `<div class="form-group"><label>标题</label><input class="form-input" id="editTaskTitle" value="${escHtml(t.title||'')}"></div>
       <div class="form-group"><label>描述</label><textarea class="form-textarea" id="editTaskDesc" rows="4">${escHtml(t.description||'')}</textarea></div>
       <div class="form-group"><label>优先级 (0-10)</label><input class="form-input" id="editTaskPriority" type="number" min="0" max="10" value="${t.priority||0}"></div>
       <div class="form-group"><label>标签 (逗号分隔)</label><input class="form-input" id="editTaskTags" value="${escHtml((t.tags||[]).join(','))}"></div>
       <div class="form-group"><label>目标 Agent</label><input class="form-input" id="editTaskAgent" value="${escHtml(t.assigned_agent||'')}" placeholder="留空自动分配"></div>`,
      async () => {
        const tags = $('editTaskTags').value.split(',').map(s=>s.trim()).filter(Boolean);
        await api('/api/tasks/'+taskId+'/update', {
          method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({
            title: $('editTaskTitle').value,
            description: $('editTaskDesc').value,
            priority: parseInt($('editTaskPriority').value)||0,
            tags: tags,
            target_agent: $('editTaskAgent').value
          })
        });
        refreshTasks('');
      }, '保存修改'
    );
  } catch(e) { console.error(e); }
}

async function cancelTask(taskId) {
  if(!confirm('确认取消任务 '+taskId+'?')) return;
  try { await api('/api/tasks/'+taskId+'/cancel',{method:'POST'}); refreshTasks(''); } catch(e) { console.error(e); }
}

// ==================== 多 Agent 编排执行 ====================

async function executeOrchestrated(desc) {
  orchIsExecuting = true;
  const pubBtn = $('publishBtn');
  pubBtn.disabled = true;
  pubBtn.textContent = '执行中...';

  // 显示实时面板
  const livePanel = $('orchLivePanel');
  const liveContent = $('orchLiveContent');
  livePanel.style.display = 'block';
  liveContent.innerHTML = `
    <div class="orch-live-container">
      <div class="orch-stage-card stage-active">
        <div class="stage-header">
          <span class="stage-icon">🚀</span>
          <span class="stage-title">正在启动编排...</span>
        </div>
        <div class="stage-detail">分析任务特征，检测最优执行模式...</div>
      </div>
      <div id="orchFlowContainer"></div>
      <div id="orchResultContainer"></div>
    </div>`;

  const flowContainer = $('orchFlowContainer');
  const resultContainer = $('orchResultContainer');
  const events = [];
  let finalResult = null;
  let detectedMode = null;

  orchAbortController = new AbortController();

  try {
    const mode = $('orchModeSelect').value;
    const resp = await fetch('/api/tasks/orchestrate/stream', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ description: desc, title: desc.slice(0, 50), mode: mode, agent_names: [] }),
      signal: orchAbortController.signal
    });
    const reader = resp.body.getReader();
    const decoder = new TextDecoder();

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      for (const line of decoder.decode(value, { stream: true }).split('\n')) {
        if (!line.startsWith('data: ')) continue;
        try {
          const data = JSON.parse(line.slice(6));
          handleOrchEvent(data, flowContainer, resultContainer, events);
          if (data.stage === 'done') finalResult = data.result;
          if (data.stage === 'error') finalResult = { error: data.error };
        } catch (e) {}
      }
    }
  } catch (err) {
    if (err.name !== 'AbortError') {
      flowContainer.innerHTML += `<div class="orch-stage-card stage-error"><div class="stage-header"><span class="stage-icon">❌</span><span class="stage-title">连接失败</span></div><div class="stage-detail">${escHtml(err.message)}</div></div>`;
    }
  }

  orchIsExecuting = false;
  pubBtn.disabled = false;
  pubBtn.textContent = '编排执行';
  orchAbortController = null;
  $('taskInput').value = '';
  refreshTasks('');
}

function handleOrchEvent(data, flowContainer, resultContainer, events) {
  const stage = data.stage;
  const info = data.info || {};
  events.push(data);

  // 更新启动卡片
  const firstCard = flowContainer.parentElement.querySelector('.orch-stage-card:first-child');
  if (firstCard && stage !== 'start' && stage !== 'heartbeat') {
    firstCard.classList.remove('stage-active');
    firstCard.classList.add('stage-done');
    firstCard.querySelector('.stage-icon').textContent = '✅';
    firstCard.querySelector('.stage-title').textContent = '编排已启动';
    firstCard.querySelector('.stage-detail').textContent = `模式: ${data.mode || detectedMode || 'auto'} | 参与 Agent: ${(data.agents || []).join(', ') || '自动选择'}`;
  }

  // 模式检测
  if (stage === 'mode_detected') {
    detectedMode = data.mode;
    if (firstCard) {
      firstCard.querySelector('.stage-detail').innerHTML = `
        <span class="orch-mode-badge ${data.mode}">${data.mode.toUpperCase()}</span>
        <span style="margin-left:8px;font-size:12px;">${escHtml(data.reason || '')}</span>`;
    }
    return;
  }

  if (stage === 'start') {
    detectedMode = data.mode;
    return;
  }

  if (stage === 'heartbeat') return;

  // 各阶段进度渲染
  if (stage.startsWith('stage_')) {
    const evtName = stage.replace('stage_', '');
    appendOrchStageCard(evtName, info, flowContainer);
  }

  // 最终结果
  if (stage === 'done') {
    renderOrchResult(data.result, resultContainer);
  }

  if (stage === 'error') {
    flowContainer.innerHTML += `<div class="orch-stage-card stage-error">
      <div class="stage-header"><span class="stage-icon">❌</span><span class="stage-title">执行失败</span></div>
      <div class="stage-detail">${escHtml(data.error || '未知错误')}</div></div>`;
  }
}

function appendOrchStageCard(evtName, info, container) {
  const now = new Date().toLocaleTimeString('zh-CN');

  // 各事件卡片配置
  const templates = {
    single_start: { icon: '👤', title: '单 Agent 开始执行', detail: `Agent: <b>${info.agent || ''}</b>` },
    single_done: { icon: '✅', title: '单 Agent 执行完成', detail: `Agent: <b>${info.agent || ''}</b>`, cls: 'stage-done' },

    parallel_start: { icon: '⚡', title: `并行执行 — ${info.agent_count || 0} 个 Agent`, detail: `Agent: ${(info.agents || []).map(a => `<span class="orch-agent-tag active">${a}</span>`).join('')}`, cls: 'stage-active' },
    agent_start: { icon: '▶️', title: `${info.agent || ''} 开始工作`, detail: `第 ${info.index || 1}/${info.total || 1} 个 Agent` },
    agent_done: { icon: '✅', title: `${info.agent || ''} 完成`, detail: `第 ${info.index || 1}/${info.total || 1} 个 Agent`, cls: 'stage-done' },
    agent_error: { icon: '❌', title: `${info.agent || ''} 出错`, detail: escHtml((info.error || '')), cls: 'stage-error' },
    synthesizing: { icon: '🧩', title: '汇总综合中...', detail: '将多个 Agent 的结果综合为一个答案' },
    parallel_done: { icon: '🎉', title: `并行执行完成`, detail: `${info.agent_count || 0} 个 Agent 全部完成`, cls: 'stage-done' },

    pipeline_start: { icon: '🔗', title: `流水线启动 — ${info.stages || 0} 个阶段`, detail: `流程: ${(info.agents || []).join(' → ')}` },
    pipeline_stage: { icon: '▶️', title: `第 ${info.stage || 1} 阶段: ${info.agent || ''}`, detail: `角色: <b>${info.role || ''}</b>` },
    pipeline_stage_done: { icon: '✅', title: `第 ${info.stage || 1} 阶段完成`, detail: `Agent ${info.agent || ''} 已完成`, cls: 'stage-done' },
    pipeline_done: { icon: '🎉', title: '流水线执行完成', detail: `${info.stages || 0} 个阶段全部完成`, cls: 'stage-done' },

    collab_start: { icon: '🤝', title: `协作讨论启动 — ${info.members || 0} 人团队`, detail: `成员: ${(info.agents || []).map(a => `<span class="orch-agent-tag">${a}</span>`).join('')}` },
    collab_round1: { icon: '💬', title: '第 1 轮讨论 — 各自独立分析', detail: '每个 Agent 从自身角度分析问题' },
    collab_round2: { icon: '🔄', title: '第 2 轮讨论 — 交叉审阅', detail: 'Agent 审阅他人观点，修正完善自身结论' },
    collab_synthesizing: { icon: '🧩', title: '综合共识中...', detail: '主持人综合团队讨论结果' },
    collab_done: { icon: '🎉', title: '协作讨论完成', detail: '团队达成共识', cls: 'stage-done' },
  };

  const tpl = templates[evtName];
  if (!tpl) return;

  const card = document.createElement('div');
  card.className = `orch-stage-card ${tpl.cls || ''}`;
  card.innerHTML = `
    <div class="stage-header">
      <span class="stage-icon">${tpl.icon}</span>
      <span class="stage-title">${tpl.title}</span>
      <span class="stage-time">${now}</span>
    </div>
    <div class="stage-detail">${tpl.detail}</div>`;
  container.appendChild(card);
}

function renderOrchResult(result, container) {
  if (!result || result.error) {
    container.innerHTML = `<div class="orch-stage-card stage-error"><div class="stage-header"><span class="stage-icon">❌</span><span class="stage-title">执行失败</span></div><div class="stage-detail">${escHtml((result && result.error) || '未知错误')}</div></div>`;
    return;
  }

  let agentsHtml = '';
  if (result.agents_used && result.agents_used.length > 0) {
    agentsHtml = `<div style="margin-bottom:8px;">参与 Agent: ${result.agents_used.map(a => `<span class="orch-agent-tag done">${escHtml(a)}</span>`).join('')}</div>`;
  }

  // 各 Agent 子结果
  let subResults = '';
  if (result.agent_results && result.agent_results.length > 0) {
    subResults = result.agent_results.map(r => `
      <div class="result-item">
        <div class="result-item-title">${escHtml(r.agent || '')} ${r.role ? '(' + escHtml(r.role) + ')' : ''} ${r.round ? '第' + r.round + '轮' : ''}</div>
        <div class="result-item-text">${escHtml((r.result || r.summary || '').slice(0, 500))}</div>
      </div>
    `).join('');
  }

  let filesHtml = '';
  if (result.output_files && result.output_files.length > 0) {
    filesHtml = `<div class="orch-output-files">📁 输出文件: ${result.output_files.map(f => {
      const fname = f.split('/').pop() || f.split('\\').pop() || f;
      return `<a href="/api/files/download?file=${encodeURIComponent(f)}">${escHtml(fname)}</a>`;
    }).join(' ')}</div>`;
  }

  const summaryText = escHtml((result.final_result || '').slice(0, 3000));

  container.innerHTML = `
    <div class="orch-result">
      <div class="result-header">
        <span class="orch-mode-badge ${result.mode || 'auto'}">${(result.mode || '').toUpperCase()}</span>
        <span style="margin-left:8px;font-size:12px;color:var(--muted);">${escHtml(result.mode_reason || '')}</span>
        <span style="margin-left:8px;font-size:12px;color:var(--muted);">耗时: ${calcDuration(result.started_at, result.finished_at)}</span>
      </div>
      ${agentsHtml}
      ${result.agent_results && result.agent_results.length > 0 ? `
        <div class="result-header" style="margin-top:8px;font-size:12px;">📋 各 Agent 执行结果</div>
        ${subResults}
        <div class="orch-summary-box">
          <div class="summary-title">📝 最终综合结果</div>
          <div class="summary-text">${summaryText || '（无汇总文本）'}</div>
        </div>
      ` : `
        <div class="orch-summary-box">
          <div class="summary-title">📝 执行结果</div>
          <div class="summary-text">${summaryText || '（无结果）'}</div>
        </div>
      `}
      ${filesHtml}
    </div>`;
}

function calcDuration(startStr, endStr) {
  if (!startStr || !endStr) return '-';
  try {
    const ms = new Date(endStr) - new Date(startStr);
    if (ms < 1000) return ms + 'ms';
    if (ms < 60000) return (ms / 1000).toFixed(1) + 's';
    return (ms / 60000).toFixed(1) + 'min';
  } catch (e) { return '-'; }
}

// ==================== 任务详情弹窗 ====================
async function showTaskDetailModal(taskId) {
  try {
    const data = await api('/api/tasks/'+taskId);
    if(!data.ok) return alert('加载任务失败');
    const t = data.task;

    // 状态中文映射
    const statusMap = {pending:'待处理',running:'执行中',completed:'已完成',failed:'失败',cancelled:'已取消'};
    const statusClass = t.status;

    // 编排信息
    const meta = t.metadata_ || {};
    const isOrch = meta.orchestration_mode;

    // 编排子Agent状态详情
    let orchAgentStatusHtml = '';
    if (isOrch && meta.orchestration_agents && meta.orchestration_agents.length > 0) {
      const agentStatuses = meta.orchestration_agent_statuses || {};
      const statusLabel = {done:'已完成',running:'执行中',failed:'失败',pending:'等待中'};
      orchAgentStatusHtml = `
        <div class="detail-section">
          <div class="detail-label">子Agent 执行状态</div>
          <div class="detail-value">
            <div class="agent-status-grid">
              ${meta.orchestration_agents.map(a => {
                const st = agentStatuses[a] || 'pending';
                const stIcon = st==='done'?'✅':st==='running'?'🔄':st==='failed'?'❌':'⏳';
                const stCls = st==='done'?'agent-done':st==='running'?'agent-running':st==='failed'?'agent-failed':'agent-pending';
                return `<div class="agent-status-row">
                  <span class="orch-agent-tag ${stCls}">${stIcon}</span>
                  <span class="agent-status-name">${escHtml(a)}</span>
                  <span class="agent-status-state ${stCls}">${statusLabel[st]||st}</span>
                </div>`;
              }).join('')}
            </div>
          </div>
        </div>`;
    }

    const orchInfo = isOrch ? `
        <div class="detail-section">
          <div class="detail-label">编排模式</div>
          <div class="detail-value">
            <span class="orch-mode-badge ${isOrch}">${isOrch.toUpperCase()}</span>
            ${meta.orchestration_reason ? `<span style="margin-left:8px;font-size:12px;color:var(--muted);">${escHtml(meta.orchestration_reason)}</span>` : ''}
          </div>
        </div>
        ${orchAgentStatusHtml}` : '';

    // 事件日志渲染
    const eventsHtml = (t.event_log||[]).length>0
      ? t.event_log.map(e => {
          const iconMap = {'THINK_START':'🤔','THINK_END':'💡','TOOL_CALL':'🔧','TOOL_RESULT':'📋',
                           'PLAN_CREATED':'📝','assigned':'🎯','completed':'✅','ERROR':'❌','error':'❌'};
          const icon = iconMap[e.event] || '📌';
          let dataStr = '';
          if (e.data) {
            if (typeof e.data === 'string') dataStr = escHtml(e.data).slice(0,300);
            else if (e.data && e.data.name) dataStr = '工具: <b>'+escHtml(e.data.name)+'</b>';
          }
          return `<div class="event-item">
            <span class="event-icon">${icon}</span>
            <span class="event-time">${(e.time||'').slice(11,19)}</span>
            <span class="event-name">${escHtml(e.event)}</span>
            ${dataStr?`<div class="event-data">${dataStr}</div>`:''}
          </div>`;
        }).join('')
      : '<div style="color:var(--muted);padding:8px 0;">暂无执行日志</div>';

    const bodyHtml = `
      <div class="task-detail">
        <div class="detail-section">
          <div class="detail-label">任务 ID</div>
          <div class="detail-value mono">${escHtml(t.id)}</div>
        </div>
        <div class="detail-section">
          <div class="detail-label">状态</div>
          <div><span class="status-badge ${statusClass}">${statusMap[t.status]||t.status}</span></div>
        </div>
        <div class="detail-section">
          <div class="detail-label">标题</div>
          <div class="detail-value">${escHtml(t.title||'-')}</div>
        </div>
        <div class="detail-section">
          <div class="detail-label">描述</div>
          <div class="detail-value" style="white-space:pre-wrap;">${escHtml(t.description||'-')}</div>
        </div>
        <div class="detail-section">
          <div class="detail-label">分配 Agent</div>
          <div class="detail-value">${escHtml(t.assigned_agent||'自动分配')}</div>
        </div>
        <div class="detail-section">
          <div class="detail-label">优先级 / 标签</div>
          <div class="detail-value">${t.priority||0} &nbsp; ${(t.tags||[]).map(tg=>`<span class="skill-tag">${escHtml(tg)}</span>`).join('')}</div>
        </div>
        ${orchInfo}
        <div class="detail-section">
          <div class="detail-label">时间</div>
          <div class="detail-value">创建: ${formatTime(t.created_at)}<br>开始: ${formatTime(t.started_at)}<br>完成: ${formatTime(t.finished_at)}</div>
        </div>
        ${t.result ? `
        <div class="detail-section">
          <div class="detail-label">执行结果</div>
          <div class="detail-value result-box">${escHtml(t.result)}</div>
        </div>` : ''}
        ${t.error ? `
        <div class="detail-section">
          <div class="detail-label">错误信息</div>
          <div class="detail-value error-box">${escHtml(t.error)}</div>
        </div>` : ''}
        ${(t.output_files && t.output_files.length > 0) ? `
        <div class="detail-section">
          <div class="detail-label">📁 输出文件 (${t.output_files.length})</div>
          <div class="output-files-list">
            ${t.output_files.map(f => {
              const fname = f.split('/').pop() || f.split('\\').pop() || f;
              const ext = (fname.split('.').pop()||'').toLowerCase();
              const icon = {'py':'🐍','js':'🟨','ts':'🔷','html':'🌐','css':'🎨','json':'📋',
                           'md':'📝','txt':'📄','csv':'📊','yaml':'⚙️','yml':'⚙️',
                           'toml':'⚙️','png':'🖼️','jpg':'🖼️','svg':'🖼️','pdf':'📕'}[ext] || '📎';
              return `<div class="output-file-row">
                <span class="file-icon">${icon}</span>
                <span class="file-name" title="${escHtml(f)}">${escHtml(fname)}</span>
                <span class="file-path-muted" title="${escHtml(f)}">${escHtml(f)}</span>
                <button class="btn btn-sm btn-outline" onclick="event.stopPropagation();downloadFile('${escAttr(f)}')">⬇ 下载</button>
                <button class="btn btn-sm btn-outline" onclick="event.stopPropagation();previewFile('${escAttr(f)}')">👁 预览</button>
              </div>`;
            }).join('')}
          </div>
        </div>` : ''}
        <div class="detail-section">
          <div class="detail-label">执行日志 (最近20条)</div>
          <div class="event-log">${eventsHtml}</div>
        </div>
      </div>`;

    openModal('任务详情: '+escHtml(t.id), bodyHtml, null, null);
    // 隐藏保存按钮 + 加宽模态框
    const saveBtn = $('modalSaveBtn');
    if(saveBtn) saveBtn.style.display = 'none';
    $('modalContent').classList.add('wide');
    // 调整模态框宽度
    const modal = document.querySelector('#modalContent .modal') || document.getElementById('modalContent');
  } catch(e) { console.error(e); }
}

// ==================== Agent 管理 ====================
async function loadAgents() {
  try {
    const data = await api('/api/agents/list');
    const agents = data.agents||[];
    if(agents.length===0) {
      $('agentTable').innerHTML = '<div style="color:var(--muted);padding:20px;">暂无 Agent，点击上方按钮创建</div>';
      return;
    }
    const models = await api('/api/models');
    $('agentTable').innerHTML = `
      <table class="data-table">
        <tr><th>名称</th><th>状态</th><th>技能</th><th>描述</th><th>操作</th></tr>
        ${agents.map(a => `<tr>
          <td><b>${escHtml(a.name)}</b></td>
          <td><span class="status-badge ${a.status}">${a.status}</span></td>
          <td>${(a.skills||[]).map(s=>`<span class="skill-tag">${escHtml(s)}</span>`).join('')||'<span style="color:var(--muted);">通用</span>'}</td>
          <td style="font-size:12px;color:var(--muted);">${escHtml(a.description||'-')}</td>
          <td class="table-actions">
            <button class="btn btn-outline btn-sm" onclick="showEditAgentModal('${escHtml(a.name)}')">编辑</button>
            <button class="btn btn-danger btn-sm" onclick="deleteAgent('${escHtml(a.name)}')">删除</button>
          </td>
        </tr>`).join('')}
      </table>`;
  } catch(e) { console.error(e); }
}

function showCreateAgentModal() {
  openModal('创建 Agent',
    `<div class="form-group"><label>名称</label><input class="form-input" id="caName" placeholder="例如: 代码助手"></div>
     <div class="form-group"><label>模型</label><select class="form-select" id="caModel"></select></div>
     <div class="form-group"><label>技能 (逗号分隔)</label><input class="form-input" id="caSkills" placeholder="例如: coding, python, debug"></div>
     <div class="form-group"><label>描述</label><input class="form-input" id="caDesc" placeholder="一句话描述"></div>`,
    async () => {
      const skills = $('caSkills').value.split(',').map(s=>s.trim()).filter(Boolean);
      const modelVal = $('caModel').value.split('|');
      await api('/api/agents/create',{method:'POST',headers:{'Content-Type':'application/json'},
        body:JSON.stringify({name:$('caName').value, model:modelVal[0], provider:modelVal[1], skills, description:$('caDesc').value})
      });
      loadAgents(); refreshAgentCombo();
    }, '创建'
  );
  // 填充模型列表
  api('/api/models').then(d => {
    $('caModel').innerHTML = (d.models||[]).map(m => `<option value="${m.id}|${m.provider}">${m.name} (${m.provider})</option>`).join('');
  });
}

function showEditAgentModal(name) {
  api('/api/agents/list').then(d => {
    const a = (d.agents||[]).find(x=>x.name===name);
    if(!a) return alert('Agent 未找到');
    openModal('编辑 Agent: '+name,
      `<div class="form-group"><label>技能 (逗号分隔)</label><input class="form-input" id="eaSkills" value="${escHtml((a.skills||[]).join(','))}"></div>
       <div class="form-group"><label>描述</label><input class="form-input" id="eaDesc" value="${escHtml(a.description||'')}"></div>`,
      async () => {
        const skills = $('eaSkills').value.split(',').map(s=>s.trim()).filter(Boolean);
        await api('/api/agents/'+encodeURIComponent(name)+'/update',{method:'POST',headers:{'Content-Type':'application/json'},
          body:JSON.stringify({skills,description:$('eaDesc').value})});
        loadAgents(); refreshAgentCombo();
      }, '保存'
    );
  });
}

async function deleteAgent(name) {
  if(!confirm('确定删除 Agent: '+name+'?')) return;
  try { await api('/api/agents/'+encodeURIComponent(name),{method:'DELETE'}); loadAgents(); refreshAgentCombo(); } catch(e) { console.error(e); }
}

// ==================== 输出文件浏览 ====================
async function loadOutputFiles() {
  try {
    const data = await api('/api/files/list');
    if(!data.ok) { $('outputFilesList').innerHTML = '<div style="color:var(--muted);">加载失败</div>'; return; }
    const files = data.files || [];
    if(files.length===0) {
      $('outputFilesList').innerHTML = '<div style="color:var(--muted);padding:24px 0;text-align:center;">暂无输出文件。<br>发布任务让 Agent 生成文件后会自动显示在这里。</div>';
      return;
    }
    let html = '<table class="data-table"><tr><th>文件名</th><th>路径</th><th>大小</th><th>操作</th></tr>';
    files.forEach(f => {
      const ext = (f.name.split('.').pop()||'').toLowerCase();
      const icon = {'py':'🐍','js':'🟨','html':'🌐','css':'🎨','json':'📋','md':'📝','txt':'📄','csv':'📊','yaml':'⚙️','yml':'⚙️','png':'🖼️','jpg':'🖼️'}[ext] || '📎';
      html += `<tr>
        <td><span style="margin-right:4px;">${icon}</span>${escHtml(f.name)}</td>
        <td style="font-size:11px;color:var(--muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${escHtml(f.path)}">${escHtml(f.path)}</td>
        <td>${escHtml(f.size_str)}</td>
        <td>
          <button class="btn btn-sm btn-outline" onclick="downloadFile('${escAttr(f.path)}')">⬇ 下载</button>
          <button class="btn btn-sm btn-outline" onclick="previewFile('${escAttr(f.path)}')">👁 预览</button>
        </td>
      </tr>`;
    });
    html += '</table>';
    $('outputFilesList').innerHTML = html;
  } catch(e) { console.error(e); }
}

// ==================== 配置编辑 ====================
let configData = {};

async function loadConfig() {
  try {
    const data = await api('/api/config/full');
    if(!data.ok) return;
    configData = data.config||{};
    renderConfigSections(configData);
  } catch(e) { console.error(e); }
}

function renderConfigSections(cfg) {
  const sections = [
    { key:'llm', title:'LLM 大模型', fields:[
      {k:'provider',l:'Provider',t:'text',h:'deepseek / openai / qwen / zhipu'},
      {k:'model',l:'Model',t:'text',h:'例如 deepseek-chat'},
      {k:'api_key',l:'API Key',t:'password',h:'留空从环境变量读取'},
      {k:'base_url',l:'Base URL',t:'text',h:'留空自动补全'},
      {k:'temperature',l:'Temperature',t:'number',h:'0-2，越高越随机'},
      {k:'max_tokens',l:'Max Tokens',t:'number',h:'最大输出长度'},
      {k:'timeout',l:'Timeout (秒)',t:'number',h:'请求超时'},
    ]},
    { key:'agent', title:'Agent 设置', fields:[
      {k:'name',l:'名称',t:'text'},
      {k:'max_iterations',l:'最大迭代',t:'number',h:'单次任务最大迭代次数'},
      {k:'verbose',l:'详细日志',t:'bool'},
    ]},
    { key:'memory', title:'记忆管理', fields:[
      {k:'short_term.max_turns',l:'短期记忆轮数',t:'number',h:'保留最近 N 轮对话'},
      {k:'short_term.summarize_threshold',l:'摘要阈值',t:'number',h:'超过 N 轮触发摘要'},
      {k:'long_term.enabled',l:'长期记忆',t:'bool'},
      {k:'long_term.db_path',l:'DB 路径',t:'text'},
    ]},
    { key:'rag', title:'RAG 知识库', fields:[
      {k:'enabled',l:'启用',t:'bool'},
      {k:'embedding_model',l:'嵌入模型',t:'text'},
      {k:'embedding_provider',l:'嵌入 Provider',t:'text'},
      {k:'chunk_size',l:'分块大小',t:'number'},
      {k:'chunk_overlap',l:'重叠大小',t:'number'},
      {k:'top_k',l:'检索 Top-K',t:'number'},
      {k:'persist_dir',l:'持久化目录',t:'text'},
    ]},
    { key:'tools', title:'工具设置', fields:[
      {k:'max_calls_per_turn',l:'每轮最大工具调用',t:'number'},
    ]},
  ];

  let html = '';
  sections.forEach(sec => {
    const secData = cfg[sec.key]||{};
    html += `<div style="background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px;margin-bottom:16px;">
      <h3 style="color:var(--text-bright);margin-bottom:12px;">${sec.title}</h3>`;
    sec.fields.forEach(f => {
      const keys = f.k.split('.');
      let val = secData;
      for(const k of keys) val = (val||{})[k];
      if(val===undefined||val===null) val='';
      const id = 'cfg_'+f.k.replace(/\./g,'_');
      if(f.t==='bool') {
        html += `<div class="form-group"><label style="display:flex;align-items:center;gap:8px;cursor:pointer;">
          <input type="checkbox" id="${id}" ${val?'checked':''}> ${f.l}
          ${f.h?`<span class="form-help">${f.h}</span>`:''}
        </label></div>`;
      } else {
        html += `<div class="form-group"><label>${f.l}</label>
          <input class="form-input" id="${id}" type="${f.t}" value="${escHtml(String(val))}">
          ${f.h?`<div class="form-help">${f.h}</div>`:''}
        </div>`;
      }
    });
    html += `<button class="btn btn-primary btn-sm" onclick="saveSection('${sec.key}')">保存 ${sec.title}</button></div>`;
  });
  $('configSections').innerHTML = html;
}

async function saveSection(section) {
  const fields = {
    llm: ['provider','model','api_key','base_url','temperature','max_tokens','timeout'],
    agent: ['name','max_iterations','verbose'],
    memory: ['short_term.max_turns','short_term.summarize_threshold','long_term.enabled','long_term.db_path'],
    rag: ['enabled','embedding_model','embedding_provider','chunk_size','chunk_overlap','top_k','persist_dir'],
    tools: ['max_calls_per_turn'],
  }[section]||[];

  const data = {};
  fields.forEach(f => {
    const id = 'cfg_'+f.replace(/\./g,'_');
    const el = $(id);
    if(!el) return;
    let val;
    if(el.type==='checkbox') val = el.checked;
    else if(el.type==='number') val = el.value==='' ? '' : Number(el.value);
    else val = el.value;

    // 处理嵌套键
    const keys = f.split('.');
    if(keys.length===1) { data[keys[0]] = val; }
    else {
      if(!data[keys[0]]) data[keys[0]] = {};
      data[keys[0]][keys[1]] = val;
    }
  });

  try {
    const resp = await api('/api/config/update',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({section,data})});
    if(resp.ok) alert(section+' 配置已保存。部分配置需重启服务生效。');
  } catch(e) { alert('保存失败: '+e.message); }
}

// ==================== 初始化 ====================
loadCommands();
initAgentCombo();
loadDashboard();

api('/api/config').then(c => {
  initModelCombo(c.models||[], c.model);
  $('togglePlan').checked = c.planning;
  $('toggleRag').checked = c.rag;
  $('toggleReflect').checked = c.reflection;
  $('stat-tools').textContent = c.tools;
  $('stat-lc').textContent = c.langchain?'启用':'兼容模式';
  addBubble('agent','你好！我是 SmartAgent。左侧「仪表盘」查看数据概览，「对话」进行交互，「任务管理」发布任务，「Agent管理」管理智能体，「配置」编辑系统参数。');
});

// ==================== 自动轮询刷新 ====================
let autoRefreshTimer = null, currentTaskFilter = '';
function startAutoRefresh() {
  if (autoRefreshTimer) return;
  autoRefreshTimer = setInterval(() => {
    const activeTab = document.querySelector('.tab-content.active');
    if (!activeTab) return;
    if (activeTab.id === 'tab-dashboard') loadDashboard();
    else if (activeTab.id === 'tab-tasks') refreshTasks(currentTaskFilter);
  }, 3000);
}
function stopAutoRefresh() {
  if (autoRefreshTimer) { clearInterval(autoRefreshTimer); autoRefreshTimer = null; }
}
startAutoRefresh();
document.addEventListener('visibilitychange', () => {
  document.hidden ? stopAutoRefresh() : startAutoRefresh();
});
</script>
</body>
</html>"""


# ============================================================
# FastAPI 应用
# ============================================================

app = FastAPI(title="SmartAgent", docs_url=None, redoc_url=None)

# --- CORS 中间件 ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 速率限制、监控中间件 (延迟加载，避免循环导入) ---
_rate_limit_middleware = None
_prometheus_middleware = None

_agent: Optional[Agent] = None
_db_initialized = False


def get_agent() -> Agent:
    global _agent
    if _agent is None:
        raise RuntimeError("Agent 尚未初始化")
    return _agent


# ============================================================
# 健康检查
# ============================================================

@app.get("/health")
async def health_check():
    """健康检查端点 —— 返回服务 + DB + LLM 状态"""
    health = {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "2.0.0",
        "checks": {
            "server": "ok",
            "database": "unknown",
            "llm": "unknown",
        },
    }

    # 检查数据库连接
    try:
        from src.infrastructure.database import is_db_available, _engine
        if is_db_available() and _engine:
            async with _engine.connect() as conn:
                await conn.execute(
                    __import__("sqlalchemy").text("SELECT 1")
                )
            health["checks"]["database"] = "ok"
        else:
            health["checks"]["database"] = "disabled"
    except Exception:
        health["checks"]["database"] = "error"

    # 检查 LLM
    try:
        agent = _agent
        if agent and agent.llm:
            health["checks"]["llm"] = "ok"
        else:
            health["checks"]["llm"] = "not_initialized"
    except Exception:
        health["checks"]["llm"] = "error"

    # 整体状态判定
    if "error" in health["checks"].values():
        health["status"] = "degraded"

    return health


# ============================================================
# 认证 API
# ============================================================

class LoginRequest(BaseModel):
    username: str
    password: str

    @field_validator("username")
    @classmethod
    def username_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v or len(v) < 2:
            raise ValueError("用户名至少 2 个字符")
        if len(v) > 64:
            raise ValueError("用户名最长 64 个字符")
        return v

    @field_validator("password")
    @classmethod
    def password_not_empty(cls, v: str) -> str:
        if not v or len(v) < 4:
            raise ValueError("密码至少 4 个字符")
        return v


class RegisterRequest(BaseModel):
    username: str
    password: str
    email: str = ""

    @field_validator("username")
    @classmethod
    def username_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v or len(v) < 2:
            raise ValueError("用户名至少 2 个字符")
        if len(v) > 64:
            raise ValueError("用户名最长 64 个字符")
        return v

    @field_validator("password")
    @classmethod
    def password_not_empty(cls, v: str) -> str:
        if not v or len(v) < 6:
            raise ValueError("密码至少 6 个字符")
        if len(v) > 128:
            raise ValueError("密码最长 128 个字符")
        return v


@app.post("/api/auth/login")
async def api_login(req: LoginRequest):
    """用户登录 —— 返回 JWT Token"""
    from src.auth import verify_password, create_access_token
    from src.infrastructure.models import UserModel
    from sqlalchemy import select
    from src.infrastructure.database import get_session

    try:
        async for session in get_session():
            result = await session.execute(
                select(UserModel).where(UserModel.username == req.username)
            )
            user = result.scalar_one_or_none()

            if user is None or not verify_password(req.password, user.password_hash):
                return JSONResponse(
                    {"ok": False, "error": "用户名或密码错误"},
                    status_code=401,
                )

            if not user.is_active:
                return JSONResponse(
                    {"ok": False, "error": "账户已被禁用"},
                    status_code=403,
                )

            # 更新最后登录时间
            user.last_login_at = datetime.utcnow()
            await session.commit()

            # 生成 Token
            access_token = create_access_token(
                data={"sub": user.username, "role": user.role}
            )

            return {
                "ok": True,
                "access_token": access_token,
                "token_type": "bearer",
                "user": user.to_dict(),
            }
    except ImportError:
        return JSONResponse(
            {"ok": False, "error": "认证系统未启用 (数据库不可用)"},
            status_code=503,
        )


@app.post("/api/auth/register")
async def api_register(req: RegisterRequest):
    """用户注册"""
    from src.auth import hash_password
    from src.infrastructure.models import UserModel
    from sqlalchemy import select
    from src.infrastructure.database import get_session

    try:
        async for session in get_session():
            # 检查用户是否已存在
            result = await session.execute(
                select(UserModel).where(UserModel.username == req.username)
            )
            if result.scalar_one_or_none() is not None:
                return JSONResponse(
                    {"ok": False, "error": "用户名已存在"},
                    status_code=409,
                )

            user = UserModel(
                username=req.username,
                password_hash=hash_password(req.password),
                email=req.email,
            )
            session.add(user)
            await session.commit()

            return {"ok": True, "message": "注册成功，请登录"}
    except ImportError:
        return JSONResponse(
            {"ok": False, "error": "注册功能不可用 (数据库未启用)"},
            status_code=503,
        )


@app.get("/api/auth/me")
async def api_me(request: Request):
    """获取当前用户信息（需要 Bearer Token）"""
    from src.auth import decode_access_token
    from sqlalchemy import select

    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return JSONResponse(
            {"ok": False, "error": "请在 Header 中提供 Bearer Token"},
            status_code=401,
        )

    token = auth_header[7:]
    payload = decode_access_token(token)
    if payload is None:
        return JSONResponse(
            {"ok": False, "error": "令牌无效或已过期"},
            status_code=401,
        )

    username = payload.get("sub", "")
    if not username:
        return JSONResponse({"ok": False, "error": "令牌内容无效"}, status_code=401)

    try:
        from src.infrastructure.database import get_session
        from src.infrastructure.models import UserModel

        async for session in get_session():
            result = await session.execute(
                select(UserModel).where(
                    UserModel.username == username,
                    UserModel.is_active == True,
                )
            )
            user = result.scalar_one_or_none()

            if user is None:
                return JSONResponse(
                    {"ok": False, "error": "用户不存在或已被禁用"},
                    status_code=401,
                )

            return {"ok": True, "user": user.to_dict()}
    except ImportError:
        return JSONResponse(
            {"ok": False, "error": "认证系统未启用"},
            status_code=503,
        )


# ============================================================
# 监控 API
# ============================================================

@app.get("/metrics")
async def api_metrics():
    """Prometheus 指标端点"""
    try:
        from src.middleware.metrics import get_metrics_text
        return Response(content=get_metrics_text(), media_type="text/plain; version=0.0.4")
    except ImportError:
        return Response(content="# metrics disabled\n", media_type="text/plain")


# ============================================================
# 系统 API
# ============================================================

@app.get("/api/system/info")
async def api_system_info():
    """系统信息（运行状态、数据库状态等）"""
    import platform
    import psutil

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


# ============================================================
# 页面路由
# ============================================================

@app.get("/", response_class=HTMLResponse)
async def index():
    return CHAT_PAGE


# ============================================================
# Agent 配置 API
# ============================================================

@app.get("/api/config")
async def api_config():
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


@app.get("/api/models")
async def api_models():
    agent = get_agent()
    return {
        "current": agent.llm.config.model if agent.llm else "N/A",
        "models": agent.available_models(),
    }


class SwitchModelRequest(BaseModel):
    model: str
    provider: str | None = None
    base_url: str | None = None


@app.post("/api/switch_model")
async def api_switch_model(req: SwitchModelRequest):
    agent = get_agent()
    agent.switch_model(model=req.model, provider=req.provider, base_url=req.base_url)
    return {
        "ok": True,
        "model": agent.llm.config.model if agent.llm else "N/A",
        "provider": agent.llm.config.provider if agent.llm else "N/A",
    }


class ToggleModeRequest(BaseModel):
    mode: str


@app.post("/api/toggle_mode")
async def api_toggle_mode(req: ToggleModeRequest):
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


# ============================================================
# 聊天 API
# ============================================================

class ChatRequest(BaseModel):
    message: str


@app.post("/api/chat")
async def api_chat(req: ChatRequest):
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


# ============================================================
# 任务管理 API
# ============================================================

class PublishTaskRequest(BaseModel):
    description: str
    title: str = ""
    priority: int = 0
    tags: list[str] = []
    target_agent: str = ""


@app.post("/api/tasks/publish")
async def api_publish_task(req: PublishTaskRequest):
    tm = get_task_manager()
    task_id = tm.publish(
        description=req.description,
        title=req.title,
        priority=req.priority,
        tags=req.tags,
        target_agent=req.target_agent,
    )
    return {"ok": True, "task_id": task_id}


# ============================================================
# 多 Agent 编排 API
# ============================================================

class OrchestrateTaskRequest(BaseModel):
    description: str
    title: str = ""
    mode: str = "auto"                    # single / parallel / pipeline / collaborative / auto
    agent_names: list[str] = []           # 指定参与 Agent，空=自动选择空闲


@app.post("/api/tasks/orchestrate")
async def api_orchestrate_task(req: OrchestrateTaskRequest):
    """编排执行任务 — 自动选择最佳策略并多 Agent 协作"""
    tm = get_task_manager()

    # 确保编排器已挂载
    if not hasattr(tm, 'execute_orchestrated'):
        patch_task_manager(tm)

    result = tm.execute_orchestrated(
        description=req.description,
        title=req.title,
        mode=req.mode,
        agent_names=req.agent_names or None,
    )
    return {"ok": result.success, "result": result.to_dict()}


# ============================================================
# 多 Agent 编排 SSE 流式端点（实时推送协作过程）
# ============================================================

@app.post("/api/tasks/orchestrate/stream")
async def api_orchestrate_task_stream(request: Request, req: OrchestrateTaskRequest):
    """
    编排执行任务 — SSE 实时流式推送多 Agent 协作全过程

    SSE 事件类型:
      - start         : 编排开始 (包含检测到的模式、参与 Agent)
      - mode_detected : 自动模式检测结果
      - stage_*       : 各阶段进度 (agent_start, agent_done, pipeline_stage 等)
      - agent_think   : Agent 思考/工具调用过程
      - done          : 编排完成 (含最终结果)
      - error         : 执行失败
      - heartbeat     : 心跳保活
    """
    tm = get_task_manager()

    if not hasattr(tm, 'execute_orchestrated'):
        patch_task_manager(tm)

    event_queue: asyncio.Queue = asyncio.Queue()
    loop = asyncio.get_event_loop()

    # ── 注入主事件循环引用，确保跨线程 DB 写入走 run_coroutine_threadsafe ──
    tm._main_loop = loop

    def on_progress(stage: str, info: dict):
        """同步回调 → 异步队列 (线程安全桥接)"""
        try:
            safe_info = {}
            for k, v in info.items():
                if isinstance(v, (str, int, float, bool, list, dict, type(None))):
                    safe_info[k] = (str(v)[:500] if isinstance(v, str) and len(str(v)) > 500 else v)
                else:
                    safe_info[k] = str(v)[:500]
            asyncio.run_coroutine_threadsafe(
                event_queue.put({"stage": "stage_" + stage, "info": safe_info}), loop
            )
        except Exception:
            pass

    # 先做模式检测（不需要线程，直接同步检测）
    if req.mode == "auto" and hasattr(tm, 'detect_best_mode'):
        detection = tm.detect_best_mode(req.description)
        detected_mode = detection.get("mode", "single")
        detected_reason = detection.get("reason", "")
    else:
        detected_mode = req.mode
        detected_reason = "手动指定"

    # 解析参与 Agent 列表
    agents_list = tm.list_agents()
    available_agents = [a["name"] for a in agents_list]
    if req.agent_names:
        selected_agents = [n for n in req.agent_names if n in available_agents]
    else:
        selected_agents = [a["name"] for a in agents_list if a.get("status") == "idle"]
        if not selected_agents:
            selected_agents = available_agents

    def run_orchestrator():
        """在后台线程执行编排"""
        try:
            result = tm.execute_orchestrated(
                description=req.description,
                title=req.title,
                mode=detected_mode,
                agent_names=req.agent_names or None,
                on_progress=on_progress,
            )
            asyncio.run_coroutine_threadsafe(
                event_queue.put({"stage": "done", "result": result.to_dict()}), loop
            )
        except Exception as e:
            import traceback
            asyncio.run_coroutine_threadsafe(
                event_queue.put({
                    "stage": "error",
                    "error": str(e),
                    "traceback": traceback.format_exc()[:500],
                }), loop
            )

    # 启动后台执行
    loop.run_in_executor(_orch_executor, run_orchestrator)

    async def generate():
        # 初始事件：模式检测结果 + Agent 列表
        yield f"data: {json.dumps({'stage': 'start', 'mode': detected_mode, 'mode_reason': detected_reason, 'agents': selected_agents, 'available_agents': available_agents, 'description': req.description[:200]}, ensure_ascii=False)}\n\n"
        yield f"data: {json.dumps({'stage': 'mode_detected', 'mode': detected_mode, 'reason': detected_reason}, ensure_ascii=False)}\n\n"

        while True:
            try:
                event = await asyncio.wait_for(event_queue.get(), timeout=2.0)
                yield f"data: {json.dumps(event, ensure_ascii=False)}\n\n"
                if event.get("stage") in ("done", "error"):
                    break
            except asyncio.TimeoutError:
                if await request.is_disconnected():
                    break
                yield f"data: {json.dumps({'stage': 'heartbeat'}, ensure_ascii=False)}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.post("/api/tasks/detect-mode")
async def api_detect_mode(req: OrchestrateTaskRequest):
    """检测最适合任务的执行模式（不实际执行）"""
    tm = get_task_manager()
    if not hasattr(tm, 'detect_best_mode'):
        patch_task_manager(tm)

    detection = tm.detect_best_mode(req.description)
    available_modes = [
        {"value": m.value, "label": m.name, "desc": _mode_description(m)}
        for m in ExecutionMode if m != ExecutionMode.AUTO
    ]
    return {
        "ok": True,
        "recommended": detection,
        "available_modes": available_modes,
    }


def _mode_description(mode: ExecutionMode) -> str:
    return {
        ExecutionMode.SINGLE: "单个 Agent 执行，适合简单问答",
        ExecutionMode.PARALLEL: "多个 Agent 同时执行，结果汇总 — 适合多角度分析、对比",
        ExecutionMode.PIPELINE: "Agent 串行接力，前一个输出给后一个 — 适合多步骤流程",
        ExecutionMode.COLLABORATIVE: "Agent 团队讨论互审 — 适合决策、评估、头脑风暴",
    }.get(mode, "")


@app.get("/api/tasks/orchestrate/modes")
async def api_list_modes():
    """列出所有可用编排模式"""
    return {
        "ok": True,
        "modes": [
            {
                "value": m.value,
                "label": m.name,
                "desc": _mode_description(m),
            }
            for m in ExecutionMode
        ],
    }


# ============================================================
# 文件输出 API — 任务生成的文件可见且可下载
# ============================================================

import os as _os
import mimetypes as _mimetypes
from pathlib import Path as _Path


def _get_output_dir() -> str:
    """获取配置的输出目录绝对路径"""
    from src.core.config import get_config, load_config
    try:
        cfg = get_config()
    except RuntimeError:
        cfg = load_config()
    return _os.path.abspath(cfg.tools.output_dir)


def _safe_file_path(filepath: str) -> str | None:
    """
    安全检查：确保文件路径在 output_dir 内。
    返回绝对路径，不安全则返回 None。
    """
    output_dir = _get_output_dir()
    abs_path = _os.path.abspath(
        filepath if _os.path.isabs(filepath)
        else _os.path.join(output_dir, filepath)
    )
    # 规范化路径防止目录穿越
    real_path = _os.path.realpath(abs_path)
    real_output = _os.path.realpath(output_dir)
    if not real_path.startswith(real_output + _os.sep) and real_path != real_output:
        return None
    return real_path


def _list_output_files(task_id: str = "") -> list[dict]:
    """扫描输出目录，返回文件列表。可过滤特定任务文件。"""
    output_dir = _get_output_dir()
    files = []
    if not _os.path.isdir(output_dir):
        return files

    known_files: set[str] = set()
    if task_id:
        tm = get_task_manager()
        task = tm.get_task(task_id)
        if task:
            known_files = set(task.output_files)

    for root, dirs, filenames in _os.walk(output_dir):
        for fname in filenames:
            abs_path = _os.path.join(root, fname)
            rel_path = _os.path.relpath(abs_path, output_dir).replace("\\", "/")

            # 如果指定了 task_id，只保留该任务的输出文件
            if task_id and abs_path not in known_files:
                continue

            try:
                size = _os.path.getsize(abs_path)
            except OSError:
                size = 0

            files.append({
                "name": fname,
                "path": rel_path,
                "size": size,
                "size_str": _format_file_size(size),
            })

    # 按文件名排序
    files.sort(key=lambda f: f["name"])
    return files


def _format_file_size(size: int) -> str:
    """格式化文件大小"""
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


@app.get("/api/files/list")
async def api_list_files(task_id: str = ""):
    """
    列出输出文件
    - task_id=xxx: 只列出该任务生成的文件
    - 不传: 列出所有输出文件
    """
    files = _list_output_files(task_id)
    return {"ok": True, "files": files, "task_id": task_id or None}


@app.get("/api/files/download")
async def api_download_file(file: str):
    """
    下载指定的输出文件
    - file: 文件路径（相对于 output_dir，或绝对路径）
    """
    real_path = _safe_file_path(file)
    if not real_path:
        raise HTTPException(status_code=404, detail="文件不存在或路径非法")
    if not _os.path.isfile(real_path):
        raise HTTPException(status_code=404, detail="文件不存在")

    # 检测 MIME 类型，下载模式
    content_type, _ = _mimetypes.guess_type(real_path)
    fname = _os.path.basename(real_path)

    # RFC 5987: 非 ASCII 文件名必须用 filename*=UTF-8''url_encoded 格式
    from urllib.parse import quote as _quote
    try:
        fname.encode("latin-1")
        # 纯 ASCII，用标准写法
        cd_header = f'attachment; filename="{fname}"'
    except UnicodeEncodeError:
        # 含中文等非 ASCII，用 RFC 5987 编码
        cd_header = f"attachment; filename*=UTF-8''{_quote(fname, safe='')}"

    return FileResponse(
        path=real_path,
        media_type=content_type or "application/octet-stream",
        headers={
            "Content-Disposition": cd_header,
        },
    )


@app.get("/api/files/preview")
async def api_preview_file(file: str):
    """
    预览文本文件（返回内容，用于前端嵌入显示）
    """
    real_path = _safe_file_path(file)
    if not real_path:
        raise HTTPException(status_code=404, detail="文件不存在或路径非法")
    if not _os.path.isfile(real_path):
        raise HTTPException(status_code=404, detail="文件不存在")

    # 只允许预览文本类文件
    content_type, _ = _mimetypes.guess_type(real_path)
    text_types = {"text/", "application/json", "application/xml", "application/javascript"}
    is_text = (content_type and any(
        content_type.startswith(t) for t in text_types
    )) or real_path.endswith((".py", ".md", ".yaml", ".yml", ".toml", ".ini", ".cfg"))

    if not is_text:
        raise HTTPException(status_code=400, detail="此文件类型不支持在线预览，请下载查看")

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


@app.get("/api/tasks/list")
async def api_list_tasks(status: str = "", limit: int = 20):
    tm = get_task_manager()
    tasks = tm.list_tasks(status=status, limit=limit)
    return {"ok": True, "tasks": tasks, "queue": tm.queue_status()}


@app.get("/api/tasks/{task_id}")
async def api_get_task(task_id: str):
    tm = get_task_manager()
    task = tm.get_task(task_id)
    if task is None:
        return JSONResponse({"ok": False, "error": "任务未找到"}, status_code=404)
    return {"ok": True, "task": task}


@app.post("/api/tasks/{task_id}/cancel")
async def api_cancel_task(task_id: str):
    tm = get_task_manager()
    tm.cancel_task(task_id)
    return {"ok": True}


@app.get("/api/tasks/queue/status")
async def api_queue_status():
    tm = get_task_manager()
    return {"ok": True, **tm.queue_status()}


# ============================================================
# 命令补全 API
# ============================================================

@app.get("/api/commands")
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


# ============================================================
# Agent 管理 API
# ============================================================

_agent_persist_executor = None

def _get_agent_persist_executor():
    global _agent_persist_executor
    if _agent_persist_executor is None:
        from concurrent.futures import ThreadPoolExecutor
        _agent_persist_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="agent_db")
    return _agent_persist_executor

async def _persist_agent_to_db(name: str, model: str, provider: str, skills: list[str], description: str):
    """将 Agent 配置写入 MySQL agent_configs 表"""
    import logging as _logging
    _log = _logging.getLogger("smart_agent.web")

    if not _db_initialized:
        _log.warning(f"Agent '{name}' 未持久化: 数据库未初始化 (_db_initialized=False)")
        return

    from src.infrastructure.database import _session_factory
    if _session_factory is None:
        _log.warning(f"Agent '{name}' 未持久化: _session_factory 为 None")
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
                _log.info(f"Agent '{name}' 已更新到数据库")
            else:
                cfg = AgentConfigModel(
                    name=name, model=model, provider=provider,
                    skills=skills, description=description,
                )
                session.add(cfg)
                _log.info(f"Agent '{name}' 已写入数据库")
            await session.commit()
    except Exception as e:
        _log.error(f"Agent '{name}' 持久化到数据库失败: {e}", exc_info=True)


async def _restore_agents(tm):
    """从数据库恢复历史 Agent 到任务管理器"""
    from src.infrastructure.database import _session_factory
    if _session_factory is None:
        return
    from src.infrastructure.models import AgentConfigModel
    from sqlalchemy import select
    import logging as _logging
    _logger = _logging.getLogger("smart_agent.web")

    async with _session_factory() as session:
        result = await session.execute(select(AgentConfigModel))
        cfgs = result.scalars().all()

    count = 0
    for cfg in cfgs:
        if cfg.name in tm._agents:
            continue  # 已存在，跳过
        try:
            from src.core.llm import LLMConfig
            from src.tools.builtin_tools import register_all
            config = LLMConfig(provider=cfg.provider or "deepseek", model=cfg.model or "deepseek-chat")
            new_agent = Agent()
            new_agent.name = cfg.name
            skills = cfg.skills or []
            skill_desc = f"专注于{'、'.join(skills)}" if skills else "通用"
            new_agent.system_prompt = (
                f"你是 {cfg.name}，一个{skill_desc}的 AI 助手。"
                f"请用你的专业知识高效完成用户的任务。"
            )
            new_agent.init(config)
            register_all(new_agent.tools)
            if hasattr(new_agent, '_rebuild_graph'):
                new_agent._rebuild_graph()
            proxy = AgentProxy(
                name=cfg.name, agent=new_agent,
                skills=skills,
                description=cfg.description or f"{skill_desc}型 Agent",
            )
            tm.register_agent(proxy)
            count += 1
        except Exception as e:
            _logger.warning(f"恢复 Agent '{cfg.name}' 失败: {e}")
    if count > 0:
        _logger.info(f"从数据库恢复了 {count} 个历史 Agent")

@app.get("/api/agents/list")
async def api_list_agents():
    tm = get_task_manager()
    agents = tm.list_agents()
    return {"ok": True, "agents": agents}


@app.post("/api/agents/register")
async def api_register_agent():
    agent = get_agent()
    tm = get_task_manager()
    proxy = AgentProxy(name=agent.name, agent=agent)
    tm.register_agent(proxy)
    tm.start_dispatcher()
    return {"ok": True, "agent_name": agent.name}


@app.post("/api/agents/unregister")
async def api_unregister_agent():
    agent = get_agent()
    tm = get_task_manager()
    tm.unregister_agent(agent.name)
    return {"ok": True, "agent_name": agent.name}


class CreateAgentRequest(BaseModel):
    name: str
    model: str = "deepseek-chat"
    provider: str = "deepseek"
    skills: list[str] = []
    description: str = ""


@app.post("/api/agents/create")
async def api_create_agent(req: CreateAgentRequest):
    from src.core.llm import LLMConfig
    from src.tools.builtin_tools import register_all

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
        name=req.name,
        agent=new_agent,
        skills=req.skills,
        description=req.description or f"{skill_desc}型 Agent",
    )
    tm.register_agent(proxy)
    tm.start_dispatcher()

    # 持久化到数据库
    await _persist_agent_to_db(req.name, req.model, req.provider, req.skills, req.description or "")

    return {"ok": True, "agent_name": req.name, "model": req.model, "skills": req.skills}


# ============================================================
# 仪表盘 API
# ============================================================

@app.get("/api/dashboard/stats")
async def api_dashboard_stats():
    tm = get_task_manager()
    agent = get_agent()
    qs = tm.queue_status()
    all_tasks = tm.list_tasks(status="", limit=100)
    recent = all_tasks[:10]

    status_counts = {"pending": 0, "running": 0, "completed": 0, "failed": 0, "cancelled": 0}
    for t in all_tasks:
        s = t.get("status", "")
        if s in status_counts:
            status_counts[s] += 1

    all_skills = set()
    for a in (tm.list_agents() or []):
        for s in (a.get("skills") or []):
            all_skills.add(s)

    return {
        "ok": True,
        "stats": {
            "total_tasks": len(all_tasks),
            "pending": status_counts["pending"],
            "running": status_counts["running"],
            "completed": status_counts["completed"],
            "failed": status_counts["failed"],
            "agents_total": qs.get("agents", 0),
            "agents_idle": qs.get("idle_agents", 0),
            "tools": len(agent.tools),
            "skills": sorted(all_skills),
        },
        "recent_tasks": recent,
        "agents": tm.list_agents(),
        "current_model": agent.llm.config.model if agent.llm else "N/A",
    }


# ============================================================
# 任务编辑 API
# ============================================================

class UpdateTaskRequest(BaseModel):
    description: str | None = None
    title: str | None = None
    priority: int | None = None
    tags: list[str] | None = None
    target_agent: str | None = None


@app.post("/api/tasks/{task_id}/update")
async def api_update_task(task_id: str, req: UpdateTaskRequest):
    tm = get_task_manager()
    task_dict = tm.get_task(task_id)
    if task_dict is None:
        return JSONResponse({"ok": False, "error": "任务未找到"}, status_code=404)

    task = None
    with tm._lock:
        task = tm._tasks.get(task_id)
    if task is None:
        return JSONResponse({"ok": False, "error": "任务对象不存在"}, status_code=404)

    if task.status.name not in ("PENDING",):
        return JSONResponse(
            {"ok": False, "error": f"只能编辑待处理状态的任务，当前状态: {task.status.name}"},
            status_code=400,
        )

    if req.description is not None:
        task.description = req.description
    if req.title is not None:
        task.title = req.title
    if req.priority is not None:
        task.priority = req.priority
    if req.tags is not None:
        task.tags = req.tags
    if req.target_agent is not None:
        task.assigned_agent = req.target_agent

    return {"ok": True, "task": task.to_dict()}


# ============================================================
# Agent 更新/删除 API
# ============================================================

class UpdateAgentRequest(BaseModel):
    skills: list[str] | None = None
    description: str | None = None


@app.post("/api/agents/{name}/update")
async def api_update_agent(name: str, req: UpdateAgentRequest):
    tm = get_task_manager()
    proxy = tm._agents.get(name)
    if proxy is None:
        return JSONResponse({"ok": False, "error": "Agent 未找到"}, status_code=404)

    if req.skills is not None:
        proxy.skills = req.skills
    if req.description is not None:
        proxy.description = req.description

    return {"ok": True, "agent": proxy.to_dict()}


@app.delete("/api/agents/{name}")
async def api_delete_agent(name: str):
    tm = get_task_manager()
    if name not in tm._agents:
        return JSONResponse({"ok": False, "error": "Agent 未找到"}, status_code=404)

    tm.unregister_agent(name)

    # 同时从数据库删除，防止重启后恢复
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

    return {"ok": True}


# ============================================================
# 配置读写 API
# ============================================================

@app.get("/api/config/full")
async def api_config_full():
    import yaml

    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "config.yaml",
    )
    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    else:
        cfg = {}
    return {"ok": True, "config": cfg}


class UpdateConfigRequest(BaseModel):
    section: str
    data: dict


@app.post("/api/config/update")
async def api_config_update(req: UpdateConfigRequest):
    import yaml

    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
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


# ============================================================
# 启动函数
# ============================================================

def init_agent():
    """初始化全局 Agent 实例 + 数据库 + 日志"""
    global _agent, _db_initialized

    import yaml
    import logging

    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
        "config.yaml",
    )

    if os.path.exists(config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
    else:
        cfg = {}

    # ---- 结构化日志 ----
    log_cfg = cfg.get("logging", {})
    try:
        from src.core.logging_config import setup_logging

        setup_logging(
            level=log_cfg.get("level", "INFO"),
            log_dir=log_cfg.get("dir", "./logs"),
            json_format=log_cfg.get("json_format", False),
            enable_mysql=log_cfg.get("mysql_errors", False),
        )
    except Exception:
        logging.basicConfig(level=logging.INFO)

    logger = logging.getLogger("smart_agent.web")

    # ---- 数据库初始化 ----
    db_cfg = cfg.get("database", {})
    db_url = os.getenv("DATABASE_URL", "")
    _db_initialized = False

    # 创建独立的 event loop，避免 "no current event loop" 问题
    import asyncio as _db_asyncio
    _db_loop = _db_asyncio.new_event_loop()
    _db_asyncio.set_event_loop(_db_loop)

    if db_url or db_cfg:
        # config.yaml 的值优先于环境变量和 DATABASE_URL
        if db_cfg:
            os.environ["DB_HOST"] = str(db_cfg.get("host", "127.0.0.1"))
            os.environ["DB_PORT"] = str(db_cfg.get("port", 3306))
            os.environ["DB_USER"] = str(db_cfg.get("user", "smart_agent"))
            os.environ["DB_PASSWORD"] = str(db_cfg.get("password", ""))
            os.environ["DB_NAME"] = str(db_cfg.get("database", "smart_agent"))
            os.environ.pop("DATABASE_URL", None)

        from src.infrastructure.database import create_engine, get_db_url
        from src.infrastructure.migrations import run_migrations, seed_default_admin

        try:
            # 步骤1: 构建 URL + 创建引擎
            url = get_db_url()
            engine = create_engine(url)
            logger.info("数据库引擎创建成功")
        except Exception as e:
            logger.warning(f"数据库引擎创建失败: {e}")
            engine = None

        if engine:
            try:
                # 步骤2: 迁移
                _db_loop.run_until_complete(run_migrations(engine))
                logger.info("数据库迁移完成")
            except Exception as e:
                logger.warning(f"数据库迁移失败: {e}")
                engine = None

        if engine:
            try:
                # 步骤3: 初始化种子数据
                _db_loop.run_until_complete(seed_default_admin(engine))
                _db_initialized = True
                logger.info("MySQL 数据库已连接并完成初始化")
            except Exception as e:
                logger.warning(f"数据库种子数据失败: {e}")

        if _db_initialized:
            # 启用任务持久化
            from src.infrastructure.task_repo import get_task_repo
            get_task_repo().enable_db()
            # 从数据库恢复历史任务（使用同一个 loop）
            try:
                _db_loop.run_until_complete(_load_history_async())
            except Exception as e:
                logger.warning(f"历史任务恢复跳过: {e}")
        else:
            logger.warning("MySQL 连接失败，使用内存模式")
    else:
        logger.info("未配置数据库，使用内存模式")

    # ---- 中间件安装 ----
    rl_cfg = cfg.get("rate_limit", {})
    if rl_cfg.get("enabled", True):
        try:
            from src.middleware.rate_limiter import RateLimitMiddleware
            app.add_middleware(
                RateLimitMiddleware,
                max_requests=rl_cfg.get("max_requests_per_minute", 120),
                burst=rl_cfg.get("burst", 30),
            )
            logger.info(f"速率限制已启用: {rl_cfg.get('max_requests_per_minute', 120)}次/分钟")
        except Exception as e:
            logger.warning(f"速率限制加载失败: {e}")

    mon_cfg = cfg.get("monitoring", {})
    if mon_cfg.get("enabled", True):
        try:
            from src.middleware.metrics import PrometheusMiddleware
            app.add_middleware(PrometheusMiddleware)
            logger.info("Prometheus 监控已启用")
        except Exception as e:
            logger.warning(f"监控中间件加载失败: {e}")

    # ---- Agent 初始化 ----
    llm_cfg = cfg.get("llm", {})
    agent_cfg = cfg.get("agent", {})

    _agent = create_agent(
        provider=llm_cfg.get("provider", "openai"),
        model=llm_cfg.get("model", "gpt-4o"),
        api_key=llm_cfg.get("api_key", ""),
        base_url=llm_cfg.get("base_url", ""),
        temperature=float(llm_cfg.get("temperature", 0.7)),
        system_prompt=agent_cfg.get(
            "system_prompt",
            "你是一个智能 AI 助手，具备工具使用、文件操作、代码执行等能力。",
        ),
        max_iterations=agent_cfg.get("max_iterations", 15),
        verbose=agent_cfg.get("verbose", True),
    )

    register_all(_agent.tools)
    # 工具注册后重建 graph，让 LangChain 感知到工具
    if hasattr(_agent, '_rebuild_graph'):
        _agent._rebuild_graph()

    logger.info(f"Agent '{_agent.name}' 初始化完成 (模型: {llm_cfg.get('model', 'N/A')})")


def _try_parse_dt(val) -> Optional[datetime]:
    """将 ISO 字符串或 datetime 转为 datetime"""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


async def _load_history_async():
    """异步加载历史任务（复用数据库 loop）"""
    from src.infrastructure.task_repo import get_task_repo
    repo = get_task_repo()
    if not repo.db_enabled:
        return
    tm = get_task_manager()
    tasks = await repo.list_tasks(status="", limit=100)
    count = 0
    with tm._lock:
        for td in tasks:
            tid = td.get("id", "")
            if tm._find_task(tid) is not None:
                continue
            from src.core.task_manager import Task, TaskStatus
            task = Task(
                id=tid,
                title=td.get("title", ""),
                description=td.get("description", ""),
                status=TaskStatus(td.get("status", "pending")),
                created_at=_try_parse_dt(td.get("created_at")) or datetime.now(),
                started_at=_try_parse_dt(td.get("started_at")),
                finished_at=_try_parse_dt(td.get("finished_at")),
                assigned_agent=td.get("assigned_agent"),
                result=td.get("result"),
                error=td.get("error"),
                priority=td.get("priority", 0),
                tags=td.get("tags", []),
            )
            tm._history.append(task)
            count += 1
    if count > 0:
        logging.getLogger("smart_agent.web").info(f"从数据库恢复了 {count} 个历史任务")


def start(host: str = "127.0.0.1", port: int = 8080):
    """启动 Web 服务"""
    import logging

    init_agent()

    logger = logging.getLogger("smart_agent.web")

    # 注册默认 Agent 到任务管理器并启动调度器
    tm = get_task_manager()
    proxy = AgentProxy(name=_agent.name, agent=_agent)
    tm.register_agent(proxy)
    tm.start_dispatcher()

    # 从数据库恢复历史 Agent（复用数据库 loop）
    if _db_initialized:
        try:
            _db_loop.run_until_complete(_restore_agents(tm))
        except Exception as e:
            logger.warning(f"Agent 恢复跳过: {e}")

    # 挂载多 Agent 编排器
    try:
        patch_task_manager(tm)
        logger.info("多 Agent 编排器已挂载")
    except Exception as e:
        logger.warning(f"编排器加载失败: {e}")

    db_status = "MySQL" if _db_initialized else "内存"
    print(f"\n  {'='*50}")
    print(f"  SmartAgent v2.0 已启动")
    print(f"  地址: http://{host}:{port}")
    print(f"  Agent: {_agent.name} 已注册  |  任务调度器已启动")
    print(f"  存储: {db_status}  |  日志: ./logs/")
    print(f"  健康检查: http://{host}:{port}/health")
    print(f"  Prometheus: http://{host}:{port}/metrics")
    print(f"  {'='*50}\n")
    uvicorn.run(app, host=host, port=port, log_level="warning")
