# -*- coding: utf-8 -*-
"""Web API 单元测试 —— 基于 FastAPI TestClient"""

from __future__ import annotations
import sys
import os

_proj_root = os.path.dirname(os.path.dirname(__file__))
if _proj_root not in sys.path:
    sys.path.insert(0, _proj_root)

import pytest
from unittest.mock import MagicMock, patch, AsyncMock

from fastapi.testclient import TestClient

# 必须在导入 app 之前 mock 掉 Agent 和数据库初始化
# 避免 test 中创建实际连接

@pytest.fixture
def client():
    """创建测试客户端（模拟 Agent + 数据库）"""
    # Mock Agent
    mock_agent = MagicMock()
    mock_agent.name = "SmartAgent"
    mock_agent.llm = MagicMock()
    mock_agent.llm.config = MagicMock()
    mock_agent.llm.config.model = "deepseek-chat"
    mock_agent.llm.config.provider = "deepseek"
    mock_agent.agent_graph = None
    mock_agent.tools = MagicMock()
    mock_agent.tools.list_all.return_value = [MagicMock(name="test_tool")]
    mock_agent.tools.to_langchain_tools.return_value = []
    mock_agent.available_models.return_value = []
    mock_agent.tools.__len__ = lambda s: 1

    mock_agent.stream_events = AsyncMock()
    mock_agent.stream_events.return_value = __import__("collections").deque()

    # Mock TaskManager
    mock_tm = MagicMock()
    mock_tm.list_tasks.return_value = []
    mock_tm.queue_status.return_value = {"pending": 0, "running": 0}
    mock_tm.list_agents.return_value = []
    mock_tm.execute_orchestrated = MagicMock()
    mock_tm.detect_best_mode.return_value = {"mode": "single", "reason": "简单任务"}

    # Mock 数据库会话（避免登录端点尝试真连接）
    mock_result = MagicMock()
    mock_result.scalar_one_or_none = MagicMock(return_value=None)
    mock_session = AsyncMock()
    mock_session.execute = AsyncMock(return_value=mock_result)
    mock_session.commit = AsyncMock()

    async def _mock_get_session():
        yield mock_session

    with patch("src.ui.web_server._agent", mock_agent), \
         patch("src.ui.web_server._db_initialized", False), \
         patch("src.ui.web_server.get_task_manager", return_value=mock_tm):
        from src.ui.web_server import app
        from src.infrastructure.database import get_session as real_get_session
        # 用 FastAPI dependency_overrides 替换数据库会话
        app.dependency_overrides[real_get_session] = _mock_get_session
        yield TestClient(app)
        app.dependency_overrides.clear()


# ============================================================
# 公共端点（无需认证）
# ============================================================

class TestPublicEndpoints:
    def test_health_check(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("healthy", "degraded")
        assert "version" in data

    def test_index_page(self, client):
        resp = client.get("/")
        assert resp.status_code == 200
        assert "SmartAgent" in resp.text or "html" in resp.text.lower()

    def test_metrics_endpoint(self, client):
        resp = client.get("/metrics")
        assert resp.status_code == 200
        assert "http_requests" in resp.text or "metrics" in resp.text.lower()

    def test_login_no_db(self, client):
        """登录需要数据库，预期 401 或 503"""
        resp = client.post("/api/auth/login", json={
            "username": "admin", "password": "admin123",
        })
        # 数据库未初始化时可能返回各种状态
        assert resp.status_code in (401, 422, 503)


# ============================================================
# 需要认证的端点
# ============================================================

class TestAuthRequiredEndpoints:
    """这些端点需要 Bearer Token，无 Token 时应返回 401"""

    def test_config_requires_auth(self, client):
        resp = client.get("/api/config")
        assert resp.status_code in (401, 403)

    def test_models_requires_auth(self, client):
        resp = client.get("/api/models")
        assert resp.status_code in (401, 403)

    def test_chat_requires_auth(self, client):
        resp = client.post("/api/chat", json={"message": "你好"})
        assert resp.status_code in (401, 403)

    def test_tasks_list_requires_auth(self, client):
        resp = client.get("/api/tasks/list")
        assert resp.status_code in (401, 403)

    def test_agents_list_requires_auth(self, client):
        resp = client.get("/api/agents/list")
        assert resp.status_code in (401, 403)

    def test_dashboard_requires_auth(self, client):
        resp = client.get("/api/dashboard/stats")
        assert resp.status_code in (401, 403)

    def test_system_info_requires_auth(self, client):
        resp = client.get("/api/system/info")
        assert resp.status_code in (401, 403)

    def test_config_full_requires_auth(self, client):
        resp = client.get("/api/config/full")
        assert resp.status_code in (401, 403)

    def test_config_update_requires_auth(self, client):
        resp = client.post("/api/config/update", json={
            "section": "llm", "data": {},
        })
        assert resp.status_code in (401, 403)


# ============================================================
# 需要认证的 POST 端点
# ============================================================

class TestAuthRequiredPostEndpoints:
    def test_switch_model_requires_auth(self, client):
        resp = client.post("/api/switch_model", json={"model": "gpt-4o"})
        assert resp.status_code in (401, 403)

    def test_toggle_mode_requires_auth(self, client):
        resp = client.post("/api/toggle_mode", json={"mode": "planning"})
        assert resp.status_code in (401, 403)

    def test_publish_task_requires_auth(self, client):
        resp = client.post("/api/tasks/publish", json={
            "description": "测试任务",
        })
        assert resp.status_code in (401, 403)

    def test_orchestrate_requires_auth(self, client):
        resp = client.post("/api/tasks/orchestrate", json={
            "description": "测试编排",
        })
        assert resp.status_code in (401, 403)

    def test_create_agent_requires_auth(self, client):
        resp = client.post("/api/agents/create", json={
            "name": "test_agent", "model": "deepseek-chat",
        })
        assert resp.status_code in (401, 403)

    def test_agent_cleanup_requires_auth(self, client):
        resp = client.post("/api/agents/cleanup")
        assert resp.status_code in (401, 403)


# ============================================================
# Pydantic 校验
# ============================================================

class TestPydanticValidation:
    def test_login_empty_username(self, client):
        resp = client.post("/api/auth/login", json={
            "username": "", "password": "test",
        })
        assert resp.status_code == 422

    def test_login_short_password(self, client):
        resp = client.post("/api/auth/login", json={
            "username": "admin", "password": "ab",
        })
        assert resp.status_code == 422

    def test_create_agent_empty_name(self, client):
        resp = client.post("/api/agents/create", json={
            "name": "", "model": "deepseek-chat",
        })
        assert resp.status_code == 422  # min_length=1

    def test_config_update_invalid(self, client):
        resp = client.post("/api/config/update", json={
            "data": {},
        })
        assert resp.status_code == 422  # missing required section
