# -*- coding: utf-8 -*-
"""Agent 核心单元测试"""

from __future__ import annotations
import sys
import os
import pytest
from unittest.mock import MagicMock, patch, AsyncMock

_proj_root = os.path.dirname(os.path.dirname(__file__))
if _proj_root not in sys.path:
    sys.path.insert(0, _proj_root)

from src.core.agent import (
    Agent, AgentState, AgentEvent, AgentCallbackHandler,
    create_agent,
)
from src.core.llm import LLMConfig
from src.core.message import Message, ToolCall, ToolResult


# ============================================================
# LLMConfig + Agent 创建
# ============================================================

class TestCreateAgent:
    def test_create_agent_with_deepseek(self):
        agent = create_agent(
            provider="deepseek",
            model="deepseek-chat",
            api_key="sk-test123",
            system_prompt="你是一个测试助手",
        )
        assert agent.name == "SmartAgent"
        assert agent.system_prompt == "你是一个测试助手"
        assert agent.llm is not None
        assert agent.llm.config.provider == "deepseek"
        assert agent.llm.config.model == "deepseek-chat"

    def test_create_agent_defaults(self):
        agent = create_agent()
        assert agent.name == "SmartAgent"
        assert agent.max_iterations == 15
        assert agent.enable_planning is False
        assert agent.enable_rag is True
        assert agent.enable_reflection is False

    def test_create_agent_full_options(self):
        agent = create_agent(
            provider="openai", model="gpt-4o",
            system_prompt="高级助手",
            max_iterations=20,
            verbose=False,
            enable_planning=True,
            enable_rag=False,
            enable_reflection=True,
        )
        assert agent.max_iterations == 20
        assert agent.verbose is False
        assert agent.enable_planning is True
        assert agent.enable_rag is False
        assert agent.enable_reflection is True


# ============================================================
# Agent 状态管理
# ============================================================

class TestAgentState:
    def test_initial_state_idle(self):
        agent = Agent()
        assert agent.state == AgentState.IDLE

    def test_agent_hooks_event_callback(self):
        agent = Agent()
        events = []

        def handler(event, data):
            events.append((event, data))

        agent.on_event = handler
        agent._emit(AgentEvent.THINK_START, {"task": "测试"})
        assert len(events) == 1
        assert events[0][0] == AgentEvent.THINK_START
        assert events[0][1] == {"task": "测试"}

    def test_event_callback_does_not_crash(self):
        """回调失败不应中断 Agent"""
        agent = Agent()

        def handler(event, data):
            raise RuntimeError("回调炸了")

        agent.on_event = handler
        agent._emit(AgentEvent.ERROR, {})  # 不应抛出异常
        assert agent.state == AgentState.IDLE

    def test_agent_log_output(self, capsys):
        agent = Agent(verbose=True)
        agent._log("测试日志")
        # verbose=True 时应该记录
        # pytest 的 capsys 可能捕获不到 logging，这里只检查不崩溃


# ============================================================
# 模型管理
# ============================================================

class TestModelManagement:
    def test_switch_model_raises_if_not_init(self):
        agent = Agent()
        with pytest.raises(RuntimeError):
            agent.switch_model("gpt-4o")

    def test_switch_model_preserves_tools(self):
        agent = Agent()
        from src.tools.builtin_tools import register_all
        register_all(agent.tools)
        tool_count_before = len(agent.tools)
        agent.init(LLMConfig(provider="openai", model="gpt-4o", api_key="test"))

        agent.switch_model("gpt-4o-mini")
        assert agent.llm.config.model == "gpt-4o-mini"
        assert len(agent.tools) == tool_count_before


# ============================================================
# available_models 兜底逻辑
# ============================================================

class TestAvailableModels:
    def test_fallback_models_no_llm(self):
        agent = Agent()
        assert agent.available_models() == []

    def test_fallback_models_openai(self):
        agent = Agent()
        agent.llm = MagicMock()
        agent.llm.config = LLMConfig(provider="openai", model="gpt-4o", api_key="test")

        with patch.object(agent, '_belongs_to_provider', return_value=True):
            with patch("urllib.request.urlopen", side_effect=Exception("timeout")):
                models = agent.available_models()
                assert len(models) > 0
                assert any(m["provider"] == "openai" for m in models)

    def test_fallback_models_deepseek(self):
        agent = Agent()
        agent.llm = MagicMock()
        agent.llm.config = LLMConfig(provider="deepseek", model="deepseek-chat", api_key="test")
        agent.llm.config.resolve_api_key = lambda: "test"
        agent.llm.config.resolve_base_url = lambda: "https://api.deepseek.com"

        with patch("urllib.request.urlopen", side_effect=Exception("timeout")):
            models = agent.available_models()
            assert len(models) > 0
            assert any("deepseek" in m["id"] for m in models)


# ============================================================
# 计划模式
# ============================================================

class TestPlanning:
    def test_task_is_complex(self):
        agent = Agent()
        assert agent._task_is_complex("先分析再总结")
        assert agent._task_is_complex("多个步骤的任务")
        assert agent._task_is_complex("比较两个方案")
        assert not agent._task_is_complex("你好")

    def test_make_plan_no_llm(self):
        agent = Agent()
        assert agent._make_plan("分析这个项目") == []

    def test_make_plan_fallback_on_error(self):
        agent = Agent()
        agent.llm = MagicMock()
        agent.llm.chat.side_effect = Exception("API 挂了")
        assert agent._make_plan("分析") == []


# ============================================================
# 反思模式
# ============================================================

class TestReflection:
    def test_reflect_no_llm(self):
        agent = Agent()
        answer = agent._reflect("测试答案", "测试任务")
        assert answer == "测试答案"  # no LLM, 原样返回

    def test_reflect_ok(self):
        agent = Agent()
        mock_response = MagicMock()
        mock_response.content = "EXACT: OK"
        agent.llm = MagicMock()
        agent.llm.chat.return_value = mock_response
        answer = agent._reflect("测试答案", "测试任务")
        assert answer == "测试答案"

    def test_reflect_error_fallback(self):
        agent = Agent()
        agent.llm = MagicMock()
        agent.llm.chat.side_effect = Exception("反思失败")
        answer = agent._reflect("测试答案", "测试任务")
        assert answer == "测试答案"


# ============================================================
# force_conclude
# ============================================================

class TestForceConclude:
    def test_force_conclude_no_llm(self):
        agent = Agent()
        result = agent._force_conclude()
        assert "超时" in result

    def test_force_conclude_error_fallback(self):
        agent = Agent()
        agent.llm = MagicMock()
        agent.llm.chat.side_effect = Exception("强制总结失败")
        result = agent._force_conclude()
        assert "超时" in result


# ============================================================
# provider 过滤
# ============================================================

class TestBelongsToProvider:
    def test_openai_models(self):
        assert Agent._belongs_to_provider("gpt-4o", "openai") is True
        assert Agent._belongs_to_provider("gpt-4o-mini", "openai") is True
        assert Agent._belongs_to_provider("deepseek-chat", "openai") is False

    def test_deepseek_models(self):
        assert Agent._belongs_to_provider("deepseek-chat", "deepseek") is True
        assert Agent._belongs_to_provider("deepseek-reasoner", "deepseek") is True
        assert Agent._belongs_to_provider("gpt-4o", "deepseek") is False

    def test_qwen_models(self):
        assert Agent._belongs_to_provider("qwen-plus", "qwen") is True
        assert Agent._belongs_to_provider("glm-4", "qwen") is False


# ============================================================
# 工具集成
# ============================================================

class TestToolIntegration:
    def test_agent_has_initial_tools(self):
        agent = Agent()
        from src.tools.builtin_tools import register_all
        register_all(agent.tools)
        assert len(agent.tools) > 0

    def test_build_langchain_tools(self):
        agent = Agent()
        from src.tools.builtin_tools import register_all
        register_all(agent.tools)
        tools = agent._build_langchain_tools()
        assert len(tools) == len(agent.tools)


# ============================================================
# system prompt
# ============================================================

class TestSystemPrompt:
    def test_build_system_prompt(self):
        agent = Agent(system_prompt="你是一个助手")
        prompt = agent._build_system_prompt()
        assert "你是一个助手" in prompt

    def test_build_full_system_prompt_includes_identity(self):
        agent = Agent(system_prompt="你是助手")
        from src.tools.builtin_tools import register_all
        register_all(agent.tools)
        prompt = agent._build_full_system_prompt()
        assert "SmartAgent" in prompt
        assert "中文" in prompt


# ============================================================
# AgentCallbackHandler
# ============================================================

class TestCallbackHandler:
    def test_handler_creation(self):
        agent = Agent()
        handler = AgentCallbackHandler(agent)
        assert handler._agent is agent
        assert handler._current_tool_calls == {}

    def test_on_tool_error(self):
        agent = Agent()
        handler = AgentCallbackHandler(agent)
        from uuid import uuid4
        handler._current_tool_calls[str(uuid4())] = {"name": "calc"}
        handler.on_tool_error(ValueError("错误"), run_id=uuid4())
        assert agent.state != AgentState.ERROR  # on_tool_error 不改变状态
