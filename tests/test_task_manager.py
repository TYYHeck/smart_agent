# -*- coding: utf-8 -*-
"""任务管理器单元测试"""

from __future__ import annotations
import sys
import os
import pytest

_proj_root = os.path.dirname(os.path.dirname(__file__))
if _proj_root not in sys.path:
    sys.path.insert(0, _proj_root)

from src.core.task_manager import (
    TaskManager, Task, TaskStatus, AgentProxy, _current_task_id,
)
from src.core.message import Message


# ============================================================
# Task
# ============================================================

class TestTask:
    def test_task_creation_defaults(self):
        task = Task(title="测试任务", description="这是一个测试")
        assert task.id is not None
        assert len(task.id) == 8
        assert task.title == "测试任务"
        assert task.description == "这是一个测试"
        assert task.status == TaskStatus.PENDING
        assert task.priority == 0
        assert task.result is None

    def test_task_to_dict(self):
        task = Task(title="T", description="D", priority=5, tags=["urgent"])
        d = task.to_dict()
        assert d["id"] == task.id
        assert d["title"] == "T"
        assert d["description"] == "D"
        assert d["status"] == "pending"
        assert d["priority"] == 5
        assert d["tags"] == ["urgent"]
        assert "event_log" in d
        assert "output_files" in d

    def test_task_to_dict_truncates_description(self):
        task = Task(title="T", description="X" * 300)
        d = task.to_dict()
        assert len(d["description"]) == 200

    def test_add_event(self):
        task = Task(title="T", description="D")
        task.add_event("tool_call", {"name": "calc", "args": {"x": 1}})
        assert len(task.event_log) == 1
        assert task.event_log[0]["event"] == "tool_call"
        assert task.event_log[0]["data"] == {"name": "calc", "args": {"x": 1}}

    def test_custom_id(self):
        task = Task(id="custom-001", title="T", description="D")
        assert task.id == "custom-001"


# ============================================================
# TaskStatus
# ============================================================

class TestTaskStatus:
    def test_enum_values(self):
        assert TaskStatus.PENDING.value == "pending"
        assert TaskStatus.RUNNING.value == "running"
        assert TaskStatus.COMPLETED.value == "completed"
        assert TaskStatus.FAILED.value == "failed"
        assert TaskStatus.CANCELLED.value == "cancelled"


# ============================================================
# AgentProxy
# ============================================================

class TestAgentProxy:
    def test_agent_proxy_defaults(self):
        proxy = AgentProxy(name="helper", agent="mock_agent")
        assert proxy.name == "helper"
        assert proxy.agent == "mock_agent"
        assert proxy.status == "idle"
        assert proxy.current_task_id is None
        assert proxy.skills == []
        assert proxy.description == ""

    def test_agent_proxy_to_dict(self):
        proxy = AgentProxy(
            name="coder", agent="mock",
            skills=["coding", "debugging"],
            description="编程专家",
        )
        d = proxy.to_dict()
        assert d["name"] == "coder"
        assert d["status"] == "idle"
        assert "coding" in d["skills"]
        assert d["description"] == "编程专家"


# ============================================================
# TaskManager 核心
# ============================================================

class TestTaskManager:
    def test_task_manager_creation(self):
        tm = TaskManager()
        assert tm.pending_count() == 0

    def test_publish_task(self):
        tm = TaskManager()
        task_id = tm.publish(description="搜索新闻", title="搜索任务", priority=3)
        assert task_id is not None
        assert len(task_id) == 8
        assert tm.pending_count() == 1

    def test_publish_auto_title(self):
        tm = TaskManager()
        desc = "这是一段很长的描述文本" * 10  # 很长
        task_id = tm.publish(description=desc)
        task = tm.get_task(task_id)
        assert task is not None
        assert len(task["title"]) <= 53  # 50 + "..."

    def test_publish_multiple_tasks(self):
        tm = TaskManager()
        for i in range(3):
            tm.publish(description=f"任务 {i}")
        assert tm.pending_count() == 3

    def test_publish_priority_ordering(self):
        tm = TaskManager()
        tm.publish(description="低优先级", priority=1)
        tm.publish(description="高优先级", priority=10)
        tm.publish(description="中优先级", priority=5)
        # 检查队列顺序（高优先级在前）
        with tm._lock:
            tasks = list(tm._queue)
        assert tasks[0].priority == 10
        assert tasks[1].priority == 5
        assert tasks[2].priority == 1

    def test_get_task_found(self):
        tm = TaskManager()
        task_id = tm.publish(description="测试")
        task = tm.get_task(task_id)
        assert task is not None
        assert task["description"] == "测试"

    def test_get_task_not_found(self):
        tm = TaskManager()
        task = tm.get_task("nonexistent")
        assert task is None

    def test_cancel_task(self):
        tm = TaskManager()
        task_id = tm.publish(description="要取消的任务")
        assert tm.pending_count() == 1
        tm.cancel_task(task_id)
        # cancel 只改状态，不移出队列
        task = tm.get_task(task_id)
        assert task is not None
        assert task["status"] == "cancelled"

    def test_queue_status_empty(self):
        tm = TaskManager()
        status = tm.queue_status()
        assert status["pending"] == 0

    def test_queue_status_with_tasks(self):
        tm = TaskManager()
        tm.publish(description="任务1")
        tm.publish(description="任务2")
        status = tm.queue_status()
        assert status["pending"] == 2

    def test_list_tasks(self):
        tm = TaskManager()
        tm.publish(description="任务A", priority=1)
        tm.publish(description="任务B", priority=2)
        tasks = tm.list_tasks()
        assert len(tasks) == 2

    def test_list_tasks_by_status(self):
        tm = TaskManager()
        task_id = tm.publish(description="待取消")
        tm.cancel_task(task_id)
        pending = tm.list_tasks(status="pending")
        cancelled = tm.list_tasks(status="cancelled")
        assert len(pending) == 0
        assert len(cancelled) == 1


# ============================================================
# Agent 注册管理
# ============================================================

class TestAgentManagement:
    def test_register_agent(self):
        tm = TaskManager()
        proxy = AgentProxy(name="helper", agent="mock")
        tm.register_agent(proxy)
        assert "helper" in tm._agents
        assert tm._agents["helper"].name == "helper"

    def test_unregister_agent(self):
        tm = TaskManager()
        proxy = AgentProxy(name="helper", agent="mock")
        tm.register_agent(proxy)
        tm.unregister_agent("helper")
        assert "helper" not in tm._agents

    def test_list_agents(self):
        tm = TaskManager()
        tm.register_agent(AgentProxy(name="a1", agent="mock1"))
        tm.register_agent(AgentProxy(name="a2", agent="mock2"))
        agents = tm.list_agents()
        assert len(agents) == 2
        names = {a["name"] for a in agents}
        assert names == {"a1", "a2"}


# ============================================================
# 智能分配
# ============================================================

class TestSmartAssign:
    def test_smart_assign_no_skills_match(self):
        tm = TaskManager()
        task = Task(title="T", description="写代码", tags=["coding"])
        agents = [("coder", AgentProxy(name="coder", agent="mock", skills=["writing"]))]
        name, proxy = tm._smart_assign(task, agents)
        # 技能不匹配但只有一个 agent，仍会分配
        assert name == "coder"

    def test_smart_assign_skills_match(self):
        tm = TaskManager()
        task = Task(title="T", description="写代码", tags=["coding"])
        agents = [
            ("writer", AgentProxy(name="writer", agent="mock", skills=["writing"])),
            ("coder", AgentProxy(name="coder", agent="mock", skills=["coding"])),
        ]
        name, proxy = tm._smart_assign(task, agents)
        assert name == "coder"
