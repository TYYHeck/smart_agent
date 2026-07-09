# -*- coding: utf-8 -*-
"""
任务管理器 —— 支持异步任务发布、队列调度与状态追踪

设计模式：发布-订阅 (Pub-Sub) + 任务队列
  1. 发布任务：用户/AI 创建任务放入队列
  2. Agent 领取：空闲 Agent 从队列取任务执行
  3. 状态追踪：每项任务有唯一 ID，可查询执行进度
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Optional
from collections import deque
from contextvars import ContextVar
import uuid
import threading
import json
import logging
import re
import asyncio as _asyncio
from concurrent.futures import ThreadPoolExecutor

# ── 当前执行上下文（用于 write_file 自动关联任务）──
_current_task_id: ContextVar[Optional[str]] = ContextVar("current_task_id", default=None)

logger = logging.getLogger("smart_agent.task_manager")

# 后台线程池，用于在同步上下文中执行异步 DB 写入
_db_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="db_persist")


# ============================================================
# 任务状态
# ============================================================

class TaskStatus(Enum):
    PENDING = "pending"       # 等待执行
    RUNNING = "running"       # 执行中
    COMPLETED = "completed"   # 已完成
    FAILED = "failed"         # 执行失败
    CANCELLED = "cancelled"   # 已取消


@dataclass
class Task:
    """一项可执行任务"""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    title: str = ""                          # 简短标题
    description: str = ""                    # 任务描述（给 Agent 的 prompt）
    status: TaskStatus = TaskStatus.PENDING
    created_at: datetime = field(default_factory=datetime.now)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    assigned_agent: Optional[str] = None     # 执行此任务的 Agent 名称
    result: Optional[str] = None             # 执行结果
    error: Optional[str] = None              # 错误信息
    priority: int = 0                        # 优先级（越大越优先）
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    # ── 执行过程日志 ──
    event_log: list[dict] = field(default_factory=list)  # [{time, event, data}, ...]
    # ── 输出文件追踪 ──
    output_files: list[str] = field(default_factory=list)  # Agent 执行中写入的文件路径列表

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description[:200],
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "assigned_agent": self.assigned_agent,
            "result": (self.result or "")[:500],
            "error": self.error,
            "priority": self.priority,
            "tags": self.tags,
            "event_log": self.event_log[-20:],  # 最近 20 条事件
            "output_files": self.output_files,   # 输出文件列表
            "metadata_": dict(self.metadata),     # 编排信息等
        }

    def add_event(self, event: str, data: Any = None):
        """记录执行过程事件"""
        self.event_log.append({
            "time": datetime.now().isoformat(timespec="seconds"),
            "event": event,
            "data": data,
        })


# ============================================================
# Agent 代理对象（用于任务执行时引用 Agent 实例）
# ============================================================

@dataclass
class AgentProxy:
    """Agent 代理 —— 让任务管理器持有 Agent 引用"""
    name: str
    agent: Any  # Agent 实例（避免循环导入，用 Any）
    status: str = "idle"  # idle / busy
    current_task_id: Optional[str] = None
    # ── 能力描述（用于智能调度）──
    skills: list[str] = field(default_factory=list)      # 技能标签: ["coding", "writing", "research"]
    description: str = ""                                  # 一句话描述

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "current_task_id": self.current_task_id,
            "skills": self.skills,
            "description": self.description,
        }


# ============================================================
# 任务管理器
# ============================================================

class TaskManager:
    """
    任务管理器 —— 全局单例

    用法:
        tm = get_task_manager()
        task_id = tm.publish("帮我分析项目架构", priority=5)
        tm.assign_next(agent_proxy)  # 分配任务给 Agent
    """

    def __init__(self):
        self._queue: deque[Task] = deque()
        self._history: list[Task] = []       # 已完成/失败的任务
        self._agents: dict[str, AgentProxy] = {}
        self._lock = threading.Lock()
        self._callbacks: list[Callable] = []  # 任务状态变更回调
        self._dispatcher_thread: Optional[threading.Thread] = None
        self._dispatcher_running = False
        self._dispatch_interval = 1.0         # 调度间隔（秒）
        self._repo = None                     # 懒加载 TaskRepository
        self._db_enabled = False
        self._main_loop: Optional[_asyncio.AbstractEventLoop] = None  # 主事件循环引用（跨线程 DB 写入用）

    # ======== 数据库持久化 ========

    def _get_repo(self):
        """懒加载 TaskRepository"""
        if self._repo is None:
            try:
                from src.infrastructure.task_repo import get_task_repo
                self._repo = get_task_repo()
                self._db_enabled = self._repo.db_enabled
            except Exception as e:
                logger.warning(f"TaskRepository 初始化失败: {e}")
                self._repo = None
                self._db_enabled = False
        else:
            self._db_enabled = self._repo.db_enabled
        return self._repo

    def _run_async(self, coro):
        """在任意上下文中安全执行异步协程（fire-and-forget 写操作）"""
        try:
            loop = _asyncio.get_running_loop()
            loop.create_task(coro)
        except RuntimeError:
            # 不在 asyncio 线程中 → 投回主事件循环（避免 SQLAlchemy async 跨循环错误）
            main_loop = self._main_loop
            if main_loop is not None and main_loop.is_running():
                _asyncio.run_coroutine_threadsafe(coro, main_loop)
            else:
                _db_executor.submit(self._run_in_thread, coro)

    @staticmethod
    def _run_in_thread(coro):
        """在新线程的事件循环中执行协程"""
        loop = _asyncio.new_event_loop()
        _asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    def _persist_task(self, task: Task):
        """将任务写入数据库（fire-and-forget）"""
        if not self._db_enabled:
            self._get_repo()
        repo = self._repo
        if repo is None or not repo.db_enabled:
            return
        task_dict = task.to_dict()
        # to_dict 截断了 description/result，补全完整字段
        task_dict["description"] = task.description
        task_dict["result"] = task.result
        task_dict["error"] = task.error
        task_dict["tags"] = task.tags
        self._run_async(repo.save_task(task_dict))

    def _persist_events(self, task: Task):
        """将任务事件批量写入数据库"""
        if not self._db_enabled:
            self._get_repo()
        repo = self._repo
        if repo is None or not repo.db_enabled:
            return
        for evt in task.event_log:
            self._run_async(repo.add_event(task.id, evt.get("event", ""), evt.get("data")))

    def load_history_from_db(self):
        """启动时从数据库恢复历史任务到内存"""
        repo = self._get_repo()
        if repo is None or not repo.db_enabled:
            return

        async def _load():
            tasks = await repo.list_tasks(status="", limit=100)
            count = 0
            with self._lock:
                for td in tasks:
                    tid = td.get("id", "")
                    if self._find_task(tid) is not None:
                        continue
                    task = Task(
                        id=tid,
                        title=td.get("title", ""),
                        description=td.get("description", ""),
                        status=TaskStatus(td.get("status", "pending")),
                        created_at=_parse_dt_str(td.get("created_at")) or datetime.now(),
                        started_at=_parse_dt_str(td.get("started_at")),
                        finished_at=_parse_dt_str(td.get("finished_at")),
                        assigned_agent=td.get("assigned_agent"),
                        result=td.get("result"),
                        error=td.get("error"),
                        priority=td.get("priority", 0),
                        tags=td.get("tags", []),
                    )
                    self._history.append(task)
                    count += 1
            if count > 0:
                logger.info(f"从数据库恢复了 {count} 个历史任务")

        try:
            loop = _asyncio.get_running_loop()
            loop.create_task(_load())
        except RuntimeError:
            _asyncio.run(_load())

    # ======== 发布任务 ========

    def publish(
        self,
        description: str,
        title: str = "",
        priority: int = 0,
        tags: list[str] | None = None,
        target_agent: str = "",
    ) -> str:
        """
        发布一项新任务

        Args:
            description: 任务描述（给 Agent 的 prompt）
            title: 简短标题（可选，不填则自动截取）
            priority: 优先级 0-10，越大越优先
            tags: 标签列表
            target_agent: 指定由哪个 Agent 执行（空=自动分配）

        Returns:
            任务 ID
        """
        task = Task(
            title=title or (description[:50] + "..." if len(description) > 50 else description),
            description=description,
            priority=priority,
            tags=tags or [],
            assigned_agent=target_agent or None,
        )
        with self._lock:
            self._queue.append(task)
            # 按优先级排序（大到小）
            self._queue = deque(
                sorted(self._queue, key=lambda t: t.priority, reverse=True)
            )
        # 持久化到数据库
        self._persist_task(task)
        # 触发立即调度
        self._notify_dispatcher()
        return task.id

    # ======== 分配 / 领取任务 ========

    def pending_count(self) -> int:
        """待执行任务数"""
        with self._lock:
            return len(self._queue)

    def pop_next(self) -> Optional[Task]:
        """取出下一个待执行任务"""
        with self._lock:
            if self._queue:
                return self._queue.popleft()
            return None

    def assign_to(self, agent_name: str, task_id: str) -> bool:
        """
        将指定任务分配给 Agent

        Returns:
            True 如果成功分配
        """
        with self._lock:
            agent = self._agents.get(agent_name)
            if not agent or agent.status != "idle":
                return False

            # 在队列和历史中查找
            task = None
            for i, t in enumerate(self._queue):
                if t.id == task_id:
                    task = t
                    del self._queue[i]
                    break
            if task is None:
                for t in self._history:
                    if t.id == task_id and t.status == TaskStatus.PENDING:
                        task = t
                        break

            if task is None:
                return False

            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now()
            task.assigned_agent = agent_name
            agent.status = "busy"
            agent.current_task_id = task.id
            return True

    def assign_next(self, agent_proxy: AgentProxy) -> Optional[Task]:
        """自动分配队列中的下一个任务给 Agent"""
        task = self.pop_next()
        if task is None:
            return None

        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        task.assigned_agent = agent_proxy.name
        agent_proxy.status = "busy"
        agent_proxy.current_task_id = task.id
        with self._lock:
            self._agents[agent_proxy.name] = agent_proxy
        return task

    # ======== 完成任务 ========

    def complete_task(self, task_id: str, result: str, error: str = ""):
        """标记任务完成"""
        with self._lock:
            task = self._find_task(task_id)
            if task is None:
                return

            task.finished_at = datetime.now()
            if error:
                task.status = TaskStatus.FAILED
                task.error = error
            else:
                task.status = TaskStatus.COMPLETED
                task.result = result

            # 释放 Agent
            if task.assigned_agent:
                agent = self._agents.get(task.assigned_agent)
                if agent:
                    agent.status = "idle"
                    agent.current_task_id = None

            if task not in self._history:
                self._history.append(task)

        # 持久化任务 + 事件到数据库
        self._persist_task(task)
        self._persist_events(task)

    def cancel_task(self, task_id: str):
        """取消任务"""
        task = None
        with self._lock:
            task = self._find_task(task_id)
            if task and task.status in (TaskStatus.PENDING, TaskStatus.RUNNING):
                task.status = TaskStatus.CANCELLED
                task.finished_at = datetime.now()
                if task not in self._history:
                    self._history.append(task)

        if task:
            self._persist_task(task)
            self._persist_events(task)

    # ======== 查询 ========

    def get_task(self, task_id: str) -> Optional[dict]:
        """按 ID 查询任务详情"""
        task = self._find_task(task_id)
        return task.to_dict() if task else None

    def list_tasks(self, status: str = "", limit: int = 20) -> list[dict]:
        """
        列出任务

        Args:
            status: 按状态过滤（pending/running/completed/failed/cancelled），空=全部
            limit: 最多返回条数
        """
        with self._lock:
            all_tasks = list(self._queue) + list(self._history)
            if status:
                all_tasks = [t for t in all_tasks if t.status.value == status]

        all_tasks.sort(key=lambda t: t.created_at, reverse=True)
        return [t.to_dict() for t in all_tasks[:limit]]

    def queue_status(self) -> dict:
        """查看队列状态概览"""
        with self._lock:
            pending = sum(1 for t in self._queue)
            running = sum(
                1 for t in list(self._queue) + self._history
                if t.status == TaskStatus.RUNNING
            )
            completed = sum(
                1 for t in self._history
                if t.status == TaskStatus.COMPLETED
            )
            failed = sum(
                1 for t in self._history
                if t.status == TaskStatus.FAILED
            )
        return {
            "pending": pending,
            "running": running,
            "completed": completed,
            "failed": failed,
            "agents": len(self._agents),
            "idle_agents": sum(1 for a in self._agents.values() if a.status == "idle"),
        }

    # ======== Agent 管理 ========

    def register_agent(self, agent_proxy: AgentProxy):
        """注册一个 Agent"""
        with self._lock:
            self._agents[agent_proxy.name] = agent_proxy

    def unregister_agent(self, name: str):
        """注销一个 Agent"""
        with self._lock:
            self._agents.pop(name, None)

    def list_agents(self) -> list[dict]:
        """列出所有已注册 Agent"""
        with self._lock:
            return [a.to_dict() for a in self._agents.values()]

    def list_agents_dict(self) -> dict[str, AgentProxy]:
        """返回 Agent 字典 {name: AgentProxy} （内部用，供编排器访问）"""
        with self._lock:
            return dict(self._agents)

    def record_output_file(self, task_id: str, filepath: str):
        """记录任务执行过程中写入的文件（供 write_file 工具回调）"""
        with self._lock:
            task = self._find_task(task_id)
            if task and filepath not in task.output_files:
                task.output_files.append(filepath)

    # ======== 内部方法 ========

    def _find_task(self, task_id: str) -> Optional[Task]:
        for t in self._queue:
            if t.id == task_id:
                return t
        for t in self._history:
            if t.id == task_id:
                return t
        return None

    # ======== 后台调度器 ========

    def start_dispatcher(self):
        """启动后台任务调度线程，自动分配任务给空闲 Agent"""
        if self._dispatcher_running:
            return
        self._dispatcher_running = True
        self._dispatcher_thread = threading.Thread(
            target=self._dispatch_loop, daemon=True, name="TaskDispatcher"
        )
        self._dispatcher_thread.start()

    def stop_dispatcher(self, timeout: float = 3.0):
        """停止后台调度线程"""
        self._dispatcher_running = False
        if self._dispatcher_thread and self._dispatcher_thread.is_alive():
            self._dispatcher_thread.join(timeout=timeout)

    def _notify_dispatcher(self):
        """发布新任务时触发——调度器会自动响应（无需显式唤醒）"""

    def _dispatch_loop(self):
        """后台调度主循环"""
        import time
        while self._dispatcher_running:
            try:
                self._dispatch_once()
            except Exception as e:
                logger.error(f"调度循环异常: {e}", exc_info=True)
            time.sleep(self._dispatch_interval)

    def _dispatch_once(self):
        """单次调度：智能分配任务给最匹配的空闲 Agent"""
        import time as _time
        with self._lock:
            if not self._queue:
                return
            idle_agents = [
                (name, proxy) for name, proxy in self._agents.items()
                if proxy.status == "idle"
            ]
            if not idle_agents:
                return

            task = self._queue.popleft()

            # ── 智能匹配 ──
            agent_name, agent_proxy = self._smart_assign(task, idle_agents)
            if agent_name is None:
                # 所有 Agent 都不匹配指定要求，放回队列
                self._queue.appendleft(task)
                return

            task.status = TaskStatus.RUNNING
            task.started_at = datetime.now()
            task.assigned_agent = agent_name
            agent_proxy.status = "busy"
            agent_proxy.current_task_id = task.id
            self._history.append(task)  # 加入 history，确保 complete/cancel 能找到

        # 持久化运行状态
        self._persist_task(task)

        # ── 在锁外执行，带事件捕获 ──
        task_id = task.id
        desc = task.description[:60]
        task.add_event("assigned", {"agent": agent_name, "skills": agent_proxy.skills})

        # 打印执行信息
        print(f"\n{'='*55}")
        print(f"  [TaskManager] 分配任务 {task_id}")
        print(f"    任务: {desc}{'...' if len(task.description) > 60 else ''}")
        print(f"    Agent: {agent_name}  |  技能: {', '.join(agent_proxy.skills) or '通用'}")
        print(f"    开始执行...")
        print(f"{'='*55}")

        # 挂载事件监听到 Agent，捕获执行过程
        original_on_event = getattr(agent_proxy.agent, 'on_event', None)

        def event_logger(event, data):
            evt_name = event.value if hasattr(event, 'value') else str(event)
            task.add_event(evt_name, str(data)[:300])

            # 打印关键事件
            from src.core.agent import AgentEvent
            if event == AgentEvent.THINK_START:
                print(f"  [{agent_name}] 🤔 开始思考...")
            elif event == AgentEvent.THINK_END:
                usage = data.get("token_usage", "") if isinstance(data, dict) else ""
                print(f"  [{agent_name}] 💡 思考完成 {usage}")
            elif event == AgentEvent.TOOL_CALL:
                tool_name = data.get("name", "?") if isinstance(data, dict) else "?"
                args_str = str(data.get("arguments", ""))[:80] if isinstance(data, dict) else ""
                print(f"  [{agent_name}] 🔧 调用工具: {tool_name}({args_str})")
            elif event == AgentEvent.TOOL_RESULT:
                tool = data.get("tool", "?") if isinstance(data, dict) else "?"
                success = data.get("success", True) if isinstance(data, dict) else True
                icon = "✅" if success else "❌"
                print(f"  [{agent_name}] {icon} 工具返回: {tool}")
            elif event == AgentEvent.ERROR:
                err = data.get("error", "") if isinstance(data, dict) else str(data)
                print(f"  [{agent_name}] ❌ 错误: {err[:120]}")
            elif event == AgentEvent.PLAN_CREATED:
                plan = data.get("plan", []) if isinstance(data, dict) else []
                print(f"  [{agent_name}] 📋 计划: {' → '.join(plan[:5])}")

            # 同时调用原始回调
            if original_on_event:
                try:
                    original_on_event(event, data)
                except Exception as e:
                    logger.warning(f"任务事件回调失败: {e}")

        # ── 设置当前任务上下文（用于 write_file 自动关联文件）──
        _current_task_id.set(task.id)

        try:
            from src.core.agent import AgentEvent
            agent_proxy.agent.on_event = event_logger
            result = agent_proxy.agent.run(task.description)
            agent_proxy.agent.on_event = original_on_event  # 恢复

            task.add_event("completed", {"result": (result or "")[:200]})
            print(f"  [{agent_name}] ✅ 任务完成 ({len(task.event_log)} 个事件)")
            print(f"{'='*55}\n")
            self.complete_task(task_id, result or "")

        except Exception as e:
            agent_proxy.agent.on_event = original_on_event
            task.add_event("error", {"error": str(e)})
            print(f"  [{agent_name}] ❌ 执行失败: {e}")
            print(f"{'='*55}\n")
            self.complete_task(task_id, "", error=str(e))

        finally:
            _current_task_id.set(None)  # 清除上下文

    # ======== 智能调度引擎 ========

    # 任务类型 → 关键词映射
    TASK_TYPE_KEYWORDS: dict[str, list[str]] = {
        "coding":     ["代码", "编程", "写", "bug", "修复", "重构", "函数", "类", "python",
                       "javascript", "java", "go", "rust", "api", "接口", "算法", "测试"],
        "research":   ["调研", "分析", "研究", "报告", "对比", "总结", "市场", "趋势",
                       "论文", "文献", "review", "调查", "评估"],
        "writing":    ["写文章", "文案", "翻译", "润色", "改写", "摘要", "博客",
                       "文档", "说明", "邮件", "周报", "日报"],
        "data":       ["数据", "统计", "图表", "excel", "csv", "json", "清洗",
                       "可视化", "报表", "sql", "数据库"],
        "file_ops":   ["文件", "读取", "保存", "下载", "目录", "路径", "压缩",
                       "解压", "pdf", "图片", "截图"],
        "shell":      ["命令", "执行", "脚本", "bash", "shell", "cmd", "终端",
                       "运行", "安装", "部署"],
    }

    def _analyze_task_type(self, description: str) -> dict[str, float]:
        """分析任务类型，返回各类型匹配度分数"""
        scores: dict[str, float] = {}
        text = description.lower()
        for task_type, keywords in self.TASK_TYPE_KEYWORDS.items():
            score = 0.0
            for kw in keywords:
                if kw.lower() in text:
                    score += 1.0
            if score > 0:
                scores[task_type] = score
        return scores

    def _estimate_difficulty(self, description: str) -> int:
        """
        估算任务难度 1-10
        依据：描述长度、关键词复杂度、是否多步骤
        """
        score = 1
        text = description.lower()
        # 长度因素
        if len(description) > 200:
            score += 2
        elif len(description) > 100:
            score += 1
        # 复杂度关键词
        complex_kw = ["多步", "复杂", "系统", "架构", "全面", "深入", "优化", "重构", "设计"]
        score += sum(1 for kw in complex_kw if kw in text)
        # 多步骤标记
        if re.search(r'[1-9][、,，.)]', description):
            score += 2  # 带编号的多步骤任务
        if "然后" in text or "接着" in text or "之后" in text:
            score += 1
        return min(score, 10)

    def _smart_assign(
        self, task: Task, idle_agents: list[tuple[str, AgentProxy]]
    ) -> tuple[Optional[str], Optional[AgentProxy]]:
        """
        智能分配：根据任务类型和难度匹配最合适的 Agent

        策略：
          1. 如果任务指定了 target_agent → 直接匹配
          2. 分析任务类型 → 匹配 Agent 技能标签
          3. 估算难度 → 优先分配能力强的 Agent
          4. 没有匹配 → 取第一个空闲 Agent
        """
        if not idle_agents:
            return None, None

        # 用户明确指定了 Agent
        if task.assigned_agent:
            for name, proxy in idle_agents:
                if name == task.assigned_agent:
                    return name, proxy
            # 指定的 Agent 不空闲，等一下
            return None, None

        # 分析任务
        task_types = self._analyze_task_type(task.description)
        difficulty = self._estimate_difficulty(task.description)

        if task_types:
            best_types = sorted(task_types, key=task_types.get, reverse=True)[:3]
            logger.info(
                f"[SmartAssign] 任务 {task.id} 类型: {best_types}, 难度: {difficulty}/10"
            )

        # 评分每个 Agent
        scored: list[tuple[int, str, AgentProxy]] = []
        for name, proxy in idle_agents:
            score = 0
            # 技能匹配加分
            for ttype, tscore in task_types.items():
                if ttype in proxy.skills:
                    score += int(tscore * 3)  # 技能匹配权重高
            # 通用型 Agent（无技能标签）在无明确任务类型时优先
            if not proxy.skills:
                score += 2  # 通用 Agent 基准分，避免被技能型 Agent 抢走不相关的任务
            scored.append((score, name, proxy))

        # 按分数降序排列
        scored.sort(key=lambda x: x[0], reverse=True)
        best_score, best_name, best_proxy = scored[0]

        if task_types and best_score > 0:
            logger.info(
                f"[SmartAssign] → {best_name} (匹配度: {best_score}, 技能: {best_proxy.skills})"
            )
        elif task_types:
            logger.info(f"[SmartAssign] → {best_name} (无技能匹配，默认分配)")

        return best_name, best_proxy


# ============================================================
# 全局单例
# ============================================================

def _parse_dt_str(val: Any) -> Optional[datetime]:
    """将 ISO 字符串或 datetime 转为 datetime"""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    try:
        return datetime.fromisoformat(str(val))
    except (ValueError, TypeError):
        return None

_task_manager: Optional[TaskManager] = None


def get_task_manager() -> TaskManager:
    """获取全局任务管理器单例"""
    global _task_manager
    if _task_manager is None:
        _task_manager = TaskManager()
    return _task_manager
