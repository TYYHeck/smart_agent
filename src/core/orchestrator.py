# -*- coding: utf-8 -*-
"""
多 Agent 编排器 —— 根据任务特征自动选择执行策略

支持四种执行模式:
  SINGLE         — 单 Agent 执行 (默认)
  PARALLEL       — 多 Agent 同时执行，结果汇总
  PIPELINE       — Agent 串行接力，前一个输出 → 后一个输入
  COLLABORATIVE  — Agent 团队讨论，互相审阅后达成共识
  AUTO           — 系统自动分析任务，选择最优模式

模式选择策略:
  - 简单问答、单一操作              → SINGLE
  - "同时/分别/对比/多角度"          → PARALLEL
  - "先...再...然后...最后" 多步骤  → PIPELINE
  - "讨论/辩论/评估/评审/决策"      → COLLABORATIVE
  - 复杂任务无明确标记              → PIPELINE (默认)
"""

from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Callable
from datetime import datetime
import threading
import logging

from .task_manager import TaskManager, Task, AgentProxy, TaskStatus, _current_task_id

logger = logging.getLogger("smart_agent.orchestrator")


# ============================================================
# 执行模式
# ============================================================

class ExecutionMode(Enum):
    SINGLE = "single"              # 单 Agent
    PARALLEL = "parallel"          # 并行执行
    PIPELINE = "pipeline"          # 串行流水线
    COLLABORATIVE = "collaborative" # 协作讨论
    AUTO = "auto"                  # 自动选择


# ============================================================
# 编排结果
# ============================================================

@dataclass
class OrchestrationResult:
    """一次编排执行的完整结果"""
    task_id: str
    mode: ExecutionMode
    mode_reason: str = ""                # 为什么选择这个模式
    agents_used: list[str] = field(default_factory=list)
    final_result: str = ""               # 最终汇总结果
    agent_results: list[dict] = field(default_factory=list)  # 每个 Agent 的子结果
    output_files: list[str] = field(default_factory=list)    # 输出文件列表
    success: bool = True
    error: str = ""
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "mode": self.mode.value,
            "mode_reason": self.mode_reason,
            "agents_used": self.agents_used,
            "final_result": self.final_result[:2000],
            "agent_results": [
                {"agent": r["agent"], "summary": (r.get("result", "") or "")[:500]}
                for r in self.agent_results
            ],
            "output_files": self.output_files,
            "success": self.success,
            "error": self.error,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
        }


# ============================================================
# 模式检测器
# ============================================================

class ModeDetector:
    """分析任务描述，自动判断最适合的执行模式"""

    # 模式 → 触发关键词
    MODE_KEYWORDS: dict[ExecutionMode, list[str]] = {
        ExecutionMode.PARALLEL: [
            "同时", "分别", "并行", "并发",
            "对比", "比较", "多个角度", "不同视角",
            "各方", "各维度", "多角度分析",
            "一边", "cross-check", "多方",
        ],
        ExecutionMode.PIPELINE: [
            "先", "然后", "接着", "之后", "最后",
            "步骤", "第一步", "第二步", "流程",
            "依次", "逐步", "按顺序", "先后",
            "再", "下一步", "紧接着",
        ],
        ExecutionMode.COLLABORATIVE: [
            "讨论", "辩论", "评估", "评审", "审查",
            "决策", "协商", "权衡", "判断", "意见",
            "投票", "推荐方案", "综合考量", "利弊",
            "可行性", "风险分析", "头脑风暴",
            "review", "debate", "decision",
        ],
    }

    # 简单任务特征（直接 SINGLE）
    SIMPLE_PATTERNS: list[str] = [
        "是什么", "是谁", "定义", "解释",
        "翻译", "总结", "概括",
        "你好", "谢谢", "再见",
    ]

    @classmethod
    def detect(cls, description: str) -> tuple[ExecutionMode, str]:
        """
        检测最佳执行模式

        Returns:
            (模式, 选择理由)
        """
        text = description.lower()

        # 1. 超短任务 → SINGLE
        if len(description) < 20:
            return ExecutionMode.SINGLE, "任务简短，单 Agent 即可"

        # 2. 简单问答匹配
        for pattern in cls.SIMPLE_PATTERNS:
            if pattern in text and len(description) < 60:
                return ExecutionMode.SINGLE, f"简单问答 '{pattern}'，单 Agent 处理"

        # 3. 各模式关键词计分
        scores: dict[ExecutionMode, int] = {}
        for mode, keywords in cls.MODE_KEYWORDS.items():
            score = 0
            for kw in keywords:
                if kw.lower() in text:
                    score += 1
            if score > 0:
                scores[mode] = score

        if not scores:
            # 无匹配关键词 → 根据复杂度判断
            if cls._is_complex(description):
                return ExecutionMode.PIPELINE, (
                    "任务较复杂（多步骤/长描述），使用流水线逐步处理"
                )
            return ExecutionMode.SINGLE, "常规任务，单 Agent 执行"

        # 选最高分的模式
        best_mode = max(scores, key=scores.get)
        reasons = {
            ExecutionMode.PARALLEL: f"检测到并行需求信号（得分 {scores[best_mode]}），多 Agent 同时分析",
            ExecutionMode.PIPELINE: f"检测到多步骤流程（得分 {scores[best_mode]}），串行流水线执行",
            ExecutionMode.COLLABORATIVE: f"检测到决策/评估需求（得分 {scores[best_mode]}），团队协作讨论",
        }
        return best_mode, reasons.get(best_mode, "自动选择")

    @staticmethod
    def _is_complex(description: str) -> bool:
        """判断任务是否复杂"""
        text = description.lower()
        # 长度
        if len(description) > 150:
            return True
        # 复杂关键词
        complex_kw = ["分析", "系统", "架构", "设计", "实现", "优化", "重构", "全面"]
        if sum(1 for kw in complex_kw if kw in text) >= 2:
            return True
        # 多步骤标记
        if any(kw in text for kw in ["第一", "第二", "第三", "1.", "2.", "3."]):
            return True
        return False


# ============================================================
# 编排器主类
# ============================================================

class Orchestrator:
    """
    多 Agent 编排器

    用法:
        orch = Orchestrator(get_task_manager())

        # 自动模式选择
        result = orch.execute("帮我分析项目架构并给出优化建议")

        # 手动指定模式
        result = orch.execute("对比 Python 和 Go 的性能", mode=ExecutionMode.PARALLEL)

        # 指定参与 Agent
        result = orch.execute("写一篇技术博客", mode=ExecutionMode.PIPELINE,
                              agent_names=["Researcher", "Writer"])
    """

    def __init__(self, task_manager: TaskManager):
        self.tm = task_manager

    # ======== 入口 ========

    def execute(
        self,
        description: str,
        title: str = "",
        mode: ExecutionMode = ExecutionMode.AUTO,
        agent_names: list[str] | None = None,
        on_progress: Callable[[str, dict], None] | None = None,
    ) -> OrchestrationResult:
        """
        执行任务

        Args:
            description: 任务描述
            title: 标题
            mode: 执行模式 (AUTO 为自动检测)
            agent_names: 指定参与的 Agent 名称列表 (None=自动选择所有空闲)
            on_progress: 进度回调 (stage, info_dict)

        Returns:
            OrchestrationResult
        """
        task = Task(
            title=title or description[:50],
            description=description,
        )

        # 自动模式检测
        mode_reason = ""
        if mode == ExecutionMode.AUTO:
            mode, mode_reason = ModeDetector.detect(description)

        result = OrchestrationResult(
            task_id=task.id,
            mode=mode,
            mode_reason=mode_reason,
            started_at=datetime.now(),
        )

        # 任务元数据（供前端详情展示）
        task.metadata["orchestration_mode"] = mode.value
        task.metadata["orchestration_reason"] = mode_reason
        task.metadata["orchestration_agents"] = agent_names or []

        # 获取可用 Agent
        agents = self._resolve_agents(agent_names)

        if not agents:
            result.success = False
            result.error = "没有可用的 Agent"
            result.finished_at = datetime.now()
            return result

        result.agents_used = [a.name for a in agents]

        # 更新元数据中的实际 Agent 列表
        task.metadata["orchestration_agents"] = result.agents_used

        # 初始化所有子 Agent 状态为 pending
        init_statuses = {name: "pending" for name in result.agents_used}
        task.metadata["orchestration_agent_statuses"] = init_statuses

        # 注册到 TaskManager 历史（让前端任务列表/详情可查看）
        task.status = TaskStatus.RUNNING
        task.started_at = datetime.now()
        with self.tm._lock:
            self.tm._history.append(task)
        self.tm._persist_task(task)

        # 日志
        logger.info(
            f"[Orchestrator] 任务 {task.id} 模式={mode.value} "
            f"原因={mode_reason or '手动指定'} "
            f"Agent={result.agents_used}"
        )

        # ── 设置当前任务上下文（用于 write_file 自动关联文件）──
        prev_task_id = _current_task_id.set(task.id)

        # 分发
        try:
            if mode == ExecutionMode.PARALLEL:
                self._execute_parallel(task, agents, result, on_progress)
            elif mode == ExecutionMode.PIPELINE:
                self._execute_pipeline(task, agents, result, on_progress)
            elif mode == ExecutionMode.COLLABORATIVE:
                self._execute_collaborative(task, agents, result, on_progress)
            else:
                self._execute_single(task, agents[0], result, on_progress)
        except Exception as e:
            logger.error(f"[Orchestrator] 执行失败: {e}")
            result.success = False
            result.error = str(e)
            task.status = TaskStatus.FAILED
            task.error = str(e)
        else:
            # 收集输出文件（write_file 通过 _current_task_id 自动关联了 task）
            result.output_files = list(task.output_files)
            task.status = TaskStatus.COMPLETED
            task.result = result.final_result
        finally:
            task.finished_at = datetime.now()
            # 持久化任务结果
            self.tm._persist_task(task)
            self.tm._persist_events(task)
            _current_task_id.reset(prev_task_id)  # 恢复上下文

        result.finished_at = datetime.now()
        return result

    # ======== 辅助：注入 event_logger 到 Agent ────

    @staticmethod
    def _run_agent_with_logging(agent_proxy: AgentProxy, task: Task, prompt: str) -> str:
        """运行 Agent 并自动捕获事件到 task.event_log"""
        original_on_event = getattr(agent_proxy.agent, 'on_event', None)

        def event_logger(event, data):
            evt_name = event.value if hasattr(event, 'value') else str(event)
            task.add_event(evt_name, str(data)[:300])
            if original_on_event:
                try:
                    original_on_event(event, data)
                except Exception:
                    pass

        try:
            agent_proxy.agent.on_event = event_logger
            output = agent_proxy.agent.run(prompt)
            agent_proxy.agent.on_event = original_on_event
            return output or ""
        except Exception:
            agent_proxy.agent.on_event = original_on_event
            raise

    @staticmethod
    def _update_agent_status(task: Task, agent_name: str, status: str):
        """更新 task.metadata 中的子 Agent 状态"""
        statuses = task.metadata.get("orchestration_agent_statuses", {})
        if isinstance(statuses, dict):
            statuses[agent_name] = status
            task.metadata["orchestration_agent_statuses"] = statuses

    # ======== 单 Agent 执行 ========

    def _execute_single(
        self,
        task: Task,
        agent_proxy: AgentProxy,
        result: OrchestrationResult,
        on_progress: Callable | None,
    ):
        """单 Agent 执行（走原有 TaskManager 流程）"""
        self._emit_progress(on_progress, "single_start", {
            "agent": agent_proxy.name,
            "task": task.description[:100],
        })
        self._update_agent_status(task, agent_proxy.name, "running")

        try:
            output = self._run_agent_with_logging(agent_proxy, task, task.description)
            result.final_result = output or ""
            result.agent_results.append({
                "agent": agent_proxy.name,
                "role": "executor",
                "result": output,
            })
            self._update_agent_status(task, agent_proxy.name, "done")
            self._emit_progress(on_progress, "single_done", {
                "agent": agent_proxy.name,
            })
        except Exception as e:
            self._update_agent_status(task, agent_proxy.name, "failed")
            result.success = False
            result.error = str(e)
            result.final_result = f"[{agent_proxy.name}] 执行失败: {e}"

    # ======== 并行执行 ========

    def _execute_parallel(
        self,
        task: Task,
        agents: list[AgentProxy],
        result: OrchestrationResult,
        on_progress: Callable | None,
    ):
        """
        并行模式:
          1. 所有 Agent 同时执行同一任务
          2. 收集所有结果
          3. 由第一个 Agent 汇总综合
        """
        n = len(agents)
        self._emit_progress(on_progress, "parallel_start", {
            "agent_count": n,
            "agents": [a.name for a in agents],
            "task": task.description[:100],
        })

        # 并行执行
        lock = threading.Lock()
        partial_results: list[dict] = []

        def _run_agent(idx: int, proxy: AgentProxy):
            try:
                self._update_agent_status(task, proxy.name, "running")
                self._emit_progress(on_progress, "agent_start", {
                    "agent": proxy.name, "index": idx + 1, "total": n,
                })
                output = self._run_agent_with_logging(proxy, task, task.description)
                with lock:
                    partial_results.append({
                        "agent": proxy.name,
                        "index": idx,
                        "result": output or "",
                    })
                self._update_agent_status(task, proxy.name, "done")
                self._emit_progress(on_progress, "agent_done", {
                    "agent": proxy.name, "index": idx + 1, "total": n,
                })
            except Exception as e:
                self._update_agent_status(task, proxy.name, "failed")
                with lock:
                    partial_results.append({
                        "agent": proxy.name,
                        "index": idx,
                        "result": f"[错误] {e}",
                        "error": str(e),
                    })
                self._emit_progress(on_progress, "agent_error", {
                    "agent": proxy.name, "error": str(e),
                })

        threads = []
        for i, agent in enumerate(agents):
            t = threading.Thread(target=_run_agent, args=(i, agent), daemon=True)
            threads.append(t)
            t.start()

        for t in threads:
            t.join(timeout=600)  # 10 分钟超时

        # 按索引排序
        partial_results.sort(key=lambda x: x.get("index", 0))
        result.agent_results = partial_results

        # 汇总：让第一个 Agent 综合所有结果
        self._emit_progress(on_progress, "synthesizing", {})
        synthesizer = agents[0]
        synthesis_prompt = self._build_synthesis_prompt(
            task.description, partial_results
        )

        try:
            summary = self._run_agent_with_logging(synthesizer, task, synthesis_prompt)
            result.final_result = summary or ""
        except Exception as e:
            # 汇总失败，手动拼接
            parts = []
            for pr in partial_results:
                parts.append(
                    f"### {pr['agent']}\n{pr.get('result', '')}"
                )
            result.final_result = "\n\n---\n\n".join(parts)

        self._emit_progress(on_progress, "parallel_done", {
            "agent_count": n,
        })

    def _build_synthesis_prompt(self, task: str, results: list[dict]) -> str:
        """构建汇总 prompt"""
        parts = [
            "你是团队的综合分析师。请将以下多个 Agent 对同一任务的分析结果进行综合汇总。\n",
            f"原始任务: {task}\n",
            "各 Agent 的分析结果:\n",
        ]
        for pr in results:
            agent_name = pr.get("agent", "unknown")
            text = pr.get("result", "")[:1000]
            parts.append(f"--- {agent_name} 的观点 ---\n{text}\n")

        parts.append(
            "\n请综合以上所有观点，给出一个完整、无重复、结构清晰的最终答案。"
            "如果各方意见一致，整合强化；如果有分歧，指出差异并给出你的判断。"
            "用中文回答。"
        )
        return "\n".join(parts)

    # ======== 流水线执行 ========

    def _execute_pipeline(
        self,
        task: Task,
        agents: list[AgentProxy],
        result: OrchestrationResult,
        on_progress: Callable | None,
    ):
        """
        流水线模式:
          1. Agent[0] 处理原始任务
          2. Agent[1] 接收 Agent[0] 输出作为输入
          3. ...依次传递
          4. 最后一个 Agent 的输出为最终结果
        """
        n = len(agents)
        self._emit_progress(on_progress, "pipeline_start", {
            "stages": n,
            "agents": [a.name for a in agents],
            "task": task.description[:100],
        })

        current_input = task.description
        stage_results: list[dict] = []

        for i, agent in enumerate(agents):
            role = self._pipe_stage_name(i, n)
            self._emit_progress(on_progress, "pipeline_stage", {
                "stage": i + 1, "total": n,
                "agent": agent.name, "role": role,
            })
            self._update_agent_status(task, agent.name, "running")

            # 构建流水线 prompt
            if i == 0:
                prompt = current_input
            else:
                prompt = (
                    f"你处于处理流水线的第 {i+1}/{n} 阶段，角色是「{role}」。\n\n"
                    f"原始任务: {task.description}\n\n"
                    f"上一阶段 ({agents[i-1].name}) 的输出:\n"
                    f"---\n{current_input}\n---\n\n"
                    f"请基于上一阶段的成果，完成你负责的「{role}」工作。"
                    f"用中文回答。"
                )

            try:
                output = self._run_agent_with_logging(agent, task, prompt)
                stage_results.append({
                    "agent": agent.name,
                    "role": role,
                    "stage": i + 1,
                    "result": output or "",
                })
                current_input = output or ""
                self._update_agent_status(task, agent.name, "done")
                self._emit_progress(on_progress, "pipeline_stage_done", {
                    "stage": i + 1, "total": n,
                    "agent": agent.name,
                })
            except Exception as e:
                self._update_agent_status(task, agent.name, "failed")
                stage_results.append({
                    "agent": agent.name,
                    "role": role,
                    "stage": i + 1,
                    "result": f"[错误] {e}",
                    "error": str(e),
                })
                result.success = False
                result.error = f"流水线第 {i+1} 阶段失败: {e}"
                break

        result.agent_results = stage_results
        result.final_result = current_input
        self._emit_progress(on_progress, "pipeline_done", {"stages": n})

    @staticmethod
    def _pipe_stage_name(index: int, total: int) -> str:
        """流水线阶段命名"""
        if total == 1:
            return "执行"
        if total == 2:
            return ["分析/执行", "总结/输出"][index]
        if total == 3:
            return ["分析拆解", "执行处理", "总结输出"][index]
        names = ["需求分析", "方案设计", "执行实施", "验证检查", "总结输出"]
        if index < len(names):
            return names[index]
        # 更多阶段
        return ["深入执行", "交叉验证", "优化润色", "最终输出"][min(index - 5, 3)]

    # ======== 协作讨论 ========

    def _execute_collaborative(
        self,
        task: Task,
        agents: list[AgentProxy],
        result: OrchestrationResult,
        on_progress: Callable | None,
    ):
        """
        协作讨论模式:
          1. 所有 Agent 各自分析并提出观点
          2. 收集所有观点，发给每个人审阅
          3. 各 Agent 修改自己的观点
          4. 最终综合得出结论
        """
        n = len(agents)
        self._emit_progress(on_progress, "collab_start", {
            "members": n,
            "agents": [a.name for a in agents],
            "task": task.description[:100],
        })

        # Round 1: 独立分析
        self._emit_progress(on_progress, "collab_round1", {"round": 1})
        initial_opinions: list[dict] = []

        for agent in agents:
            self._update_agent_status(task, agent.name, "running")
            prompt = (
                f"团队正在讨论以下问题，你是团队成员「{agent.name}」。\n\n"
                f"问题: {task.description}\n\n"
                f"请从你的专业角度给出分析和建议。"
                f"如果你能发现其他人可能忽略的角度，请指出。"
                f"用中文回答。"
            )
            try:
                opinion = self._run_agent_with_logging(agent, task, prompt)
                initial_opinions.append({
                    "agent": agent.name,
                    "opinion": opinion or "",
                    "round": 1,
                })
                self._update_agent_status(task, agent.name, "done")
            except Exception as e:
                self._update_agent_status(task, agent.name, "failed")
                initial_opinions.append({
                    "agent": agent.name,
                    "opinion": f"[错误] {e}",
                    "error": str(e),
                    "round": 1,
                })

        # Round 2: 互审 + 修订
        self._emit_progress(on_progress, "collab_round2", {"round": 2})
        revised_opinions: list[dict] = []

        for agent in agents:
            # 收集其他人的观点（排除自己）
            others = "\n\n".join([
                f"【{o['agent']}的观点】\n{o.get('opinion', '')[:800]}"
                for o in initial_opinions
                if o["agent"] != agent.name
            ])

            prompt = (
                f"你之前对以下问题的观点已经提交。现在请审阅团队其他成员的观点：\n\n"
                f"原始问题: {task.description}\n\n"
                f"你之前的观点:\n{self._find_opinion(initial_opinions, agent.name)[:500]}\n\n"
                f"其他成员的观点:\n{others}\n\n"
                f"请基于团队讨论，给出你最终的、更完善的观点。"
                f"如果同意他人的某些观点，可以直接整合；"
                f"如果不同意，请说明理由。用中文回答。"
            )
            try:
                revised = self._run_agent_with_logging(agent, task, prompt)
                revised_opinions.append({
                    "agent": agent.name,
                    "opinion": revised or "",
                    "round": 2,
                })
            except Exception as e:
                revised_opinions.append({
                    "agent": agent.name,
                    "opinion": self._find_opinion(initial_opinions, agent.name),
                    "error": str(e),
                    "round": 2,
                })

        # 综合
        self._emit_progress(on_progress, "collab_synthesizing", {})
        all_opinions = initial_opinions + revised_opinions
        synthesis_prompt = self._build_collab_synthesis_prompt(
            task.description, all_opinions
        )

        try:
            final = self._run_agent_with_logging(agents[0], task, synthesis_prompt)
            result.final_result = final or ""
        except Exception as e:
            result.final_result = "团队讨论因技术原因中断，各成员观点如下:\n\n" + "\n\n---\n\n".join([
                f"### {o['agent']}\n{o.get('opinion', '')[:1000]}"
                for o in revised_opinions
            ])

        result.agent_results = [
            {"agent": r["agent"], "round": r["round"], "result": r.get("opinion", "")}
            for r in (initial_opinions + revised_opinions)
        ]
        self._emit_progress(on_progress, "collab_done", {})

    @staticmethod
    def _find_opinion(opinions: list[dict], agent_name: str) -> str:
        for o in opinions:
            if o.get("agent") == agent_name:
                return o.get("opinion", "")
        return ""

    def _build_collab_synthesis_prompt(self, task: str, opinions: list[dict]) -> str:
        parts = [
            "你是团队的主持人。请综合所有团队成员的讨论结果，给出最终结论。\n",
            f"讨论主题: {task}\n",
            "讨论记录:\n",
        ]
        for o in opinions:
            parts.append(
                f"### {o['agent']} (第{o.get('round', '?')}轮)\n"
                f"{o.get('opinion', '')[:800]}\n"
            )
        parts.append(
            "\n请给出:\n"
            "1. 团队共识\n"
            "2. 存在的分歧及原因\n"
            "3. 最终建议方案\n"
            "用中文回答。"
        )
        return "\n".join(parts)

    # ======== 辅助方法 ========

    def _resolve_agents(self, agent_names: list[str] | None) -> list[AgentProxy]:
        """解析可用 Agent 列表"""
        all_agents = self.tm.list_agents_dict()

        if agent_names:
            # 按指定名称获取
            result = []
            for name in agent_names:
                if name in all_agents:
                    result.append(all_agents[name])
            return result

        # 获取所有空闲 Agent
        idle = [a for a in all_agents.values() if a.status == "idle"]
        if idle:
            return idle

        # 都没空闲，取全部
        return list(all_agents.values())

    def _emit_progress(self, callback: Callable | None, stage: str, info: dict):
        if callback:
            try:
                callback(stage, info)
            except Exception:
                pass


# ============================================================
# 添加到 TaskManager
# ============================================================

def patch_task_manager(tm: TaskManager) -> TaskManager:
    """
    为 TaskManager 添加编排相关方法
    （不修改原有 task_manager.py，用 monkey-patch 方式扩展）
    """
    orch = Orchestrator(tm)

    def list_agents_dict(self) -> dict[str, AgentProxy]:
        """返回 Agent 字典（内部方法，给 Orchestrator 用）"""
        with self._lock:
            return dict(self._agents)

    def execute_orchestrated(
        self,
        description: str,
        title: str = "",
        mode: str = "auto",
        agent_names: list[str] | None = None,
        on_progress: Callable | None = None,
    ) -> OrchestrationResult:
        """
        编排执行任务

        Args:
            description: 任务描述
            title: 标题
            mode: "single" | "parallel" | "pipeline" | "collaborative" | "auto"
            agent_names: 指定 Agent 列表
            on_progress: 进度回调

        Returns:
            OrchestrationResult
        """
        exec_mode = ExecutionMode(mode)
        return orch.execute(
            description=description,
            title=title,
            mode=exec_mode,
            agent_names=agent_names,
            on_progress=on_progress,
        )

    def detect_best_mode(self, description: str) -> dict:
        """检测最适合的执行模式（供 UI 展示用）"""
        mode, reason = ModeDetector.detect(description)
        return {
            "mode": mode.value,
            "reason": reason,
        }

    # Monkey-patch
    tm.list_agents_dict = list_agents_dict.__get__(tm, TaskManager)
    tm.execute_orchestrated = execute_orchestrated.__get__(tm, TaskManager)
    tm.detect_best_mode = detect_best_mode.__get__(tm, TaskManager)

    return tm
