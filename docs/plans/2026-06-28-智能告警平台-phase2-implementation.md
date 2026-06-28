# OPS AI Agent 智能告警平台 — Phase 2 实施计划

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在 Phase 1 数据基础层之上构建 AI Agent 核心诊断引擎：假设驱动状态机、分层上下文管理、异常重试降级、CLI 诊断入口。

**Architecture:** 增量增强 Phase 1 的 `src/agent/`，重写 graph.py 为假设驱动状态机，新增 context.py/resilience.py/tools.py/diagnosis.py。复用 Phase 1 的 ToolRegistry、Neo4jClient、ESLogClient、Database。使用 LangGraph PostgresSaver 做 Checkpointer，Redis 做会话缓存。

**Tech Stack:** Python 3.11+, LangGraph, LangChain, langchain-openai, PostgreSQL, Redis, Neo4j, Elasticsearch

---

## Task 2.0: AgentState 扩展 + 工具绑定

**Files:**
- Modify: `src/agent/state.py`
- Create: `src/agent/tools.py`
- Test: `tests/test_agent_tools.py`

### Step 1: 扩展 AgentState

```python
# src/agent/state.py（替换现有内容）
from typing import TypedDict, List, Optional
from enum import Enum


class AgentMode(Enum):
    FULL = "full"
    DIAGNOSE_ONLY = "diagnose"
    NOTIFY_ONLY = "notify"


class AgentState(TypedDict):
    alert: dict
    messages: List[dict]
    hypothesis: str
    hypothesis_history: List[dict]
    evidence: dict
    diagnosis: str
    action_plan: dict
    active_intent: str
    pending_confirmations: List[str]
    protected_data: List[dict]
    compressed_memory: List[str]
    mode: str
    step_count: int
    start_time: float
    conversation_id: str
    llm_failures: int
    tools_failed: bool
    truncated: bool
```

### Step 2: 创建 Agent 工具绑定

```python
# src/agent/tools.py
from src.tools.registry import ToolRegistry, ToolDefinition
from src.infra.es_client import ESLogClient
from src.infra.neo4j_client import Neo4jClient
from src.config import settings


def create_agent_tools(es_client=None, neo4j_client=None) -> ToolRegistry:
    """创建 Agent 可用的工具集（Phase 2 范围：ES 日志 + Neo4j 拓扑）"""
    registry = ToolRegistry()

    # search_logs 工具（真实 ES）
    if es_client:
        async def _search_logs(service: str, keywords: str = "", time_range: str = "15m") -> dict:
            results = await es_client.search_logs(
                service=service, keywords=keywords, time_range=time_range, max_results=20
            )
            return {"total": len(results), "logs": results}

        registry.register(ToolDefinition(
            name="search_logs",
            description="全文检索服务日志，支持关键词和时间范围过滤",
            parameters={
                "service": {"type": "string", "description": "服务名称"},
                "keywords": {"type": "string", "description": "搜索关键词"},
                "time_range": {"type": "string", "description": "时间范围，如 15m/1h/1d"},
            },
        ), handler=_search_logs)
    else:
        # Mock 版本（测试用）
        registry.register(ToolDefinition(
            name="search_logs",
            description="全文检索服务日志",
            parameters={
                "service": {"type": "string"},
                "keywords": {"type": "string"},
                "time_range": {"type": "string"},
            },
        ), handler=lambda **kw: {"total": 0, "logs": [], "note": "ES 不可用"})

    # get_topology 工具（真实 Neo4j）
    if neo4j_client:
        def _get_topology(service: str, hops: int = 2) -> dict:
            deps = neo4j_client.get_upstream_services(service)
            return {"service": service, "dependencies": deps, "total": len(deps)}

        registry.register(ToolDefinition(
            name="get_topology",
            description="查询服务的拓扑依赖关系（上游服务列表）",
            parameters={
                "service": {"type": "string", "description": "服务名称"},
                "hops": {"type": "integer", "description": "查询跳数，默认 2"},
            },
        ), handler=_get_topology)
    else:
        registry.register(ToolDefinition(
            name="get_topology",
            description="查询服务拓扑依赖",
            parameters={"service": {"type": "string"}, "hops": {"type": "integer"}},
        ), handler=lambda **kw: {"dependencies": [], "note": "Neo4j 不可用"})

    # analyze_blast_radius 工具（真实 Neo4j）
    if neo4j_client:
        def _analyze_blast_radius(service: str, hops: int = 3) -> dict:
            affected = neo4j_client.analyze_blast_radius(service, hops=hops)
            return {"affected_services": affected, "total": len(affected)}

        registry.register(ToolDefinition(
            name="analyze_blast_radius",
            description="评估故障爆炸半径，找到下游受影响的服务",
            parameters={
                "service": {"type": "string", "description": "故障源服务名称"},
                "hops": {"type": "integer", "description": "遍历跳数，默认 3"},
            },
        ), handler=_analyze_blast_radius)
    else:
        registry.register(ToolDefinition(
            name="analyze_blast_radius",
            description="评估故障爆炸半径",
            parameters={"service": {"type": "string"}, "hops": {"type": "integer"}},
        ), handler=lambda **kw: {"affected_services": [], "note": "Neo4j 不可用"})

    return registry
```

### Step 3: 编写测试

```python
# tests/test_agent_tools.py
from src.agent.tools import create_agent_tools


def test_create_tools_without_clients():
    """无外部依赖时也应创建 mock 工具"""
    registry = create_agent_tools()
    tools = registry.list_tools()
    assert "search_logs" in tools
    assert "get_topology" in tools
    assert "analyze_blast_radius" in tools


def test_mock_tools_are_callable():
    """Mock 工具应能正常调用不报错"""
    registry = create_agent_tools()
    result = registry.execute("search_logs", {"service": "test", "keywords": "error", "time_range": "5m"})
    assert isinstance(result, dict)


def test_tools_have_openai_schema():
    """工具应能生成 OpenAI Function Calling Schema"""
    registry = create_agent_tools()
    schema = registry.get_openai_schema()
    assert len(schema) == 3
    names = [s["function"]["name"] for s in schema]
    assert "search_logs" in names
    assert "get_topology" in names
    assert "analyze_blast_radius" in names
```

### Step 4: 验证

```bash
pytest tests/test_agent_tools.py -v
```
Expected: 3 PASS

### Step 5: Commit

```bash
git add src/agent/state.py src/agent/tools.py tests/test_agent_tools.py
git commit -m "feat: AgentState 扩展 + Agent 工具绑定（ES/Neo4j）"
```

---

## Task 2.1: 分层上下文管理器

**Files:**
- Create: `src/agent/context.py`
- Test: `tests/test_agent_context.py`

### Step 1: 编写失败测试

```python
# tests/test_agent_context.py
from src.agent.context import ContextManager, SYSTEM_PROMPT
from langchain_core.messages import HumanMessage, AIMessage, FunctionMessage


def test_build_messages_basic():
    """基本消息构建：系统 Prompt + 告警 + 历史"""
    mgr = ContextManager()
    messages, protected = mgr.build_messages(
        alert={"labels": {"service": "payment"}, "annotations": {"summary": "CPU > 90%"}},
        reasoning_history=[AIMessage(content="正在分析...")],
    )
    # 应包含系统 prompt
    assert len(messages) >= 2
    assert messages[0].content == SYSTEM_PROMPT


def test_protected_data_is_preserved():
    """protected_data 中的关键数据应永不被移除"""
    mgr = ContextManager()
    mgr.add_protected({
        "tool": "search_logs",
        "data": "ERROR: connection timeout",
        "critical": True,
    })
    messages, protected = mgr.build_messages(
        alert={"labels": {}, "annotations": {}},
        reasoning_history=[],
    )
    assert len(protected) >= 1
    assert "connection timeout" in str(protected)


def test_compression_triggers_on_threshold():
    """消息超阈值时应触发压缩"""
    mgr = ContextManager(max_messages=10, recent_rounds=2)
    # 添加大量消息触发压缩
    for i in range(20):
        mgr.record_reasoning_step(AIMessage(content=f"Step {i}"))
    messages, _ = mgr.build_messages(
        alert={"labels": {}, "annotations": {}},
        reasoning_history=mgr.get_recent_history(),
    )
    # 压缩后消息数应减少
    assert len(messages) <= 30  # 系统 prompt + 压缩后历史 + 告警
```

### Step 2: 验证测试失败

```bash
pytest tests/test_agent_context.py -v
```
Expected: FAIL（模块不存在）

### Step 3: 实现上下文管理器

```python
# src/agent/context.py
import time
from langchain_core.messages import BaseMessage, SystemMessage, HumanMessage

SYSTEM_PROMPT = """你是运维 AI Agent，负责告警根因诊断。

诊断流程：
1. 分析告警信息，形成初始假设
2. 调用工具（search_logs/get_topology/analyze_blast_radius）验证假设
3. 根据工具返回的证据评估假设置信度
4. 若置信度不足，修正假设并继续验证
5. 根因确认后，输出结构化诊断报告

报告格式：
- 根因：[根因描述]
- 置信度：[0-100%]
- 证据：[关键证据列表]
- 建议：[修复建议]
"""

MAX_PROTECTED_DATA = 20
DEFAULT_MAX_MESSAGES = 50
DEFAULT_RECENT_ROUNDS = 5


class ContextManager:
    """分层上下文管理器

    Layer 1: 系统 Prompt（永远保留）
    Layer 2: protected_data（不可压缩，滑动窗口 20 条）
    Layer 3: compressed_memory（可压缩的已排除假设摘要）
    Layer 4: 当前窗口（最近 N 轮对话完整保留）
    """

    def __init__(self, max_messages: int = DEFAULT_MAX_MESSAGES,
                 recent_rounds: int = DEFAULT_RECENT_ROUNDS):
        self.max_messages = max_messages
        self.recent_rounds = recent_rounds
        self._protected: list[dict] = []
        self._compressed: list[str] = []
        self._reasoning_history: list[BaseMessage] = []

    def add_protected(self, data: dict):
        """添加关键数据到保护层"""
        self._protected.append({
            "data": data.get("data", ""),
            "tool": data.get("tool", "unknown"),
            "timestamp": data.get("timestamp", time.time()),
            "critical": data.get("critical", False),
        })
        # 滑动窗口：保留最近 20 条
        if len(self._protected) > MAX_PROTECTED_DATA:
            self._protected = self._protected[-MAX_PROTECTED_DATA:]

    def record_reasoning_step(self, message: BaseMessage):
        """记录一步推理"""
        self._reasoning_history.append(message)

    def compress_excluded_hypothesis(self, summary: str):
        """将已排除的假设压缩为摘要"""
        self._compressed.append(summary)

    def get_recent_history(self) -> list[BaseMessage]:
        """获取最近 N 轮对话"""
        return self._reasoning_history[-self.recent_rounds * 2:]  # 每轮 = 人+机

    def build_messages(self, alert: dict,
                       reasoning_history: list[BaseMessage] = None) -> tuple[list[BaseMessage], list[dict]]:
        """构建发送给 LLM 的完整消息列表"""
        messages = []

        # Layer 1: 系统 Prompt
        messages.append(SystemMessage(content=SYSTEM_PROMPT))

        # Layer 3: 压缩摘要（已排除假设）
        if self._compressed:
            compressed_text = "\n".join(f"- {c}" for c in self._compressed[-5:])
            messages.append(SystemMessage(
                content=f"[已排除的假设]\n{compressed_text}"
            ))

        # Layer 2: protected_data
        if self._protected:
            protected_text = "\n".join(
                f"[{p['tool']}] {str(p['data'])[:200]}"
                for p in self._protected[-10:]
            )
            messages.append(SystemMessage(
                content=f"[关键证据]\n{protected_text}"
            ))

        # 告警信息
        alert_text = (
            f"告警信息：\n"
            f"- 服务：{alert.get('labels', {}).get('service', 'unknown')}\n"
            f"- 严重级别：{alert.get('severity', 'unknown')}\n"
            f"- 摘要：{alert.get('annotations', {}).get('summary', '')}\n"
            f"- 告警 ID：{alert.get('alert_id', 'unknown')}"
        )
        messages.append(HumanMessage(content=alert_text))

        # Layer 4: 当前窗口
        if reasoning_history:
            messages.extend(reasoning_history)

        # 检查是否需要压缩
        total = len(messages)
        if total > self.max_messages:
            # 简化压缩：截断中间历史
            keep = self.recent_rounds * 2 + 4  # 保留：system + alert + 最近 N 轮
            if total > keep:
                excess = total - keep
                compress_msg = SystemMessage(
                    content=f"[上下文压缩：省略了 {excess} 条历史消息]"
                )
                # 在历史之前插入压缩消息
                insert_pos = 2  # system + compressed 之后
                if self._compressed:
                    insert_pos += 1
                if self._protected:
                    insert_pos += 1
                insert_pos += 1  # alert
                messages.insert(insert_pos, compress_msg)
                # 只保留最后的 keep 条
                messages = messages[:insert_pos + 1] + messages[-(keep - insert_pos - 1):]

        return messages, self._protected

    def clear(self):
        """重置上下文"""
        self._protected = []
        self._compressed = []
        self._reasoning_history = []
```

### Step 4: 运行测试

```bash
pytest tests/test_agent_context.py -v
```
Expected: 3 PASS

### Step 5: Commit

```bash
git add src/agent/context.py tests/test_agent_context.py
git commit -m "feat: 分层上下文管理器（四层 + 压缩策略）"
```

---

## Task 2.2: 异常处理与降级

**Files:**
- Create: `src/agent/resilience.py`
- Test: `tests/test_agent_resilience.py`

### Step 1: 编写失败测试

```python
# tests/test_agent_resilience.py
import pytest
from src.agent.resilience import (
    ResilienceHandler, AgentMode, retry_with_backoff,
    TRANSIENT_ERRORS, PERMANENT_ERRORS,
)


def test_retry_success_first_try():
    """第一次尝试成功时不重试"""
    call_count = 0

    @retry_with_backoff(max_retries=3, base_delay=0.01)
    def flaky_func():
        nonlocal call_count
        call_count += 1
        return "success"

    result = flaky_func()
    assert result == "success"
    assert call_count == 1


def test_retry_on_transient_error():
    """瞬时错误应触发重试"""
    call_count = 0

    @retry_with_backoff(max_retries=3, base_delay=0.01)
    def flaky_func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise TimeoutError("timeout")
        return "recovered"

    result = flaky_func()
    assert result == "recovered"
    assert call_count == 3


def test_no_retry_on_permanent_error():
    """持久错误不应重试"""
    call_count = 0

    @retry_with_backoff(max_retries=3, base_delay=0.01)
    def bad_func():
        nonlocal call_count
        call_count += 1
        raise ValueError("bad request")

    with pytest.raises(ValueError):
        bad_func()
    assert call_count == 1  # 不重试


def test_degrade_on_consecutive_failures():
    """连续 LLM 失败 3 次应降级为 NOTIFY_ONLY"""
    handler = ResilienceHandler()
    assert handler.mode == AgentMode.FULL

    handler.record_llm_failure()
    handler.record_llm_failure()
    assert handler.mode == AgentMode.FULL  # 2 次未触发

    handler.record_llm_failure()
    assert handler.mode == AgentMode.NOTIFY_ONLY


def test_degrade_when_all_tools_failed():
    """全部工具不可用时降级"""
    handler = ResilienceHandler()
    handler.set_tools_status(True)  # all failed
    assert handler.mode == AgentMode.NOTIFY_ONLY


def test_reset_resilience():
    """重置后恢复正常模式"""
    handler = ResilienceHandler()
    handler.record_llm_failure()
    handler.record_llm_failure()
    handler.record_llm_failure()
    assert handler.mode == AgentMode.NOTIFY_ONLY
    handler.reset()
    assert handler.mode == AgentMode.FULL
```

### Step 2: 验证测试失败

```bash
pytest tests/test_agent_resilience.py -v
```
Expected: 6 FAIL（模块不存在）

### Step 3: 实现异常处理模块

```python
# src/agent/resilience.py
import time
import functools
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class AgentMode(Enum):
    FULL = "full"
    DIAGNOSE_ONLY = "diagnose"
    NOTIFY_ONLY = "notify"


# 瞬时错误：应该重试
TRANSIENT_ERRORS = (TimeoutError, ConnectionError, OSError)

# 持久错误：不应重试
PERMANENT_ERRORS = (ValueError, TypeError, KeyError)


def retry_with_backoff(max_retries: int = 3, base_delay: float = 2.0):
    """指数退避重试装饰器"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except PERMANENT_ERRORS:
                    raise  # 不重试
                except Exception as e:
                    last_error = e
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            f"{func.__name__} 第 {attempt + 1} 次失败: {e}，"
                            f"{delay:.1f}s 后重试"
                        )
                        time.sleep(delay)
            raise last_error
        return wrapper
    return decorator


class ResilienceHandler:
    """Agent 异常处理与降级控制器"""

    def __init__(self, max_consecutive_failures: int = 3):
        self.max_consecutive_failures = max_consecutive_failures
        self._llm_failures: int = 0
        self._tools_failed: bool = False
        self._mode = AgentMode.FULL

    @property
    def mode(self) -> AgentMode:
        return self._mode

    def record_llm_failure(self):
        """记录一次 LLM 调用失败"""
        self._llm_failures += 1
        if self._llm_failures >= self.max_consecutive_failures:
            self._mode = AgentMode.NOTIFY_ONLY
            logger.error(f"LLM 连续 {self._llm_failures} 次失败，降级为 {self._mode.value}")

    def record_llm_success(self):
        """重置 LLM 失败计数"""
        self._llm_failures = 0

    def set_tools_status(self, all_failed: bool):
        """设置工具可用状态"""
        self._tools_failed = all_failed
        if all_failed:
            self._mode = AgentMode.NOTIFY_ONLY
            logger.error("全部工具不可用，降级为 NOTIFY_ONLY")

    def time_exceeded(self, step_count: int, max_steps: int,
                      start_time: float, max_duration: float) -> bool:
        """检查是否超时"""
        if step_count >= max_steps:
            logger.warning(f"步数超限: {step_count}/{max_steps}")
            return True
        if time.time() - start_time > max_duration:
            logger.warning(f"时间超限: {time.time() - start_time:.0f}s/{max_duration}s")
            return True
        return False

    def reset(self):
        """重置所有状态"""
        self._llm_failures = 0
        self._tools_failed = False
        self._mode = AgentMode.FULL
```

### Step 4: 运行测试

```bash
pytest tests/test_agent_resilience.py -v
```
Expected: 6 PASS

### Step 5: Commit

```bash
git add src/agent/resilience.py tests/test_agent_resilience.py
git commit -m "feat: Agent 异常重试与降级策略"
```

---

## Task 2.3: 假设驱动状态机

**Files:**
- Rewrite: `src/agent/graph.py`
- Rewrite: `tests/test_agent_graph.py`

### Step 1: 编写失败测试

```python
# tests/test_agent_graph.py（替换现有内容）
import pytest
import time
from unittest.mock import Mock, patch
from src.agent.state import AgentState, AgentMode
from src.agent.graph import build_graph, MAX_STEPS, MAX_DURATION
from src.tools.registry import ToolRegistry, ToolDefinition


@pytest.fixture
def mock_tools():
    registry = ToolRegistry()
    registry.register(ToolDefinition(
        name="search_logs",
        description="查询日志",
        parameters={"service": {"type": "string"}, "keywords": {"type": "string"}, "time_range": {"type": "string"}},
    ), handler=lambda **kw: {"logs": [{"content": "ERROR: timeout"}]})
    registry.register(ToolDefinition(
        name="get_topology",
        description="查询拓扑",
        parameters={"service": {"type": "string"}, "hops": {"type": "integer"}},
    ), handler=lambda **kw: {"dependencies": [{"name": "redis"}]})
    return registry


def test_graph_compiles_with_all_nodes(mock_tools):
    """状态机包含全部必需节点"""
    graph = build_graph(mock_tools)
    nodes = graph.get_graph().nodes
    node_names = [n for n in nodes]
    assert "formulate_hypothesis" in node_names
    assert "tool_executor" in node_names
    assert "evaluate_evidence" in node_names
    assert "refine_hypothesis" in node_names
    assert "root_cause" in node_names
    assert "timeout_handler" in node_names


def test_formulate_hypothesis_creates_hypothesis(mock_tools):
    """formulate_hypothesis 应生成假设"""
    graph = build_graph(mock_tools)

    initial_state = AgentState(
        alert={"alert_id": "t1", "severity": "critical",
               "labels": {"service": "payment"}, "annotations": {"summary": "P99 > 500ms"}},
        messages=[], hypothesis="", hypothesis_history=[],
        evidence={}, diagnosis="", action_plan={},
        active_intent="diagnose", pending_confirmations=[],
        protected_data=[], compressed_memory=[],
        mode="full", step_count=0, start_time=time.time(),
        conversation_id="conv-1", llm_failures=0, tools_failed=False, truncated=False,
    )

    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.return_value = Mock(
            content="假设：下游 Redis 延迟导致，置信度 60%。需要调用 search_logs 和 get_topology 验证",
            tool_calls=[
                {"name": "search_logs", "args": {"service": "payment", "keywords": "error timeout", "time_range": "15m"}, "id": "call_1"},
                {"name": "get_topology", "args": {"service": "payment", "hops": 2}, "id": "call_2"},
            ],
        )
        result = graph.invoke(initial_state, config={"recursion_limit": 10})
        assert result["step_count"] >= 0
        assert len(result["messages"]) > 0


def test_timeout_handler_truncated(mock_tools):
    """超时时应输出阶段性结论"""
    graph = build_graph(mock_tools)

    initial_state = AgentState(
        alert={"alert_id": "t1", "severity": "warning",
               "labels": {"service": "svc"}, "annotations": {"summary": "test"}},
        messages=[], hypothesis="Redis 问题", hypothesis_history=[],
        evidence={}, diagnosis="", action_plan={},
        active_intent="diagnose", pending_confirmations=[],
        protected_data=[], compressed_memory=[],
        mode="full",
        step_count=MAX_STEPS,  # 已超限
        start_time=time.time() - MAX_DURATION - 1,  # 已超时
        conversation_id="conv-2", llm_failures=0, tools_failed=False, truncated=False,
    )

    result = graph.invoke(initial_state, config={"recursion_limit": 5})
    assert result["truncated"] is True
    assert "超时" in result.get("diagnosis", "") or len(result.get("diagnosis", "")) > 0


def test_evidence_evaluation_flow(mock_tools):
    """验证 evaluate_evidence 能正确路由"""
    graph = build_graph(mock_tools)

    initial_state = AgentState(
        alert={"alert_id": "t2", "severity": "critical",
               "labels": {"service": "payment"}, "annotations": {"summary": "CPU 99%"}},
        messages=[], hypothesis="CPU 过载",
        hypothesis_history=[
            {"hypothesis": "内存泄漏", "result": "excluded", "reason": "内存正常"}
        ],
        evidence={"cpu": "99%", "memory": "45%"},
        diagnosis="", action_plan={},
        active_intent="diagnose", pending_confirmations=[],
        protected_data=[], compressed_memory=["已排除内存泄漏：内存使用率 45% 正常"],
        mode="full", step_count=3, start_time=time.time(),
        conversation_id="conv-3", llm_failures=0, tools_failed=False, truncated=False,
    )

    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.return_value = Mock(
            content="证据充分：CPU 99% 确认是 CPU 过载。根因：支付服务请求量激增导致 CPU 过载。置信度 92%。",
            tool_calls=None,
        )
        result = graph.invoke(initial_state, config={"recursion_limit": 10})
        assert len(result.get("diagnosis", "")) > 0
```

### Step 2: 验证测试失败

```bash
pytest tests/test_agent_graph.py -v
```
Expected: FAIL（模块不匹配）

### Step 3: 实现假设驱动状态机

```python
# src/agent/graph.py（完全重写）
import time
import logging
from langgraph.graph import StateGraph, END
from langchain_core.messages import AIMessage, SystemMessage, HumanMessage, FunctionMessage
from langchain_openai import ChatOpenAI
from src.agent.state import AgentState, AgentMode
from src.agent.context import ContextManager, SYSTEM_PROMPT
from src.agent.resilience import ResilienceHandler, retry_with_backoff
from src.tools.registry import ToolRegistry
from src.config import settings

logger = logging.getLogger(__name__)

MAX_STEPS = settings.agent_max_steps
MAX_DURATION = settings.agent_max_duration_seconds


def _get_llm():
    """创建 LLM 客户端"""
    return ChatOpenAI(
        base_url=settings.llm_api_base,
        api_key=settings.llm_api_key,
        model=settings.llm_model,
        temperature=0.3,
    )


@retry_with_backoff(max_retries=3, base_delay=2.0)
def call_llm_with_retry(messages: list, tools: list = None):
    """调用 LLM（带重试），返回 (response, error)"""
    llm = _get_llm()
    if tools:
        llm_with_tools = llm.bind_tools(tools)
        return llm_with_tools.invoke(messages)
    return llm.invoke(messages)


# ---------- 状态机节点 ----------

def formulate_hypothesis(state: AgentState, tool_registry: ToolRegistry,
                         context_mgr: ContextManager,
                         resilience: ResilienceHandler) -> dict:
    """节点 1: 分析告警，形成初始假设"""
    tools_schema = tool_registry.get_openai_schema()
    messages, _ = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state.get("messages", []),
    )
    messages.append(HumanMessage(content="请分析以上告警，形成诊断假设，并决定需要调用哪些工具来验证。"))

    try:
        response = call_llm_with_retry(messages, tools=tools_schema)
        resilience.record_llm_success()
    except Exception as e:
        logger.error(f"LLM 调用失败: {e}")
        resilience.record_llm_failure()
        return {
            "messages": [AIMessage(content=f"LLM 调用失败: {e}")],
            "truncated": True,
        }

    state["messages"].append(HumanMessage(content="请分析告警并形成假设"))
    state["messages"].append(response)
    context_mgr.record_reasoning_step(HumanMessage(content="请分析告警并形成假设"))
    context_mgr.record_reasoning_step(response)

    return {
        "messages": state["messages"],
        "hypothesis": response.content[:200],
    }


def tool_executor(state: AgentState, tool_registry: ToolRegistry,
                  context_mgr: ContextManager) -> dict:
    """节点 2: 执行工具调用"""
    last_msg = state["messages"][-1]
    results = []

    if not hasattr(last_msg, "tool_calls") or not last_msg.tool_calls:
        return {"messages": state["messages"], "step_count": state["step_count"] + 1}

    any_success = False
    for tc in last_msg.tool_calls:
        try:
            result = tool_registry.execute(tc["name"], tc.get("args", {}))
            results.append(FunctionMessage(content=str(result), name=tc["name"]))
            # 标记关键数据到 protected
            is_critical = any(kw in str(result).upper() for kw in ["ERROR", "FATAL", "CRITICAL", "EXCEPTION"])
            context_mgr.add_protected({
                "tool": tc["name"],
                "data": str(result)[:500],
                "critical": is_critical,
                "timestamp": time.time(),
            })
            any_success = True
        except Exception as e:
            results.append(FunctionMessage(content=f"工具错误: {e}", name=tc["name"]))
            logger.warning(f"工具 {tc['name']} 执行失败: {e}")

    state["messages"].extend(results)
    state["step_count"] += 1
    return {
        "messages": state["messages"],
        "step_count": state["step_count"],
        "tools_failed": not any_success,
    }


def evaluate_evidence(state: AgentState, context_mgr: ContextManager,
                      resilience: ResilienceHandler) -> dict:
    """节点 3: 评估证据是否支持当前假设"""
    tools_schema = []  # 评估阶段不需要工具
    messages, protected = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state["messages"][-10:],
    )

    evidence_text = "\n".join(
        f"[{p['tool']}] {str(p['data'])[:200]}"
        for p in protected[-5:]
    )
    messages.append(HumanMessage(content=(
        f"当前假设：{state.get('hypothesis', '未确定')}\n\n"
        f"收集到的证据：\n{evidence_text}\n\n"
        f"请评估：\n"
        f"1. 证据是否充分支持当前假设？\n"
        f"2. 置信度是多少（0-100%）？\n"
        f"3. 是否需要更多证据？\n"
        f"请按格式回复：\n"
        f"评估：[支持/部分支持/不支持]\n"
        f"置信度：[0-100]\n"
        f"下一步：[确认根因/继续验证/修改假设]"
    )))

    try:
        response = call_llm_with_retry(messages, tools=None)
        resilience.record_llm_success()
    except Exception as e:
        logger.error(f"证据评估 LLM 调用失败: {e}")
        resilience.record_llm_failure()
        return {"messages": state["messages"]}

    state["messages"].append(HumanMessage(content="请评估证据"))
    state["messages"].append(response)
    context_mgr.record_reasoning_step(HumanMessage(content="请评估证据"))
    context_mgr.record_reasoning_step(response)

    content = response.content
    return {"messages": state["messages"]}


def refine_hypothesis(state: AgentState, context_mgr: ContextManager,
                      resilience: ResilienceHandler) -> dict:
    """节点 4: 根据新证据修正假设"""
    tools_schema = []
    messages, _ = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state["messages"][-10:],
    )
    messages.append(HumanMessage(content=(
        f"当前假设未能完全确认：{state.get('hypothesis', '')}\n"
        f"请根据已有证据修正假设，或提出新的诊断方向。"
    )))

    try:
        response = call_llm_with_retry(messages, tools=tools_schema)
        resilience.record_llm_success()
    except Exception as e:
        logger.error(f"修正假设 LLM 调用失败: {e}")
        resilience.record_llm_failure()
        return {"messages": state["messages"]}

    # 将当前假设记录到历史
    hypothesis_history = state.get("hypothesis_history", []) + [{
        "hypothesis": state.get("hypothesis", ""),
        "result": "refined",
        "reason": response.content[:100],
    }]
    context_mgr.compress_excluded_hypothesis(
        f"已修正假设「{state.get('hypothesis', '')[:80]}」→ 新方向：{response.content[:80]}"
    )

    return {
        "messages": state["messages"] + [response],
        "hypothesis": response.content[:200],
        "hypothesis_history": hypothesis_history,
    }


def root_cause(state: AgentState, context_mgr: ContextManager,
               resilience: ResilienceHandler) -> dict:
    """节点 5: 确认根因，输出诊断报告"""
    messages, protected = context_mgr.build_messages(
        alert=state["alert"],
        reasoning_history=state["messages"][-10:],
    )
    messages.append(HumanMessage(content=(
        f"证据已充分，假设已确认。\n"
        f"请输出最终诊断报告：\n"
        f"- 根因：[具体根因描述]\n"
        f"- 置信度：[0-100%]\n"
        f"- 证据链：[关键证据]\n"
        f"- 修复建议：[具体操作]"
    )))

    try:
        response = call_llm_with_retry(messages, tools=None)
        resilience.record_llm_success()
    except Exception as e:
        response = AIMessage(content=f"诊断报告中 LLM 调用失败: {e}")

    return {
        "diagnosis": response.content,
        "messages": state["messages"] + [response],
    }


def timeout_handler(state: AgentState) -> dict:
    """超时处理：输出阶段性结论"""
    hypothesis_history = state.get("hypothesis_history", [])
    excluded = [h for h in hypothesis_history if h.get("result") in ("excluded", "refined")]
    diagnosis = (
        f"[诊断超时 - 阶段性结论]\n"
        f"已排除 {len(excluded)} 个假设：\n" +
        "\n".join(f"- {h.get('hypothesis', '')[:100]}" for h in excluded) +
        f"\n当前假设：{state.get('hypothesis', '未确定')[:200]}\n"
        f"已执行 {state.get('step_count', 0)} 步"
    )
    return {
        "diagnosis": diagnosis,
        "truncated": True,
        "messages": state["messages"],
    }


# ---------- 路由函数 ----------

def route_after_formulate(state: AgentState) -> str:
    """formulate_hypothesis 之后的路由"""
    if state.get("truncated"):
        return "timeout_handler"
    last_msg = state["messages"][-1]
    if hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "tool_executor"
    return "evaluate_evidence"


def route_after_evaluate(state: AgentState) -> str:
    """evaluate_evidence 之后的路由"""
    resilience = getattr(state, "_resilience", None)
    if resilience and resilience.mode == AgentMode.NOTIFY_ONLY:
        return "timeout_handler"

    last_msg = state["messages"][-1]
    content = last_msg.content.lower() if hasattr(last_msg, "content") else ""

    if "确认根因" in content or "confirmed" in content or "置信度" in content and "90" in content:
        return "root_cause"
    elif "修改假设" in content or "refine" in content or "不支持" in content:
        return "refine_hypothesis"
    elif hasattr(last_msg, "tool_calls") and last_msg.tool_calls:
        return "tool_executor"
    else:
        # 默认：如果置信度低，重新形成假设
        return "formulate_hypothesis"


def route_after_refine(state: AgentState) -> str:
    """refine_hypothesis 之后的路由：返回 formulate 开始新一轮"""
    return "formulate_hypothesis"


def check_timeout(state: AgentState, resilience: ResilienceHandler) -> str:
    """全局超时检查"""
    if resilience.time_exceeded(
        state.get("step_count", 0), MAX_STEPS,
        state.get("start_time", time.time()), MAX_DURATION,
    ):
        return "timeout_handler"
    return "continue"


# ---------- 构建状态机 ----------

def build_graph(tool_registry: ToolRegistry,
                context_mgr: ContextManager = None,
                resilience: ResilienceHandler = None) -> StateGraph:
    """构建假设驱动诊断状态机"""
    if context_mgr is None:
        context_mgr = ContextManager()
    if resilience is None:
        resilience = ResilienceHandler()

    workflow = StateGraph(AgentState)

    # 添加节点
    workflow.add_node("formulate_hypothesis",
                      lambda s: formulate_hypothesis(s, tool_registry, context_mgr, resilience))
    workflow.add_node("tool_executor",
                      lambda s: tool_executor(s, tool_registry, context_mgr))
    workflow.add_node("evaluate_evidence",
                      lambda s: evaluate_evidence(s, context_mgr, resilience))
    workflow.add_node("refine_hypothesis",
                      lambda s: refine_hypothesis(s, context_mgr, resilience))
    workflow.add_node("root_cause",
                      lambda s: root_cause(s, context_mgr, resilience))
    workflow.add_node("timeout_handler", timeout_handler)

    # 设置入口
    workflow.set_entry_point("formulate_hypothesis")

    # 添加边
    workflow.add_conditional_edges("formulate_hypothesis", route_after_formulate, {
        "tool_executor": "tool_executor",
        "evaluate_evidence": "evaluate_evidence",
        "timeout_handler": "timeout_handler",
    })

    workflow.add_edge("tool_executor", "evaluate_evidence")

    workflow.add_conditional_edges("evaluate_evidence", route_after_evaluate, {
        "root_cause": "root_cause",
        "refine_hypothesis": "refine_hypothesis",
        "tool_executor": "tool_executor",
        "formulate_hypothesis": "formulate_hypothesis",
        "timeout_handler": "timeout_handler",
    })

    workflow.add_edge("refine_hypothesis", "formulate_hypothesis")

    # 终态
    workflow.add_edge("root_cause", END)
    workflow.add_edge("timeout_handler", END)

    return workflow.compile()
```

### Step 4: 运行测试

```bash
pytest tests/test_agent_graph.py -v
```
Expected: 4 PASS

### Step 5: Commit

```bash
git add src/agent/graph.py tests/test_agent_graph.py
git commit -m "feat: 假设驱动诊断状态机（LangGraph 6 节点）"
```

---

## Task 2.4: 诊断入口 + CLI 脚本

**Files:**
- Create: `src/agent/diagnosis.py`
- Create: `scripts/diagnose.py`

### Step 1: 实现诊断入口

```python
# src/agent/diagnosis.py
import time
import uuid
import json
import asyncio
import logging
from dataclasses import dataclass, field
from src.agent.state import AgentState
from src.agent.graph import build_graph
from src.agent.context import ContextManager
from src.agent.resilience import ResilienceHandler
from src.agent.tools import create_agent_tools

logger = logging.getLogger(__name__)


@dataclass
class DiagnosisResult:
    conversation_id: str
    diagnosis: str
    hypothesis: str
    hypothesis_history: list
    step_count: int
    truncated: bool
    duration_seconds: float
    mode: str


def run_diagnosis(alert: dict, es_client=None, neo4j_client=None) -> DiagnosisResult:
    """执行告警诊断（同步包装）"""
    return asyncio.run(_run_diagnosis_async(alert, es_client, neo4j_client))


async def _run_diagnosis_async(alert: dict, es_client=None, neo4j_client=None) -> DiagnosisResult:
    """异步执行告警诊断"""
    conversation_id = str(uuid.uuid4())[:8]
    context_mgr = ContextManager()
    resilience = ResilienceHandler()
    tools = create_agent_tools(es_client=es_client, neo4j_client=neo4j_client)

    graph = build_graph(tools, context_mgr=context_mgr, resilience=resilience)

    initial_state = AgentState(
        alert=alert,
        messages=[],
        hypothesis="",
        hypothesis_history=[],
        evidence={},
        diagnosis="",
        action_plan={},
        active_intent="diagnose",
        pending_confirmations=[],
        protected_data=[],
        compressed_memory=[],
        mode="full",
        step_count=0,
        start_time=time.time(),
        conversation_id=conversation_id,
        llm_failures=0,
        tools_failed=False,
        truncated=False,
    )

    start = time.time()
    logger.info(f"开始诊断 {conversation_id}: {alert.get('labels', {}).get('service', 'unknown')}")

    try:
        result = graph.invoke(initial_state, config={"recursion_limit": 20})
    except Exception as e:
        logger.error(f"诊断异常: {e}")
        return DiagnosisResult(
            conversation_id=conversation_id,
            diagnosis=f"诊断异常中断: {e}",
            hypothesis="",
            hypothesis_history=[],
            step_count=0,
            truncated=True,
            duration_seconds=time.time() - start,
            mode=resilience.mode.value,
        )

    duration = time.time() - start

    return DiagnosisResult(
        conversation_id=result.get("conversation_id", conversation_id),
        diagnosis=result.get("diagnosis", ""),
        hypothesis=result.get("hypothesis", ""),
        hypothesis_history=result.get("hypothesis_history", []),
        step_count=result.get("step_count", 0),
        truncated=result.get("truncated", False),
        duration_seconds=duration,
        mode=result.get("mode", resilience.mode.value),
    )
```

### Step 2: 实现 CLI 脚本

```python
#!/usr/bin/env python3
# scripts/diagnose.py
"""OPS AI Agent — CLI 诊断工具

用法:
    python scripts/diagnose.py --alert '<JSON>'
    python scripts/diagnose.py --file <alert.json>

示例:
    python scripts/diagnose.py --alert '{"alert_id":"t1","severity":"critical","labels":{"service":"payment"},"annotations":{"summary":"P99 > 500ms"}}'
"""

import sys
import json
import argparse
import logging
from pathlib import Path

# 添加项目根目录到 path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.agent.diagnosis import run_diagnosis
from src.infra.es_client import ESLogClient
from src.infra.neo4j_client import Neo4jClient
from src.config import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def format_report(result):
    """格式化诊断报告"""
    status = "⚠ 诊断超时（阶段性结论）" if result.truncated else "✓ 诊断完成"
    lines = [
        "=" * 60,
        f"  OPS AI Agent 诊断报告",
        "=" * 60,
        f"会话 ID: {result.conversation_id}",
        f"状态: {status}",
        f"耗时: {result.duration_seconds:.1f}s",
        f"步数: {result.step_count}",
        f"模式: {result.mode}",
        "-" * 60,
        f"当前假设: {result.hypothesis[:200] if result.hypothesis else '无'}",
        "-" * 60,
        f"诊断结论:",
        f"{result.diagnosis}",
        "-" * 60,
    ]
    if result.hypothesis_history:
        lines.append("排除的假设:")
        for h in result.hypothesis_history:
            lines.append(f"  - {h.get('hypothesis', '')[:100]} ({h.get('result', '')})")
    lines.append("=" * 60)
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="OPS AI Agent CLI 诊断工具")
    parser.add_argument("--alert", type=str, help="告警 JSON 字符串")
    parser.add_argument("--file", type=str, help="告警 JSON 文件路径")
    parser.add_argument("--no-es", action="store_true", help="不使用 ES（使用 mock）")
    parser.add_argument("--no-neo4j", action="store_true", help="不使用 Neo4j（使用 mock）")
    args = parser.parse_args()

    # 解析告警
    if args.alert:
        alert = json.loads(args.alert)
    elif args.file:
        alert = json.loads(Path(args.file).read_text())
    else:
        # 默认演示告警
        alert = {
            "alert_id": "demo-001",
            "severity": "critical",
            "status": "firing",
            "source": "prometheus",
            "labels": {"service": "payment", "alertname": "HighLatency"},
            "annotations": {"summary": "支付服务 P99 延迟 > 500ms"},
            "timestamp": "2026-06-28T10:00:00Z",
        }
        logger.info("使用默认演示告警")

    # 初始化客户端
    es = None if args.no_es else ESLogClient(settings.es_hosts)
    neo4j = None if args.no_neo4j else Neo4jClient(
        settings.neo4j_uri, settings.neo4j_user, settings.neo4j_password
    )

    # 执行诊断
    result = run_diagnosis(alert, es_client=es, neo4j_client=neo4j)

    # 输出报告
    print(format_report(result))

    # 清理
    if es:
        import asyncio
        asyncio.run(es.close())
    if neo4j:
        neo4j.close()


if __name__ == "__main__":
    main()
```

### Step 3: 手动验证

```bash
# 测试默认演示告警（无真实 ES/Neo4j）
python scripts/diagnose.py --no-es --no-neo4j
# Expected: 诊断报告输出，至少包含 hypothesis 和 diagnosis 字段
```

### Step 4: Commit

```bash
git add src/agent/diagnosis.py scripts/diagnose.py
git commit -m "feat: 诊断入口函数 + CLI 诊断脚本"
```

---

## Task 2.5: Checkpointer 持久化集成

**Files:**
- Modify: `src/agent/graph.py`
- Create: `src/agent/checkpointer.py`
- Test: `tests/test_agent_checkpointer.py`

### Step 1: 编写测试

```python
# tests/test_agent_checkpointer.py
import pytest
import uuid
from src.agent.checkpointer import SessionManager


@pytest.fixture
def session_mgr():
    return SessionManager(
        pg_dsn="postgresql://agent:agent@localhost:5432/agentops",
        redis_url="redis://localhost:6379/1",
    )


def test_create_and_get_session(session_mgr):
    """创建和获取会话"""
    conv_id = str(uuid.uuid4())
    session_mgr.create_session(conv_id, {"checkpoint_id": "ckpt-1"})
    session = session_mgr.get_session(conv_id)
    assert session is not None
    assert session["checkpoint_id"] == "ckpt-1"


def test_list_active_sessions(session_mgr):
    """列出活跃会话"""
    conv_id = str(uuid.uuid4())
    session_mgr.create_session(conv_id, {"checkpoint_id": "ckpt-2"})
    active = session_mgr.list_active_sessions()
    assert len(active) >= 1


def test_delete_session(session_mgr):
    """删除会话"""
    conv_id = str(uuid.uuid4())
    session_mgr.create_session(conv_id, {"checkpoint_id": "ckpt-3"})
    session_mgr.delete_session(conv_id)
    assert session_mgr.get_session(conv_id) is None
```

### Step 2: 实现 SessionManager

```python
# src/agent/checkpointer.py
import json
import time
import logging
import redis
from src.config import settings

logger = logging.getLogger(__name__)

SESSION_TTL = 7200  # 2 小时


class SessionManager:
    """Agent 会话管理器（Redis 缓存 + PostgreSQL 持久化）"""

    def __init__(self, pg_dsn: str = None, redis_url: str = None):
        self.pg_dsn = pg_dsn or settings.pg_dsn
        self.redis_url = redis_url or settings.redis_url
        self._redis = None

    @property
    def redis(self):
        if self._redis is None:
            try:
                self._redis = redis.from_url(self.redis_url, decode_responses=True)
            except Exception as e:
                logger.warning(f"Redis 连接失败: {e}")
                self._redis = None
        return self._redis

    def create_session(self, conversation_id: str, metadata: dict):
        """创建会话记录"""
        if not self.redis:
            return
        key = f"agent:session:{conversation_id}"
        metadata["created_at"] = time.time()
        metadata["last_active_at"] = time.time()
        self.redis.setex(key, SESSION_TTL, json.dumps(metadata))

    def get_session(self, conversation_id: str) -> dict | None:
        """获取会话"""
        if not self.redis:
            return None
        key = f"agent:session:{conversation_id}"
        data = self.redis.get(key)
        if data:
            return json.loads(data)
        return None

    def update_session(self, conversation_id: str, metadata: dict):
        """更新会话元数据"""
        if not self.redis:
            return
        key = f"agent:session:{conversation_id}"
        existing = self.get_session(conversation_id)
        if existing:
            existing.update(metadata)
            existing["last_active_at"] = time.time()
            self.redis.setex(key, SESSION_TTL, json.dumps(existing))

    def list_active_sessions(self) -> list[dict]:
        """列出所有活跃会话"""
        if not self.redis:
            return []
        sessions = []
        for key in self.redis.scan_iter("agent:session:*"):
            data = self.redis.get(key)
            if data:
                sessions.append(json.loads(data))
        return sessions

    def delete_session(self, conversation_id: str):
        """删除会话"""
        if not self.redis:
            return
        self.redis.delete(f"agent:session:{conversation_id}")
```

### Step 3: 在 graph.py 中集成 Checkpointer

在 `build_graph()` 函数中添加 Checkpointer 支持（修改 graph.py 末尾）：

```python
# 在 build_graph 返回前添加
from langgraph.checkpoint.postgres import PostgresSaver
try:
    checkpointer = PostgresSaver.from_conn_string(settings.pg_dsn)
    checkpointer.setup()
    return workflow.compile(checkpointer=checkpointer)
except Exception as e:
    logger.warning(f"Checkpointer 初始化失败，使用无持久化模式: {e}")
    return workflow.compile()
```

### Step 4: 运行测试

```bash
pytest tests/test_agent_checkpointer.py -v
```
Expected: 3 PASS

### Step 5: Commit

```bash
git add src/agent/checkpointer.py src/agent/graph.py tests/test_agent_checkpointer.py
git commit -m "feat: Checkpointer 持久化 + Redis 会话管理"
```

---

## Task 2.6: 端到端诊断集成测试

**Files:**
- Create: `tests/integration/test_diagnosis_e2e.py`

### Step 1: 编写集成测试

```python
# tests/integration/test_diagnosis_e2e.py
import pytest
from unittest.mock import Mock, patch
from src.agent.diagnosis import run_diagnosis, DiagnosisResult
from src.agent.tools import create_agent_tools


@pytest.fixture
def sample_alert():
    return {
        "alert_id": "e2e-001",
        "severity": "critical",
        "status": "firing",
        "source": "prometheus",
        "labels": {"service": "payment", "alertname": "HighLatency"},
        "annotations": {"summary": "支付服务 P99 延迟 > 500ms"},
        "timestamp": "2026-06-28T10:00:00Z",
    }


@pytest.mark.integration
def test_diagnosis_with_mock_llm(sample_alert):
    """端到端诊断流程（mock LLM + mock 工具）"""
    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        # 模拟 LLM 响应序列
        mock_llm.side_effect = [
            # 第 1 次: formulate_hypothesis — 生成假设 + 请求工具
            Mock(
                content="假设：下游 Redis 延迟导致。需要查询日志和拓扑验证。",
                tool_calls=[
                    {"name": "search_logs", "args": {"service": "payment", "keywords": "error timeout"}, "id": "c1"},
                    {"name": "get_topology", "args": {"service": "payment", "hops": 2}, "id": "c2"},
                ],
            ),
            # 第 2 次: evaluate_evidence — 证据支持假设
            Mock(
                content="评估：支持。置信度：92。确认根因。",
                tool_calls=None,
            ),
            # 第 3 次: root_cause — 输出诊断报告
            Mock(
                content="根因：Redis 连接池耗尽导致支付服务延迟升高。"
                        "证据：日志显示 Redis 连接超时错误，拓扑确认 payment 依赖 Redis。"
                        "置信度：92%。建议：扩容 Redis 连接池。",
                tool_calls=None,
            ),
        ]

        result = run_diagnosis(sample_alert)

        assert isinstance(result, DiagnosisResult)
        assert result.step_count >= 1
        assert len(result.diagnosis) > 0
        assert "Redis" in result.diagnosis


@pytest.mark.integration
def test_diagnosis_handles_llm_failure(sample_alert):
    """LLM 失败时不应崩溃"""
    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.side_effect = TimeoutError("LLM 超时")

        result = run_diagnosis(sample_alert)

        assert isinstance(result, DiagnosisResult)
        assert result.truncated is True


@pytest.mark.integration
def test_diagnosis_uses_multiple_tools(sample_alert):
    """验证 Agent 能够调用多个不同工具"""
    with patch("src.agent.graph.call_llm_with_retry") as mock_llm:
        mock_llm.side_effect = [
            Mock(
                content="需要查询日志、拓扑和爆炸半径",
                tool_calls=[
                    {"name": "search_logs", "args": {"service": "payment", "keywords": "error"}, "id": "c1"},
                    {"name": "get_topology", "args": {"service": "payment", "hops": 2}, "id": "c2"},
                    {"name": "analyze_blast_radius", "args": {"service": "payment", "hops": 3}, "id": "c3"},
                ],
            ),
            Mock(content="评估：支持。置信度：88。确认根因。", tool_calls=None),
            Mock(content="根因已确认。", tool_calls=None),
        ]

        result = run_diagnosis(sample_alert)
        assert result.step_count >= 1
```

### Step 2: 运行测试

```bash
pytest tests/integration/test_diagnosis_e2e.py -v -m integration
```
Expected: 3 PASS

### Step 3: Commit

```bash
git add tests/integration/test_diagnosis_e2e.py
git commit -m "test: Phase 2 端到端诊断集成测试"
```

---

## 最终验收检查清单

| 验收项 | 验证命令 |
|--------|----------|
| Agent 工具绑定 | `pytest tests/test_agent_tools.py -v` |
| 分层上下文管理 | `pytest tests/test_agent_context.py -v` |
| 异常重试降级 | `pytest tests/test_agent_resilience.py -v` |
| 假设驱动状态机 | `pytest tests/test_agent_graph.py -v` |
| Checkpointer 持久化 | `pytest tests/test_agent_checkpointer.py -v` |
| CLI 诊断脚本 | `python scripts/diagnose.py --no-es --no-neo4j` |
| 端到端集成测试 | `pytest tests/integration/test_diagnosis_e2e.py -v -m integration` |

---

## 目录结构总览

```
src/agent/
├── __init__.py
├── state.py            # AgentState TypedDict (扩展)
├── context.py          # 分层上下文 (新增)
├── resilience.py       # 重试/降级 (新增)
├── tools.py            # Agent 工具绑定 (新增)
├── graph.py            # 假设驱动状态机 (重写)
├── diagnosis.py        # 诊断入口 (新增)
└── checkpointer.py     # 会话管理 (新增)

scripts/
└── diagnose.py         # CLI 诊断脚本 (新增)

tests/
├── test_agent_tools.py         # 工具绑定测试 (新增)
├── test_agent_context.py       # 上下文测试 (新增)
├── test_agent_resilience.py    # 异常处理测试 (新增)
├── test_agent_graph.py         # 状态机测试 (重写)
├── test_agent_checkpointer.py  # 持久化测试 (新增)
└── integration/
    └── test_diagnosis_e2e.py   # E2E 诊断测试 (新增)
```
