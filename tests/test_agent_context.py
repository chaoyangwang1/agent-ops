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
