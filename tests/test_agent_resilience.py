import pytest
from src.agent.resilience import (
    ResilienceHandler, AgentMode, retry_with_backoff,
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
