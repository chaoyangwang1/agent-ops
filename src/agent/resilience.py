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
                    raise
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
