import pytest
import asyncio
from unittest.mock import patch, Mock
from src.chatops.notifier import FeishuNotifier, DingtalkNotifier
from src.agent.diagnosis import DiagnosisResult


@pytest.fixture
def sample_result():
    return DiagnosisResult(
        conversation_id="conv-1",
        diagnosis="根因：Redis 连接池耗尽。置信度：92%。建议：扩容 Redis。",
        hypothesis="Redis 问题",
        hypothesis_history=[],
        step_count=3,
        truncated=False,
        duration_seconds=12.5,
        mode="full",
    )


def test_feishu_notifier_formats_card(sample_result):
    notifier = FeishuNotifier(webhook_url="https://hooks.feishu.com/test")
    card = notifier._build_card(sample_result)
    assert "Redis" in card["content"]


def test_feishu_notifier_send(sample_result):
    notifier = FeishuNotifier(webhook_url="https://hooks.feishu.com/test")
    with patch("httpx.AsyncClient.post") as mock_post:
        mock_post.return_value = Mock(status_code=200, json=lambda: {"code": 0})
        result = asyncio.run(notifier.send(sample_result))
        assert result is True


def test_dingtalk_notifier_formats_message(sample_result):
    notifier = DingtalkNotifier(webhook_url="https://hooks.dingtalk.com/test")
    msg = notifier._build_message(sample_result)
    assert "Redis" in msg["markdown"]["text"]
