import httpx
import logging
from abc import ABC, abstractmethod
from src.agent.diagnosis import DiagnosisResult

logger = logging.getLogger(__name__)


class BaseNotifier(ABC):
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    @abstractmethod
    async def send(self, result: DiagnosisResult) -> bool:
        pass


class FeishuNotifier(BaseNotifier):
    """飞书 Webhook 通知器"""

    async def send(self, result: DiagnosisResult) -> bool:
        card = self._build_card(result)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(self.webhook_url, json={"msg_type": "interactive", "card": card})
                return resp.status_code == 200
        except Exception as e:
            logger.error(f"飞书推送失败: {e}")
            return False

    def _build_card(self, result: DiagnosisResult) -> dict:
        status_emoji = "✅" if not result.truncated else "⚠️"
        return {
            "header": {"title": {"content": f"{status_emoji} OPS AI Agent 诊断报告", "tag": "plain_text"}},
            "elements": [
                {"tag": "div", "text": {"content": f"**会话 ID**: {result.conversation_id}"}},
                {"tag": "div", "text": {"content": f"**耗时**: {result.duration_seconds:.1f}s | **步数**: {result.step_count}"}},
                {"tag": "hr"},
                {"tag": "div", "text": {"content": f"**诊断结论**\n{result.diagnosis}"}},
                {"tag": "hr"},
                {"tag": "action", "actions": [
                    {"tag": "button", "text": {"content": "确认回滚"}, "type": "primary"},
                    {"tag": "button", "text": {"content": "重启服务"}, "type": "default"},
                    {"tag": "button", "text": {"content": "忽略"}, "type": "danger"},
                ]},
            ],
            "content": f"{result.diagnosis}",
        }


class DingtalkNotifier(BaseNotifier):
    """钉钉 Webhook 通知器"""

    async def send(self, result: DiagnosisResult) -> bool:
        message = self._build_message(result)
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.post(self.webhook_url, json=message)
                return resp.status_code == 200
        except Exception as e:
            logger.error(f"钉钉推送失败: {e}")
            return False

    def _build_message(self, result: DiagnosisResult) -> dict:
        status = "✅ 诊断完成" if not result.truncated else "⚠️ 诊断超时"
        text = (
            f"## {status}\n\n"
            f"**会话 ID**: {result.conversation_id}\n\n"
            f"**耗时**: {result.duration_seconds:.1f}s | **步数**: {result.step_count}\n\n"
            f"---\n\n"
            f"**诊断结论**\n{result.diagnosis}\n\n"
        )
        return {
            "msgtype": "markdown",
            "markdown": {"title": "OPS AI Agent 诊断报告", "text": text},
        }
