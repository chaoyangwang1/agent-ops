import asyncio
import logging
from datetime import datetime
from src.config import settings

logger = logging.getLogger(__name__)


class AgentWorker:
    """后台 Worker：消费 Kafka 聚合告警 → 自动触发 Agent 诊断"""

    def __init__(self, kafka, diagnosis_fn, notifier=None,
                 incident_store=None, conversation_repo=None):
        self.kafka = kafka
        self.diagnosis_fn = diagnosis_fn
        self.notifier = notifier
        self.incident_store = incident_store
        self.conversation_repo = conversation_repo
        self._running = False

    def start(self):
        self._running = True
        logger.info("AgentWorker 已启动")

    def stop(self):
        self._running = False
        logger.info("AgentWorker 已停止")

    async def run(self):
        """主循环"""
        self.start()
        while self._running:
            try:
                alerts = self.kafka.consume(
                    settings.kafka_topic_aggregated_alerts,
                    max_messages=10,
                    timeout=5,
                )
                for alert_data in alerts:
                    await self._process_alert(alert_data)
            except Exception as e:
                logger.error(f"Worker 消费失败: {e}")
            await asyncio.sleep(1)

    async def _process_alert(self, alert_data: dict):
        """处理单条聚合告警"""
        alert = {
            "alert_id": alert_data.get("alert_id", alert_data.get("aggregation_key", "")),
            "severity": alert_data.get("severity", "warning"),
            "labels": alert_data.get("labels", {}),
            "annotations": alert_data.get("annotations", {}),
        }
        service = alert["labels"].get("service", "unknown")
        logger.info(f"自动诊断触发: service={service}")

        result = self.diagnosis_fn(alert)

        # ChatOps 推送
        if self.notifier and not result.truncated:
            try:
                await self.notifier.send(result)
            except Exception as e:
                logger.error(f"通知发送失败: {e}")

        # 自动入库
        if self.incident_store and not result.truncated:
            try:
                self.incident_store.add_incident(
                    summary=alert.get("annotations", {}).get("summary", ""),
                    root_cause=result.diagnosis[:1024],
                    service=service,
                    severity=alert.get("severity", "warning"),
                )
            except Exception as e:
                logger.error(f"故障入库失败: {e}")

        # 对话归档
        if self.conversation_repo:
            from src.conversation.models import ConversationRecord
            record = ConversationRecord(
                conversation_id=result.conversation_id,
                alert_id=alert.get("alert_id", ""),
                service=service,
                severity=alert.get("severity", "warning"),
                status="timeout" if result.truncated else "completed",
                diagnosis=result.diagnosis,
                hypothesis_history=result.hypothesis_history,
                step_count=result.step_count,
                truncated=result.truncated,
                mode=result.mode,
                duration_seconds=result.duration_seconds,
                completed_at=datetime.utcnow(),
            )
            try:
                await self.conversation_repo.archive(record)
            except Exception as e:
                logger.error(f"对话归档失败: {e}")
