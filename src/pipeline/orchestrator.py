import asyncio
import logging
from src.pipeline.aggregator import AlertAggregator
from src.pipeline.adapters import UnifiedAlert
from src.infra.kafka_client import KafkaManager
from src.infra.database import Database
from src.infra.es_client import ESLogClient
from src.config import settings

logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """告警处理管道：消费 raw.alerts → 聚合 → 输出 aggregated.alerts"""

    def __init__(self, kafka: KafkaManager, db: Database, es: ESLogClient,
                 window_seconds: int = 300):
        self.kafka = kafka
        self.db = db
        self.es = es
        self.aggregator = AlertAggregator(window_seconds=window_seconds)
        self._running = False

    def process(self, alert: UnifiedAlert):
        """处理单条告警"""
        logger.info(f"Processing alert {alert.alert_id} from {alert.source}")
        self.aggregator.process(alert)

    def flush(self) -> list:
        """刷新窗口，输出聚合告警"""
        aggregated = self.aggregator.flush()
        for agg in aggregated:
            # 写入 Kafka
            self.kafka.produce(
                settings.kafka_topic_aggregated_alerts,
                agg.__dict__,
            )
            logger.info(f"Aggregated alert: key={agg.aggregation_key}, "
                        f"type={agg.aggregation_type}, count={agg.merged_count}")
        return aggregated

    async def run(self):
        """主循环：持续消费 → 处理 → 定时刷新"""
        self._running = True
        last_flush = asyncio.get_event_loop().time()

        while self._running:
            # 消费 raw.alerts
            try:
                messages = self.kafka.consume(
                    settings.kafka_topic_raw_alerts,
                    max_messages=100,
                    timeout=5,
                )
                for msg in messages:
                    alert = UnifiedAlert(**msg)
                    self.process(alert)
            except Exception as e:
                logger.error(f"消费告警失败: {e}")

            # 按窗口刷新
            now = asyncio.get_event_loop().time()
            if now - last_flush >= self.aggregator.window_seconds:
                self.flush()
                last_flush = now

            await asyncio.sleep(1)

    def stop(self):
        self._running = False
