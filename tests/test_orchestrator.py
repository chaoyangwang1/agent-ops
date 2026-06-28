import asyncio
import pytest
from unittest.mock import Mock
from src.pipeline.orchestrator import PipelineOrchestrator
from src.pipeline.adapters import UnifiedAlert


def make_alert(alert_id, labels):
    return UnifiedAlert(
        alert_id=alert_id, source="test", fingerprint="fp",
        severity="warning", status="firing", labels=labels,
        annotations={}, timestamp="2026-06-28T10:00:00Z",
    )


@pytest.mark.asyncio
async def test_pipeline_processes_alert():
    mock_kafka = Mock()
    mock_db = Mock()
    mock_es = Mock()
    orch = PipelineOrchestrator(
        kafka=mock_kafka,
        db=mock_db,
        es=mock_es,
        window_seconds=1,  # 短窗口便于测试
    )
    orch.process(make_alert("a1", {"service": "svc", "node": "n1"}))
    orch.process(make_alert("a2", {"service": "svc", "node": "n1"}))
    orch.process(make_alert("a3", {"service": "svc", "node": "n1"}))

    await asyncio.sleep(1.5)
    results = orch.flush()

    assert len(results) > 0
    # 验证写入了 Kafka
    assert mock_kafka.produce.called
