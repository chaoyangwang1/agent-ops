import pytest
import asyncio
from unittest.mock import Mock, AsyncMock
from src.worker.agent_worker import AgentWorker


@pytest.fixture
def worker():
    mock_kafka = Mock()
    mock_kafka.consume.return_value = []
    mock_diagnosis = Mock()
    mock_diagnosis.return_value = Mock(
        conversation_id="conv-1", diagnosis="测试诊断", truncated=False,
        hypothesis="测试假设", hypothesis_history=[], step_count=3,
        duration_seconds=1.5, mode="full",
    )
    mock_notifier = AsyncMock()
    mock_store = Mock()
    return AgentWorker(
        kafka=mock_kafka,
        diagnosis_fn=mock_diagnosis,
        notifier=mock_notifier,
        incident_store=mock_store,
        conversation_repo=None,
    )


@pytest.mark.asyncio
async def test_worker_processes_alert(worker):
    worker.kafka.consume.return_value = [{
        "aggregation_key": "key-1",
        "severity": "critical",
        "labels": {"service": "payment"},
        "annotations": {"summary": "CPU 过高"},
    }]
    worker._running = True

    async def stop_after():
        await asyncio.sleep(0.5)
        worker.stop()

    await asyncio.gather(worker.run(), stop_after())
    assert worker.diagnosis_fn.called


def test_worker_start_stop(worker):
    worker.start()
    assert worker._running is True
    worker.stop()
    assert worker._running is False
