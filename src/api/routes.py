from fastapi import APIRouter, Depends
from fastapi.responses import PlainTextResponse
from prometheus_client import generate_latest
from src.pipeline.adapters import AlertNormalizer
from src.infra.kafka_client import KafkaManager
from src.config import settings
from src.api.dependencies import require_auth
from pydantic import BaseModel

router = APIRouter()


class IngestRequest(BaseModel):
    source: str
    raw: dict


class IngestResponse(BaseModel):
    accepted: int
    alert_ids: list[str]


def get_kafka():
    return KafkaManager(settings.kafka_bootstrap_servers)


@router.get("/health")
async def health():
    return {"status": "ok", "version": "0.1.0"}


@router.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    return generate_latest()


@router.post("/api/v1/alerts/ingest", response_model=IngestResponse, status_code=202)
async def ingest_alert(req: IngestRequest, kafka: KafkaManager = Depends(get_kafka),
                       auth: dict = Depends(require_auth)):
    alerts = AlertNormalizer.normalize(req.source, req.raw)
    for alert in alerts:
        kafka.produce(settings.kafka_topic_raw_alerts, alert.__dict__)
    return IngestResponse(
        accepted=len(alerts),
        alert_ids=[a.alert_id for a in alerts],
    )
