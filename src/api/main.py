from contextlib import asynccontextmanager
from fastapi import FastAPI
from src.api.routes import router
from src.chatops.routes import router as chatops_router
from src.config import settings


@asynccontextmanager
async def _lifespan(app: FastAPI):
    worker = None
    worker_task = None
    if settings.agent_auto_trigger:
        from src.worker.agent_worker import AgentWorker
        from src.infra.kafka_client import KafkaManager
        from src.agent.diagnosis import run_diagnosis
        from src.chatops.notifier import FeishuNotifier

        notifier = FeishuNotifier(settings.feishu_webhook_url) if settings.feishu_webhook_url else None
        kafka = KafkaManager(settings.kafka_bootstrap_servers)
        worker = AgentWorker(
            kafka=kafka,
            diagnosis_fn=lambda alert: run_diagnosis(alert),
            notifier=notifier,
        )
        worker_task = asyncio.create_task(worker.run())

    yield

    if worker and worker_task:
        worker.stop()
        worker_task.cancel()


def create_app() -> FastAPI:
    app = FastAPI(title="OPS AI Agent", version="0.2.0", lifespan=_lifespan)
    app.include_router(router)
    app.include_router(chatops_router)
    return app


app = create_app()
