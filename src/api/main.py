from fastapi import FastAPI
from src.api.routes import router


def create_app() -> FastAPI:
    app = FastAPI(title="OPS AI Agent", version="0.1.0")
    app.include_router(router)
    return app


app = create_app()
