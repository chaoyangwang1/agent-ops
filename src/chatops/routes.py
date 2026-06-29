from fastapi import APIRouter, Depends, HTTPException
from src.conversation.repository import ConversationRepository
from src.infra.database import Database
from src.config import settings
from src.api.dependencies import require_auth

router = APIRouter(prefix="/api/v1", tags=["diagnosis"])


def get_conversation_repo() -> ConversationRepository:
    db = Database(settings.pg_dsn)
    return ConversationRepository(db)


@router.get("/diagnosis/{conversation_id}")
async def get_diagnosis(conversation_id: str,
                        repo: ConversationRepository = Depends(get_conversation_repo),
                        auth: dict = Depends(require_auth)):
    """查询诊断结果"""
    try:
        await repo.db.init()
    except Exception:
        pass
    result = await repo.get_by_id(conversation_id)
    if result is None:
        raise HTTPException(status_code=404, detail="诊断记录不存在")
    return {
        "conversation_id": result.conversation_id,
        "status": result.status,
        "service": result.service,
        "severity": result.severity,
        "diagnosis": result.diagnosis,
        "step_count": result.step_count,
        "duration_seconds": result.duration_seconds,
        "truncated": result.truncated,
    }


@router.get("/conversations")
async def list_conversations(service: str = None, severity: str = None, limit: int = 20,
                             repo: ConversationRepository = Depends(get_conversation_repo),
                             auth: dict = Depends(require_auth)):
    """搜索历史对话"""
    try:
        await repo.db.init()
    except Exception:
        pass
    if service:
        results = await repo.list_by_service(service, limit=limit)
    else:
        results = await repo.list_recent(limit=limit)
    return [{
        "conversation_id": r.conversation_id,
        "status": r.status,
        "service": r.service,
        "severity": r.severity,
        "diagnosis": r.diagnosis[:200],
        "duration_seconds": r.duration_seconds,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    } for r in results]
