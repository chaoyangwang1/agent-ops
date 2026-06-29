import uuid
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class AuditService:
    def __init__(self, db=None, es=None):
        self._db = db
        self._es = es

    def record(
        self,
        conversation_id: str,
        action: str,
        target: dict,
        policy_result: str,
        execution_status: str,
        executed_by: str,
        approval_status: str = "",
        snapshot: dict = None,
        revert_plan: dict = None,
        error_message: str = "",
    ):
        audit_id = str(uuid.uuid4())
        now = datetime.utcnow()
        # 写 PG
        if self._db:
            try:
                self._db(
                    audit_id,
                    {
                        "audit_id": audit_id,
                        "conversation_id": conversation_id,
                        "action": action,
                        "target_namespace": target.get("namespace", ""),
                        "target_resource": target.get("name", ""),
                        "policy_result": policy_result,
                        "approval_status": approval_status,
                        "execution_status": execution_status,
                        "executed_by": executed_by,
                        "error_message": error_message,
                        "started_at": now,
                        "completed_at": now,
                    },
                )
            except Exception as e:
                logger.error(f"审计 PG 写入失败: {e}")
        # 写 ES
        if self._es:
            try:
                self._es(
                    audit_id,
                    {
                        "audit_id": audit_id,
                        "action": action,
                        "target": f"{target.get('namespace', '')}/{target.get('name', '')}",
                        "outcome": execution_status,
                        "details": f"policy={policy_result}",
                        "timestamp": now.isoformat(),
                    },
                )
            except Exception as e:
                logger.error(f"审计 ES 写入失败: {e}")
        logger.info(f"审计记录: {audit_id} {action} → {execution_status}")
        return audit_id
