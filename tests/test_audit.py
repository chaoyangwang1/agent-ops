from unittest.mock import Mock
from src.execution.audit import AuditService


def test_record_execution():
    audit = AuditService(db=Mock(), es=Mock())
    audit.record(
        conversation_id="conv-1",
        action="restart_pod",
        target={"namespace": "prod", "name": "payment"},
        policy_result="allowed",
        execution_status="success",
        executed_by="agent-001",
    )
    assert audit._db.called


def test_record_rollback():
    audit = AuditService(db=Mock(), es=Mock())
    audit.record(
        conversation_id="conv-2",
        action="scale_deployment",
        target={"namespace": "prod", "name": "order"},
        policy_result="allowed",
        execution_status="rolled_back",
        executed_by="agent-002",
    )
    assert audit._es.called


def test_record_denied():
    audit = AuditService(db=Mock(), es=Mock())
    audit.record(
        conversation_id="conv-3",
        action="delete_pod",
        target={"namespace": "prod", "name": "redis"},
        policy_result="denied",
        execution_status="denied",
        executed_by="agent-003",
    )
    assert audit._db.called
