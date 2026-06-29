import logging
from src.tools.registry import ToolRegistry, ToolDefinition
from src.execution.policy import PolicyEngine, PolicyResult
from src.execution.approval import ApprovalService
from src.execution.k8s_executor import MockK8sExecutor, create_executor
from src.execution.rollback import RollbackManager
from src.execution.audit import AuditService

logger = logging.getLogger(__name__)


def create_action_tools(executor=None, policy=None, approval=None, audit=None) -> ToolRegistry:
    registry = ToolRegistry()
    executor = executor or create_executor()
    policy = policy or PolicyEngine()
    approval = approval or ApprovalService()
    audit = audit or AuditService()
    rollback_mgr = RollbackManager(executor)

    def _wrap_execute(action_name, risk_level, execute_fn):
        def handler(**kwargs):
            conv_id = kwargs.pop("_conversation_id", "unknown")
            namespace = kwargs.get("namespace", "default")
            resource_name = kwargs.get("name", kwargs.get("pod_name", ""))
            target = {"namespace": namespace, "name": resource_name}
            total = kwargs.get("total_replicas", 3)
            affected = kwargs.get("affected_replicas", 1)
            policy_action = {
                "action": action_name,
                "resource": {"kind": "Deployment", "name": resource_name, "namespace": namespace},
                "risk_level": risk_level,
                "affected_replicas": affected,
                "total_replicas": total,
            }
            result = policy.evaluate(policy_action)
            if result == PolicyResult.DENIED:
                audit.record(conv_id, action_name, target, "denied", "denied", f"agent-{conv_id}")
                return {"status": "denied", "reason": "策略引擎拒绝此操作"}
            if result == PolicyResult.NEEDS_APPROVAL:
                req = approval.create_request(action_name, target, risk_level)
                return {"status": "pending_approval", "approval_id": req["approval_id"], "message": f"高风险操作需要审批，审批 ID: {req['approval_id']}"}
            snapshot = rollback_mgr.snapshot_before(action_name, target)
            try:
                exec_result = execute_fn(**kwargs)
                audit.record(conv_id, action_name, target, "allowed", exec_result.status, f"agent-{conv_id}", snapshot=snapshot)
                return exec_result.to_dict()
            except Exception as e:
                audit.record(conv_id, action_name, target, "allowed", "failed", f"agent-{conv_id}", error_message=str(e))
                return {"status": "failed", "message": str(e)}
        return handler

    registry.register(ToolDefinition(
        name="restart_pod", description="滚动重启指定服务的 Pod（风险：低）",
        parameters={"namespace": {"type": "string"}, "pod_name": {"type": "string"}, "idempotency_key": {"type": "string"}},
    ), handler=_wrap_execute("restart_pod", "low", lambda **kw: executor.restart_pod(kw["namespace"], kw["pod_name"], kw.get("idempotency_key", ""))))

    registry.register(ToolDefinition(
        name="scale_deployment", description="调整 Deployment 副本数（风险：中）",
        parameters={"namespace": {"type": "string"}, "name": {"type": "string"}, "replicas": {"type": "integer"}, "idempotency_key": {"type": "string"}},
    ), handler=_wrap_execute("scale_deployment", "medium", lambda **kw: executor.scale_deployment(kw["namespace"], kw["name"], kw["replicas"], kw.get("idempotency_key", ""))))

    registry.register(ToolDefinition(
        name="rollback_deployment", description="回滚 Deployment 到上一版本（风险：高，需审批）",
        parameters={"namespace": {"type": "string"}, "name": {"type": "string"}, "revision": {"type": "integer"}, "idempotency_key": {"type": "string"}},
    ), handler=_wrap_execute("rollback_deployment", "high", lambda **kw: executor.rollback_deployment(kw["namespace"], kw["name"], kw.get("revision", 0), kw.get("idempotency_key", ""))))

    return registry
