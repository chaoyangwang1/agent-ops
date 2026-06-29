import time
import logging
import os
from pathlib import Path
from dataclasses import dataclass, field
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    action: str
    status: str
    message: str = ""
    details: dict = field(default_factory=dict)
    duration_ms: float = 0

    def to_dict(self):
        return {
            "action": self.action,
            "status": self.status,
            "message": self.message,
            "details": self.details,
            "duration_ms": self.duration_ms,
        }


class BaseK8sExecutor(ABC):
    def __init__(self):
        self._executed_keys: set[str] = set()

    def _check_idempotent(self, key: str) -> ExecutionResult | None:
        if key in self._executed_keys:
            return ExecutionResult(
                action="unknown",
                status="skipped",
                message=f"幂等键 {key} 已执行，跳过",
            )
        self._executed_keys.add(key)
        return None

    @abstractmethod
    def restart_pod(self, namespace, pod_name, idempotency_key) -> ExecutionResult:
        pass

    @abstractmethod
    def scale_deployment(
        self, namespace, name, replicas, idempotency_key
    ) -> ExecutionResult:
        pass

    @abstractmethod
    def rollback_deployment(
        self, namespace, name, revision, idempotency_key
    ) -> ExecutionResult:
        pass


class MockK8sExecutor(BaseK8sExecutor):
    def restart_pod(self, namespace, pod_name, idempotency_key) -> ExecutionResult:
        dup = self._check_idempotent(idempotency_key)
        if dup:
            return dup
        time.sleep(0.01)
        return ExecutionResult(
            action="restart_pod",
            status="success",
            message=f"模拟重启 Pod {namespace}/{pod_name} 成功",
            details={"namespace": namespace, "pod": pod_name},
            duration_ms=10,
        )

    def scale_deployment(
        self, namespace, name, replicas, idempotency_key
    ) -> ExecutionResult:
        dup = self._check_idempotent(idempotency_key)
        if dup:
            return dup
        return ExecutionResult(
            action="scale_deployment",
            status="success",
            message=f"模拟扩缩容 {namespace}/{name} → {replicas} 副本",
            details={"namespace": namespace, "name": name, "replicas": replicas},
            duration_ms=5,
        )

    def rollback_deployment(
        self, namespace, name, revision, idempotency_key
    ) -> ExecutionResult:
        dup = self._check_idempotent(idempotency_key)
        if dup:
            return dup
        return ExecutionResult(
            action="rollback_deployment",
            status="success",
            message=f"模拟回滚 {namespace}/{name} → revision {revision}",
            details={"namespace": namespace, "name": name, "revision": revision},
            duration_ms=10,
        )


def create_executor():
    kubeconfig = os.environ.get("KUBECONFIG") or os.path.expanduser("~/.kube/config")
    if Path(kubeconfig).exists():
        logger.info("kubeconfig 存在，但 kubernetes-client 未安装，使用 Mock 执行器")
    return MockK8sExecutor()
