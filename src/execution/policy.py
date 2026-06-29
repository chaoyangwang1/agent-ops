import logging
from enum import Enum
from datetime import datetime

logger = logging.getLogger(__name__)

CORE_MIDDLEWARE_PATTERNS = ["kafka", "redis", "mysql", "postgres", "etcd", "zookeeper"]
DEFAULT_ALLOWED_NAMESPACES = ["prod", "staging", "dev", "default"]
ERROR_BUDGET_THRESHOLD = 0.05
HIGH_IMPACT_RATIO = 0.5


class PolicyResult(Enum):
    ALLOWED = "allowed"
    DENIED = "denied"
    NEEDS_APPROVAL = "needs_approval"


class PolicyEngine:
    def __init__(self, allowed_namespaces=None, change_window_start=10,
                 change_window_end=18, change_window_days=None):
        self.allowed_namespaces = allowed_namespaces or DEFAULT_ALLOWED_NAMESPACES
        self.change_window_start = change_window_start
        self.change_window_end = change_window_end
        self.change_window_days = change_window_days or [0, 1, 2, 3, 4]

    def evaluate(self, action: dict, context: dict = None) -> PolicyResult:
        l1 = self._check_l1(action)
        if l1 == PolicyResult.DENIED:
            return PolicyResult.DENIED
        if action.get("risk_level") == "high":
            l3 = self._check_l3(action)
            if l3 == PolicyResult.DENIED:
                return PolicyResult.DENIED
            return PolicyResult.NEEDS_APPROVAL
        l2 = self._check_l2(action)
        if l2 != PolicyResult.ALLOWED:
            return l2
        return self._check_l3(action)

    def _check_l1(self, action: dict) -> PolicyResult:
        resource = action.get("resource", {})
        kind = resource.get("kind", "")
        name = resource.get("name", "")
        namespace = resource.get("namespace", "")
        if kind == "StatefulSet":
            return PolicyResult.DENIED
        for pattern in CORE_MIDDLEWARE_PATTERNS:
            if pattern in name.lower():
                return PolicyResult.DENIED
        if namespace not in self.allowed_namespaces:
            return PolicyResult.DENIED
        if action.get("action") in ("delete_pod", "delete_deployment"):
            return PolicyResult.DENIED
        return PolicyResult.ALLOWED

    def _check_l2(self, action: dict) -> PolicyResult:
        affected = action.get("affected_replicas", 1)
        total = action.get("total_replicas", 1)
        error_budget = action.get("error_budget_remaining", 1.0)
        if error_budget < ERROR_BUDGET_THRESHOLD:
            return PolicyResult.DENIED
        if total > 0 and affected / total > HIGH_IMPACT_RATIO:
            return PolicyResult.NEEDS_APPROVAL
        return PolicyResult.ALLOWED

    def _check_l3(self, action: dict) -> PolicyResult:
        if action.get("risk_level") != "high":
            return PolicyResult.ALLOWED
        if not self._in_change_window():
            return PolicyResult.DENIED
        return PolicyResult.ALLOWED

    def _in_change_window(self) -> bool:
        now = datetime.now()
        if now.weekday() not in self.change_window_days:
            return False
        return self.change_window_start <= now.hour < self.change_window_end
