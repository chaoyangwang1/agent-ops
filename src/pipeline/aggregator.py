import hashlib
import json
import time
from dataclasses import dataclass, field
from collections import defaultdict
from src.pipeline.adapters import UnifiedAlert


@dataclass
class AggregatedAlert:
    aggregation_key: str
    aggregation_type: str  # "mechanical" | "node" | "deployment" | "namespace"
    severity: str
    status: str
    labels: dict
    source_alerts: list[str]  # 原始 alert_ids
    merged_count: int
    first_at: str
    last_at: str


class AlertAggregator:
    """告警聚合器：机械聚合 + 语义聚合"""

    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self._buffer: dict[str, list[UnifiedAlert]] = defaultdict(list)
        self._last_flush = time.time()

    def _make_key(self, alert: UnifiedAlert) -> str:
        """机械聚合 key：基于关键标签 hash"""
        key_labels = {
            k: alert.labels[k]
            for k in ["service", "alertname", "namespace"]
            if k in alert.labels
        }
        raw = json.dumps(key_labels, sort_keys=True)
        return hashlib.md5(raw.encode()).hexdigest()

    def _make_node_key(self, alert: UnifiedAlert) -> str | None:
        node = alert.labels.get("node") or alert.labels.get("instance")
        return f"node:{node}" if node else None

    def _make_deploy_key(self, alert: UnifiedAlert) -> str | None:
        deploy = alert.labels.get("deployment")
        return f"deploy:{deploy}" if deploy else None

    def process(self, alert: UnifiedAlert):
        """处理一条告警，放入缓冲区"""
        # 机械聚合
        mech_key = self._make_key(alert)
        self._buffer[mech_key].append(alert)
        # 语义聚合 key（辅助，不影响机械聚合）
        for key_fn in [self._make_node_key, self._make_deploy_key]:
            key = key_fn(alert)
            if key:
                self._buffer[key].append(alert)

    def flush(self) -> list[AggregatedAlert]:
        """刷新窗口，输出聚合告警"""
        results = []
        now = time.time()

        # 收集语义聚合覆盖的 alert_ids
        semantic_alert_ids: set[str] = set()
        semantic_results = []

        for key, alerts in self._buffer.items():
            if not alerts:
                continue
            agg_type = "mechanical"
            if key.startswith("node:"):
                agg_type = "node"
            elif key.startswith("deploy:"):
                agg_type = "deployment"

            # 确定最高严重级别
            severity_order = {"critical": 3, "warning": 2, "info": 1}
            max_sev = max(alerts, key=lambda a: severity_order.get(a.severity, 0))

            alert_ids = [a.alert_id for a in alerts]
            agg = AggregatedAlert(
                aggregation_key=key,
                aggregation_type=agg_type,
                severity=max_sev.severity,
                status="firing",
                labels=alerts[0].labels,
                source_alerts=alert_ids,
                merged_count=len(alerts),
                first_at=min(a.timestamp for a in alerts),
                last_at=max(a.timestamp for a in alerts),
            )

            if agg_type != "mechanical" and len(alerts) > 1:
                semantic_alert_ids.update(alert_ids)
                semantic_results.append(agg)
            else:
                results.append(agg)

        # 过滤掉已被语义聚合覆盖的机械聚合
        results = [r for r in results
                   if not (r.aggregation_type == "mechanical"
                           and set(r.source_alerts).issubset(semantic_alert_ids))]

        results.extend(semantic_results)
        self._buffer.clear()
        self._last_flush = now
        return results
