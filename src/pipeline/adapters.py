import uuid
from datetime import datetime
from dataclasses import dataclass


@dataclass
class UnifiedAlert:
    alert_id: str
    source: str
    fingerprint: str
    severity: str
    status: str
    labels: dict
    annotations: dict
    timestamp: str
    value: float | None = None


class AlertNormalizer:
    SEVERITY_MAP = {"critical": "critical", "warning": "warning", "info": "info"}

    @classmethod
    def normalize(cls, source: str, raw: dict) -> list[UnifiedAlert]:
        handler = getattr(cls, f"_from_{source}", None)
        if handler:
            return handler(raw)
        return cls._from_generic(source, raw)

    @classmethod
    def _from_prometheus(cls, raw: dict) -> list[UnifiedAlert]:
        results = []
        for alert in raw.get("alerts", []):
            labels = alert.get("labels", {})
            severity = labels.get("severity", "warning")
            results.append(UnifiedAlert(
                alert_id=str(uuid.uuid4()),
                source="prometheus",
                fingerprint=labels.get("alertname", ""),
                severity=severity,
                status=raw.get("status", "firing"),
                labels=labels,
                annotations=alert.get("annotations", {}),
                timestamp=alert.get("startsAt", datetime.utcnow().isoformat()),
            ))
        return results

    @classmethod
    def _from_generic(cls, source: str, raw: dict) -> list[UnifiedAlert]:
        return [UnifiedAlert(
            alert_id=str(uuid.uuid4()),
            source=source,
            fingerprint=raw.get("title", str(uuid.uuid4())[:8]),
            severity=raw.get("severity", "info"),
            status=raw.get("status", "firing"),
            labels=raw.get("labels", {}),
            annotations=raw.get("annotations", {}),
            timestamp=raw.get("timestamp", datetime.utcnow().isoformat()),
        )]
