from unittest.mock import Mock
from src.sync.k8s_syncer import K8sTopologySyncer


def test_sync_pod_to_neo4j():
    mock_neo4j = Mock()
    syncer = K8sTopologySyncer(mock_neo4j)

    pod_event = {
        "type": "ADDED",
        "object": {
            "metadata": {
                "name": "payment-7d8f9-abc",
                "namespace": "prod",
                "labels": {"app": "payment"},
            },
            "spec": {"nodeName": "node-1"},
            "status": {"phase": "Running"},
        }
    }
    syncer.handle_pod_event(pod_event)
    mock_neo4j.merge_service.assert_called()
    mock_neo4j.query.assert_called()  # SCHEDULED_ON 关系


def test_sync_node_to_neo4j():
    mock_neo4j = Mock()
    syncer = K8sTopologySyncer(mock_neo4j)

    syncer.handle_node_event({
        "type": "ADDED",
        "object": {
            "metadata": {"name": "node-1", "labels": {"zone": "us-east-1a"}},
            "status": {"conditions": [{"type": "Ready", "status": "True"}]},
        }
    })
    mock_neo4j.query.assert_called()
