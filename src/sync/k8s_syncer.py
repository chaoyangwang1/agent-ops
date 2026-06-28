import logging

logger = logging.getLogger(__name__)


class K8sTopologySyncer:
    """同步 K8s 拓扑到 Neo4j"""

    def __init__(self, neo4j):
        self.neo4j = neo4j

    def handle_pod_event(self, event: dict):
        obj = event["object"]
        metadata = obj["metadata"]
        namespace = metadata["namespace"]
        pod_name = metadata["name"]
        app = metadata.get("labels", {}).get("app", pod_name.split("-")[0])
        node_name = obj.get("spec", {}).get("nodeName", "")
        phase = obj.get("status", {}).get("phase", "Unknown")

        # 确保 Service 节点存在
        self.neo4j.merge_service(app, {"namespace": namespace})

        # 确保 Node 节点存在
        if node_name:
            self.neo4j.query(
                "MERGE (n:Node {name: $name})", {"name": node_name}
            )
            # Pod SCHEDULED_ON Node
            self.neo4j.query("""
                MATCH (s:Service {name: $svc})
                MATCH (n:Node {name: $node})
                MERGE (s)-[:SCHEDULED_ON]->(n)
                SET s.last_seen = datetime()
            """, {"svc": app, "node": node_name})

        logger.info(f"Synced Pod: {namespace}/{pod_name} (app={app}, node={node_name}, phase={phase})")

    def handle_node_event(self, event: dict):
        obj = event["object"]
        name = obj["metadata"]["name"]
        labels = obj["metadata"].get("labels", {})
        zone = labels.get("topology.kubernetes.io/zone", labels.get("zone", "unknown"))
        ready = any(
            c["type"] == "Ready" and c["status"] == "True"
            for c in obj.get("status", {}).get("conditions", [])
        )

        self.neo4j.query("""
            MERGE (n:Node {name: $name})
            SET n.zone = $zone, n.ready = $ready, n.last_seen = datetime()
        """, {"name": name, "zone": zone, "ready": ready})

        logger.info(f"Synced Node: {name} (zone={zone}, ready={ready})")

    def handle_deployment_event(self, event: dict):
        obj = event["object"]
        metadata = obj["metadata"]
        name = metadata["name"]
        namespace = metadata["namespace"]
        replicas = obj.get("spec", {}).get("replicas", 0)

        self.neo4j.merge_service(name, {
            "namespace": namespace,
            "kind": "Deployment",
            "replicas": replicas,
        })
        logger.info(f"Synced Deployment: {namespace}/{name} (replicas={replicas})")
