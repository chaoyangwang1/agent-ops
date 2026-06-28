from neo4j import GraphDatabase


class Neo4jClient:
    def __init__(self, uri: str, user: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def query(self, cypher: str, params: dict = None) -> list[dict]:
        with self.driver.session() as session:
            result = session.run(cypher, params or {})
            return [dict(record) for record in result]

    def init_schema(self):
        """初始化约束和索引"""
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Service) REQUIRE s.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Node) REQUIRE n.name IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (t:Team) REQUIRE t.name IS UNIQUE",
            "CREATE INDEX IF NOT EXISTS FOR (s:SLO) ON (s.name)",
        ]
        for c in constraints:
            try:
                self.query(c)
            except Exception:
                pass  # 约束已存在

    def merge_service(self, name: str, properties: dict):
        props = ", ".join(f"{k}: ${k}" for k in properties)
        self.query(
            f"MERGE (s:Service {{name: $name}}) SET s += {{{props}}}",
            {"name": name, **properties},
        )

    def merge_dependency(self, from_svc: str, to_svc: str, relation: str):
        self.query(f"""
            MATCH (a:Service {{name: $from}})
            MATCH (b:Service {{name: $to}})
            MERGE (a)-[:{relation}]->(b)
        """, {"from": from_svc, "to": to_svc})

    def get_upstream_services(self, service: str) -> list[dict]:
        return self.query("""
            MATCH (s:Service {name: $name})-[r:DEPENDS_ON|USES]->(d)
            RETURN d.name AS name, type(r) AS relation
        """, {"name": service})

    def analyze_blast_radius(self, service: str, hops: int = 3) -> list[dict]:
        return self.query(f"""
            MATCH path = (s:Service {{name: $name}})-[:DEPENDS_ON*1..{hops}]->(d:Service)
            RETURN d.name AS service, length(path) AS distance
            ORDER BY distance
        """, {"name": service})

    def find_common_dependency(self, svc_a: str, svc_b: str) -> list[dict]:
        return self.query("""
            MATCH p = shortestPath((a:Service {name: $a})-[:DEPENDS_ON*]-(b:Service {name: $b}))
            RETURN [n in nodes(p) | n.name] AS path
        """, {"a": svc_a, "b": svc_b})

    def close(self):
        self.driver.close()
