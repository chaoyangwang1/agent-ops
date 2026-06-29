from pymilvus import connections, Collection, FieldSchema, CollectionSchema, DataType, utility

COLLECTION_NAME = "incidents"
VECTOR_DIM = 1536


class MilvusManager:
    def __init__(self, host: str = "localhost", port: str = "19530"):
        self.host = host
        self.port = port
        self.collection_name = COLLECTION_NAME
        connections.connect(host=host, port=port)
        self._collection = None

    @property
    def collection(self):
        if self._collection is None and utility.has_collection(self.collection_name):
            self._collection = Collection(self.collection_name)
        return self._collection

    def init_collection(self):
        if utility.has_collection(self.collection_name):
            self._collection = Collection(self.collection_name)
            self._collection.load()
            return
        fields = [
            FieldSchema(name="incident_id", dtype=DataType.VARCHAR, is_primary=True, max_length=64),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM),
            FieldSchema(name="summary", dtype=DataType.VARCHAR, max_length=512),
            FieldSchema(name="root_cause", dtype=DataType.VARCHAR, max_length=1024),
            FieldSchema(name="service", dtype=DataType.VARCHAR, max_length=128),
            FieldSchema(name="severity", dtype=DataType.VARCHAR, max_length=16),
            FieldSchema(name="created_at", dtype=DataType.INT64),
        ]
        schema = CollectionSchema(fields, description="故障案例向量库")
        self._collection = Collection(self.collection_name, schema)
        index_params = {"metric_type": "IP", "index_type": "IVF_FLAT", "params": {"nlist": 128}}
        self._collection.create_index("embedding", index_params)
        self._collection.load()

    def insert(self, data: dict) -> list:
        ids = self._collection.insert([
            data["incident_id"], data["embedding"], data["summary"],
            data["root_cause"], data["service"], data["severity"], data["created_at"],
        ])
        return ids.primary_keys

    def flush(self):
        if self._collection:
            self._collection.flush()

    def search(self, query_vector: list, top_k: int = 5) -> list[dict]:
        if self._collection is None:
            return []
        search_params = {"metric_type": "IP", "params": {"nprobe": 10}}
        results = self._collection.search(
            [query_vector], "embedding", search_params, limit=top_k,
            output_fields=["incident_id", "summary", "root_cause", "service", "severity"]
        )
        if not results or len(results[0]) == 0:
            return []
        return [
            {"id": hit.id, "distance": hit.distance, **{f: hit.entity.get(f) for f in hit.entity.fields}}
            for hit in results[0]
        ]

    def count(self) -> int:
        if self._collection is None:
            return 0
        return self._collection.num_entities

    def close(self):
        connections.disconnect("default")
