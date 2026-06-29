from langchain_openai import OpenAIEmbeddings
from src.config import settings

_embedding_model = None


def get_embedding_model() -> OpenAIEmbeddings:
    global _embedding_model
    if _embedding_model is None:
        _embedding_model = OpenAIEmbeddings(
            base_url=settings.llm_api_base,
            api_key=settings.llm_api_key,
            model="text-embedding-3-small",
        )
    return _embedding_model


def embed_text(text: str) -> list[float]:
    model = get_embedding_model()
    return model.embed_query(text)


def embed_texts(texts: list[str]) -> list[list[float]]:
    model = get_embedding_model()
    return model.embed_documents(texts)
