"""RAG pipeline for enriching match reports with local project documents.

local documents are split, embedded with a sentence-transformers model, 
persisted in Chroma, and retrieved by semantic similarity. If those optional 
dependencies are missing, the module keeps the app usable through a small TF-IDF fallback.
"""

from __future__ import annotations

import hashlib
import math
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DOCS_PATH = PROJECT_ROOT / "data" / "docs" / "rag_corpus"
DEFAULT_PERSIST_DIR = PROJECT_ROOT / "output" / "chroma_db"
DEFAULT_EMBEDDING_MODEL = "sentence-transformers/all-mpnet-base-v2"
TOKEN_RE = re.compile(r"\w+", re.UNICODE)


@dataclass(frozen=True)
class DocumentChunk:
    """Indexed text fragment with source metadata."""

    source: str
    title: str
    text: str


@dataclass(frozen=True)
class RetrievedContext:
    """Retrieved context plus traceable source metadata."""

    context: str
    sources: list[str]
    backend: str

    def as_text(self) -> str:
        return self.context


def _tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_RE.findall(text)]


def _chunk_text(text: str, max_words: int = 140, overlap: int = 25) -> list[str]:
    words = text.split()
    if not words:
        return []

    chunks = []
    step = max(max_words - overlap, 1)
    for start in range(0, len(words), step):
        chunk = " ".join(words[start : start + max_words])
        if chunk:
            chunks.append(chunk)
    return chunks


def load_documents(docs_path: str | Path = DEFAULT_DOCS_PATH) -> list[DocumentChunk]:
    """Load `.md` and `.txt` documents and split them into retrieval chunks."""
    base_path = Path(docs_path)
    if not base_path.exists():
        return []

    documents: list[DocumentChunk] = []
    for path in sorted(base_path.rglob("*")):
        if path.suffix.lower() not in {".md", ".txt"} or not path.is_file():
            continue

        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        title = path.stem.replace("_", " ").replace("-", " ").title()
        for index, chunk in enumerate(_chunk_text(text)):
            documents.append(
                DocumentChunk(
                    source=f"{path.relative_to(base_path)}#chunk-{index + 1}",
                    title=title,
                    text=chunk,
                )
            )
    return documents


def _corpus_fingerprint(documents: list[DocumentChunk]) -> str:
    digest = hashlib.sha1()
    for doc in documents:
        digest.update(doc.source.encode("utf-8"))
        digest.update(doc.text.encode("utf-8"))
    return digest.hexdigest()[:12]


def _langchain_available() -> bool:
    try:
        import langchain_chroma  # noqa: F401
        import langchain_core.documents  # noqa: F401
        import langchain_huggingface  # noqa: F401
        import langchain_text_splitters  # noqa: F401
    except ImportError:
        return False
    return True


@lru_cache(maxsize=4)
def _build_chroma_retriever(
    docs_path_str: str,
    persist_dir_str: str,
    embedding_model: str,
    k: int,
) -> Any:
    """Build or reopen a persistent Chroma retriever for the current corpus."""
    from langchain_chroma import Chroma
    from langchain_core.documents import Document
    from langchain_huggingface import HuggingFaceEmbeddings
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    docs_path = Path(docs_path_str)
    persist_dir = Path(persist_dir_str)
    raw_documents = []

    for path in sorted(docs_path.rglob("*")):
        if path.suffix.lower() not in {".md", ".txt"} or not path.is_file():
            continue
        text = path.read_text(encoding="utf-8", errors="ignore").strip()
        if not text:
            continue
        raw_documents.append(
            Document(
                page_content=text,
                metadata={
                    "source_file": str(path.relative_to(docs_path)),
                    "title": path.stem.replace("_", " ").replace("-", " ").title(),
                },
            )
        )

    if not raw_documents:
        return None

    splitter = RecursiveCharacterTextSplitter(chunk_size=700, chunk_overlap=120)
    splits = splitter.split_documents(raw_documents)
    for index, doc in enumerate(splits):
        doc.metadata["chunk"] = index + 1
        doc.metadata["source"] = f"{doc.metadata['source_file']}#chunk-{index + 1}"

    collection_name = f"match_rag_{_corpus_fingerprint(load_documents(docs_path))}"
    embeddings = HuggingFaceEmbeddings(model_name=embedding_model)
    vector_store = Chroma(
        collection_name=collection_name,
        embedding_function=embeddings,
        persist_directory=str(persist_dir),
    )

    if vector_store._collection.count() == 0:
        ids = [
            hashlib.sha1(f"{doc.metadata['source']}::{doc.page_content}".encode("utf-8")).hexdigest()
            for doc in splits
        ]
        vector_store.add_documents(splits, ids=ids)

    return vector_store.as_retriever(search_kwargs={"k": k})


def build_vector_store(documents: list[DocumentChunk]) -> dict[str, Any]:
    """Build a small in-memory TF-IDF style index for lexical fallback retrieval."""
    tokenized_docs = [_tokenize(doc.text) for doc in documents]
    doc_freq: dict[str, int] = {}
    for tokens in tokenized_docs:
        for token in set(tokens):
            doc_freq[token] = doc_freq.get(token, 0) + 1

    total_docs = max(len(documents), 1)
    vectors: list[dict[str, float]] = []
    for tokens in tokenized_docs:
        counts: dict[str, int] = {}
        for token in tokens:
            counts[token] = counts.get(token, 0) + 1
        vectors.append(
            {
                token: (count / len(tokens)) * math.log((1 + total_docs) / (1 + doc_freq[token]) + 1)
                for token, count in counts.items()
            }
            if tokens
            else {}
        )

    return {"documents": documents, "vectors": vectors, "doc_freq": doc_freq, "total_docs": total_docs}


def _query_vector(query: str, vector_store: dict[str, Any]) -> dict[str, float]:
    tokens = _tokenize(query)
    counts: dict[str, int] = {}
    for token in tokens:
        counts[token] = counts.get(token, 0) + 1

    total_docs = vector_store.get("total_docs", 1)
    doc_freq = vector_store.get("doc_freq", {})
    return {
        token: (count / len(tokens)) * math.log((1 + total_docs) / (1 + doc_freq.get(token, 0)) + 1)
        for token, count in counts.items()
    } if tokens else {}


def _cosine(left: dict[str, float], right: dict[str, float]) -> float:
    numerator = sum(value * right.get(token, 0.0) for token, value in left.items())
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


def _format_sources(sources: list[str]) -> str:
    if not sources:
        return "- sin fuentes recuperadas"
    return "\n".join(f"- {source}" for source in sources)


def _retrieve_with_fallback(
    query: str,
    vector_store: dict[str, Any] | None = None,
    k: int = 4,
) -> RetrievedContext:
    if vector_store is None:
        vector_store = build_vector_store(load_documents())

    documents: list[DocumentChunk] = vector_store.get("documents", [])
    if not documents:
        return RetrievedContext(
            context="No se encontraron documentos de contexto en data/docs/rag_corpus.",
            sources=[],
            backend="tfidf-fallback",
        )

    query_vector = _query_vector(query, vector_store)
    ranked = sorted(
        (
            (_cosine(query_vector, doc_vector), doc)
            for doc, doc_vector in zip(documents, vector_store.get("vectors", []))
        ),
        key=lambda item: item[0],
        reverse=True,
    )

    selected = [item for item in ranked[:k] if item[0] > 0] or ranked[:k]
    sources: list[str] = []
    lines = []
    for score, doc in selected:
        if doc.source not in sources:
            sources.append(doc.source)
        lines.append(f"[Fuente: {doc.source} | {doc.title} | relevancia={score:.3f}]\n{doc.text}")
    return RetrievedContext(context="\n\n".join(lines), sources=sources, backend="tfidf-fallback")


def retrieve_context_with_sources(
    query: str,
    vector_store: dict[str, Any] | None = None,
    k: int = 4,
    docs_path: str | Path = DEFAULT_DOCS_PATH,
    persist_dir: str | Path = DEFAULT_PERSIST_DIR,
    embedding_model: str = DEFAULT_EMBEDDING_MODEL,
) -> RetrievedContext:
    """Retrieve contextual snippets with source metadata.

    Chroma is used when the LangChain vector dependencies are installed. The
    lexical fallback accepts `vector_store` to preserve compatibility with the
    previous project code and lightweight test environments.
    """
    if vector_store is not None or not _langchain_available():
        return _retrieve_with_fallback(query, vector_store=vector_store, k=k)

    try:
        retriever = _build_chroma_retriever(str(Path(docs_path)), str(Path(persist_dir)), embedding_model, k)
        if retriever is None:
            return _retrieve_with_fallback(query, vector_store=build_vector_store([]), k=k)
        docs = retriever.invoke(query)
    except Exception as exc:
        fallback = _retrieve_with_fallback(query, vector_store=build_vector_store(load_documents(docs_path)), k=k)
        return RetrievedContext(
            context=(
                f"[Aviso: no se pudo usar Chroma/embeddings ({exc}). "
                "Se usa recuperacion lexical local.]\n\n"
                f"{fallback.context}"
            ),
            sources=fallback.sources,
            backend="tfidf-fallback",
        )

    sources: list[str] = []
    lines = []
    for doc in docs:
        source = doc.metadata.get("source", doc.metadata.get("source_file", "desconocido"))
        title = doc.metadata.get("title", "Documento")
        if source not in sources:
            sources.append(source)
        lines.append(f"[Fuente: {source} | {title} | backend=chroma]\n{doc.page_content}")

    return RetrievedContext(context="\n\n".join(lines), sources=sources, backend="chroma")


def retrieve_context(query: str, vector_store: dict[str, Any] | None = None, k: int = 4) -> str:
    """Retrieve top-k relevant context snippets for a report-generation query."""
    return retrieve_context_with_sources(query, vector_store=vector_store, k=k).as_text()


def format_context_with_sources(retrieved: RetrievedContext) -> str:
    """Return context followed by an explicit source list for prompts and reports."""
    return f"{retrieved.context}\n\nFuentes RAG ({retrieved.backend}):\n{_format_sources(retrieved.sources)}"
