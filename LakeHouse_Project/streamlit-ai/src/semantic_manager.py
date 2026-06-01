"""Semantic retrieval using Ollama embeddings + ChromaDB."""

from __future__ import annotations

from typing import Dict, List, Optional

import chromadb
import pandas as pd
import requests
import streamlit as st

from .settings import (
    CHROMA_COLLECTION,
    CHROMA_DIR,
    OLLAMA_BASE_URL,
    OLLAMA_EMBED_MODEL,
)

EMBED_TIMEOUT = 8
OLLAMA_HEALTH_TIMEOUT = 2
DEFAULT_STARTUP_SEMANTIC_LIMIT = 30000


def _safe_text(value: object) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none"}:
        return ""
    return text


def build_profile_text(row: pd.Series) -> str:
    return (
        f"Game {row.get('name', '')}. "
        f"Genres: {row.get('genres_cleaned', '')}. "
        f"Tags: {row.get('tags_cleaned', '')}. "
        f"Categories: {row.get('categories_cleaned', '')}. "
        f"Developer: {row.get('developers_cleaned', '')}. "
        f"Publisher: {row.get('publishers_cleaned', '')}. "
        f"Price category: {row.get('price_category_vnd', '')}. "
        f"Review positive ratio: {row.get('steam_review_positive_ratio', 0):.2f}."
    )


def _embed_text(text: str) -> Optional[List[float]]:
    if not is_ollama_available():
        return None

    # Ollama >= 0.2 favors /api/embed, while older versions used /api/embeddings.
    requests_plan = [
        ("/api/embed", {"model": OLLAMA_EMBED_MODEL, "input": text}),
        ("/api/embeddings", {"model": OLLAMA_EMBED_MODEL, "prompt": text}),
    ]

    for path, payload in requests_plan:
        try:
            resp = requests.post(
                f"{OLLAMA_BASE_URL}{path}",
                json=payload,
                timeout=EMBED_TIMEOUT,
            )
            if resp.status_code == 404:
                continue
            resp.raise_for_status()

            body = resp.json()
            if "embedding" in body and isinstance(body["embedding"], list):
                return body["embedding"]

            embeddings = body.get("embeddings")
            if isinstance(embeddings, list) and embeddings and isinstance(embeddings[0], list):
                return embeddings[0]
        except Exception:
            continue

    return None


@st.cache_data(show_spinner=False, ttl=10)
def is_ollama_available() -> bool:
    try:
        resp = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=OLLAMA_HEALTH_TIMEOUT)
        return resp.status_code == 200
    except Exception:
        return False


@st.cache_resource(show_spinner=False)
def get_chroma_collection():
    client = chromadb.PersistentClient(path=CHROMA_DIR)
    return client.get_or_create_collection(name=CHROMA_COLLECTION)


def ensure_semantic_index(games_df: pd.DataFrame, max_new: Optional[int] = DEFAULT_STARTUP_SEMANTIC_LIMIT) -> Dict[str, object]:
    collection = get_chroma_collection()

    all_ids = [str(int(x)) for x in games_df["steam_appid"].tolist()]
    existing = set(collection.get(include=[]).get("ids", []))
    missing_ids = [game_id for game_id in all_ids if game_id not in existing]

    if not missing_ids:
        return {"indexed": len(existing), "new": 0, "status": "ready", "message": "Semantic index is up-to-date."}

    if not is_ollama_available():
        return {
            "indexed": len(existing),
            "new": 0,
            "status": "ollama_unavailable",
            "message": "Ollama not reachable. Running structured-only mode.",
        }

    if max_new is not None and max_new > 0:
        missing_ids = missing_ids[:max_new]

    missing_df = games_df[games_df["steam_appid"].astype(str).isin(missing_ids)].copy()

    batch_ids: List[str] = []
    batch_docs: List[str] = []
    batch_meta: List[dict] = []
    batch_embeddings: List[List[float]] = []
    new_count = 0

    for _, row in missing_df.iterrows():
        game_id = str(int(row["steam_appid"]))
        profile_text = _safe_text(row.get("profile_text")) or build_profile_text(row)

        embedding = _embed_text(profile_text)
        if not embedding:
            continue

        batch_ids.append(game_id)
        batch_docs.append(profile_text)
        batch_embeddings.append(embedding)
        release_year = row.get("release_year", 0)
        try:
            release_year = int(release_year or 0)
        except Exception:
            release_year = 0
        batch_meta.append(
            {
                "name": _safe_text(row.get("name")),
                "price_category_vnd": _safe_text(row.get("price_category_vnd")),
                "release_year": release_year,
            }
        )

        if len(batch_ids) >= 128:
            collection.add(
                ids=batch_ids,
                embeddings=batch_embeddings,
                documents=batch_docs,
                metadatas=batch_meta,
            )
            new_count += len(batch_ids)
            batch_ids, batch_docs, batch_meta, batch_embeddings = [], [], [], []

    if batch_ids:
        collection.add(
            ids=batch_ids,
            embeddings=batch_embeddings,
            documents=batch_docs,
            metadatas=batch_meta,
        )
        new_count += len(batch_ids)

    final_count = len(collection.get(include=[]).get("ids", []))
    remaining = max(0, len(all_ids) - final_count)
    status = "ready" if remaining == 0 else "partial"
    message = "Semantic index updated." if status == "ready" else f"Semantic indexing partial: {remaining:,} items remaining."
    return {"indexed": final_count, "new": new_count, "status": status, "message": message}


def semantic_search(query: str, top_n: int = 20) -> pd.DataFrame:
    if not is_ollama_available():
        return pd.DataFrame(columns=["steam_appid", "semantic_score"])

    collection = get_chroma_collection()
    embedding = _embed_text(query)
    if not embedding:
        return pd.DataFrame(columns=["steam_appid", "semantic_score"])

    result = collection.query(
        query_embeddings=[embedding],
        n_results=top_n,
        include=["distances"],
    )

    ids = result.get("ids", [[]])[0]
    distances = result.get("distances", [[]])[0]

    if not ids:
        return pd.DataFrame(columns=["steam_appid", "semantic_score"])

    rows = []
    for game_id, distance in zip(ids, distances):
        # cosine distance -> similarity in [0,1] approx.
        sim = max(0.0, 1.0 - float(distance))
        rows.append({"steam_appid": int(game_id), "semantic_score": sim})

    return pd.DataFrame(rows)
