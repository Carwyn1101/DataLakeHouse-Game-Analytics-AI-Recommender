"""
Shared runtime settings for the Streamlit AI recommender.

Các giá trị ở đây là contract giữa Docker Compose, Streamlit app và các module
retrieval. Hầu hết có thể override bằng environment variable để chạy cùng code
trong container hoặc local dev.
"""

import os

CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "20000"))
TOTAL_GAMES = int(os.getenv("TOTAL_GAMES", "80000"))
TOTAL_CHUNKS = int(os.getenv("TOTAL_CHUNKS", "4"))

# These paths are inside the Streamlit container. Because /app is bind-mounted
# from ./streamlit-ai, both indexes are persisted to the project folder on host.
INDEX_DIR = os.getenv("FAISS_INDEX_DIR", "/app/faiss_indexes")
CHROMA_DIR = os.getenv("CHROMA_DIR", "/app/chroma_db")
CHROMA_COLLECTION = os.getenv("CHROMA_COLLECTION", "game_semantic_profiles")

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://ollama:11434")
OLLAMA_CHAT_MODEL = os.getenv("OLLAMA_CHAT_MODEL", "llama3.2:1b")
OLLAMA_EMBED_MODEL = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
USE_LLM_INTENT_PARSING = os.getenv("USE_LLM_INTENT_PARSING", "false").lower() in {"1", "true", "yes"}

HYBRID_WEIGHTS = {
    "faiss_structured": 0.5,
    "semantic": 0.3,
    "review_quality": 0.2,
}
