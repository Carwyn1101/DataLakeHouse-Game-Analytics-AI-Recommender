"""
Hybrid retrieval, reranking, and chatbot tool routing.

Module này là lớp điều phối nghiệp vụ cho chatbot: parse intent, parse filter
từ ngôn ngữ tự nhiên, gọi FAISS/lexical retrieval, gọi semantic retrieval, rồi
trộn điểm bằng `HYBRID_WEIGHTS`. Các hàm explain_* dùng Ollama để tạo phần giải
thích; khi LLM không khả dụng hoặc trả lời không hoàn chỉnh, module rơi về các
template deterministic để app vẫn trả lời ổn định.
"""

from __future__ import annotations

import math
import re
import unicodedata
from typing import Dict, List, Tuple

import pandas as pd
import requests
import streamlit as st

from .faiss_manager import _business_key, recommend_with_faiss, search_across_all_chunks
from .semantic_manager import build_profile_text, is_ollama_available, semantic_search
from .settings import HYBRID_WEIGHTS, OLLAMA_BASE_URL, OLLAMA_CHAT_MODEL, USE_LLM_INTENT_PARSING


def _finite_number(value) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _safe_ratio(value) -> float:
    number = _finite_number(value)
    return max(0.0, min(1.0, number)) if number is not None else 0.0


def _normalize_text(value: str) -> str:
    """Lowercase and strip accents/punctuation for tolerant text matching."""
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fold_accents_keep_symbols(value: str) -> str:
    text = str(value or "").strip().lower()
    text = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in text if unicodedata.category(ch) != "Mn")


def _to_vnd(number_text: str, unit_text: str) -> float | None:
    try:
        number = float(str(number_text).replace(",", ".").strip())
    except Exception:
        return None

    unit = str(unit_text or "").strip().lower()
    if unit in {"k", "nghin"}:
        return number * 1_000
    if unit in {"tr", "trieu", "m"}:
        return number * 1_000_000
    if unit in {"ty", "ti"}:
        return number * 1_000_000_000
    return number


def _to_ratio(value_text: str) -> float | None:
    try:
        value = float(str(value_text).strip())
    except Exception:
        return None
    if value > 1.0:
        value = value / 100.0
    return _safe_ratio(value)


def parse_user_filters(message: str) -> dict:
    """Parse natural-language constraints (price/review/year/coop) from user input."""
    text = _fold_accents_keep_symbols(message)
    normalized = _normalize_text(message)

    filters = {
        "is_coop": any(k in normalized for k in ["co op", "coop", "cooperative"]),
        "price_min": None,
        "price_max": None,
        "review_min": None,
        "review_max": None,
        "year_min": None,
        "year_max": None,
    }

    # Price range: "tu 200k den 500k", "from 200k to 500k"
    range_price = re.search(
        r"(?:tu|from)\s*(\d+(?:[\.,]\d+)?)\s*(k|nghin|tr|trieu|m|ty|ti|vnd)?\s*(?:den|toi|to|-)\s*(\d+(?:[\.,]\d+)?)\s*(k|nghin|tr|trieu|m|ty|ti|vnd)?",
        text,
    )
    if range_price:
        left = _to_vnd(range_price.group(1), range_price.group(2))
        right = _to_vnd(range_price.group(3), range_price.group(4) or range_price.group(2))
        if left is not None and right is not None:
            filters["price_min"] = min(left, right)
            filters["price_max"] = max(left, right)

    # Price max: "duoi 500k", "under 500k", "<= 500k"
    if filters["price_max"] is None:
        m = re.search(
            r"(?:duoi|under|<=|nho hon|khong qua|toi da|max)\s*(\d+(?:[\.,]\d+)?)\s*(k|nghin|tr|trieu|m|ty|ti|vnd)?",
            text,
        )
        if m:
            filters["price_max"] = _to_vnd(m.group(1), m.group(2))

    # Price min: "tren 200k", "tu 200k", ">= 200k"
    if filters["price_min"] is None:
        m = re.search(
            r"(?:tren|hon|>=|it nhat|min|tu|from)\s*(\d+(?:[\.,]\d+)?)\s*(k|nghin|tr|trieu|m|ty|ti|vnd)\b",
            text,
        )
        if m:
            filters["price_min"] = _to_vnd(m.group(1), m.group(2))

    # Review min/max: supports "review tren 85", "review >= 0.8"
    review_min = re.search(r"(?:review|danh gia|positive)[^\d]{0,12}(?:tren|hon|>=|it nhat|min)\s*(\d+(?:[\.,]\d+)?)", text)
    if review_min:
        filters["review_min"] = _to_ratio(review_min.group(1))
    review_max = re.search(r"(?:review|danh gia|positive)[^\d]{0,12}(?:duoi|<|<=|toi da|max)\s*(\d+(?:[\.,]\d+)?)", text)
    if review_max:
        filters["review_max"] = _to_ratio(review_max.group(1))

    # Backward-compatible heuristic for "review tot" phrasing.
    if filters["review_min"] is None and any(k in normalized for k in ["review tot", "danh gia tot", "review cao"]):
        filters["review_min"] = 0.8

    # Year ranges and bounds.
    year_range = re.search(r"(?:tu nam|from)\s*(19\d{2}|20\d{2})\s*(?:den|toi|to|-)\s*(19\d{2}|20\d{2})", text)
    if year_range:
        y1, y2 = int(year_range.group(1)), int(year_range.group(2))
        filters["year_min"] = min(y1, y2)
        filters["year_max"] = max(y1, y2)

    year_min = re.search(r"(?:sau|tu nam|from|>=)\s*(19\d{2}|20\d{2})", text)
    if filters["year_min"] is None and year_min:
        filters["year_min"] = int(year_min.group(1))

    year_max = re.search(r"(?:truoc|before|<=)\s*(19\d{2}|20\d{2})", text)
    if filters["year_max"] is None and year_max:
        filters["year_max"] = int(year_max.group(1))

    return filters


def apply_structured_filters(data: pd.DataFrame, filters: dict) -> pd.DataFrame:
    filtered = data.copy()

    if filters.get("is_coop"):
        filtered = filtered[
            filtered["categories_cleaned"].str.contains("co-op|coop", case=False, na=False)
        ]

    review_min = filters.get("review_min")
    review_max = filters.get("review_max")
    if review_min is not None:
        filtered = filtered[filtered["steam_review_positive_ratio"].fillna(0).astype(float) >= float(review_min)]
    if review_max is not None:
        filtered = filtered[filtered["steam_review_positive_ratio"].fillna(0).astype(float) <= float(review_max)]

    price_min = filters.get("price_min")
    price_max = filters.get("price_max")
    if price_min is not None:
        filtered = filtered[filtered["price_vnd"].fillna(0).astype(float) >= float(price_min)]
    if price_max is not None:
        filtered = filtered[filtered["price_vnd"].fillna(0).astype(float) <= float(price_max)]

    year_min = filters.get("year_min")
    year_max = filters.get("year_max")
    if year_min is not None:
        filtered = filtered[filtered["release_year"].fillna(0).astype(int) >= int(year_min)]
    if year_max is not None:
        filtered = filtered[filtered["release_year"].fillna(0).astype(int) <= int(year_max)]

    return filtered


@st.cache_data(show_spinner=False)
def build_catalog(_indexes: Dict[int, dict]) -> pd.DataFrame:
    frames = []
    for chunk_id, index_data in _indexes.items():
        df = index_data["games_df"].copy()
        df["chunk_id"] = chunk_id
        frames.append(df)

    if not frames:
        return pd.DataFrame()

    catalog = pd.concat(frames, ignore_index=True)
    catalog = catalog.drop_duplicates(subset=["steam_appid"], keep="first")

    if "profile_text" not in catalog.columns:
        catalog["profile_text"] = catalog.apply(build_profile_text, axis=1)

    catalog["review_quality"] = catalog["steam_review_positive_ratio"].apply(_safe_ratio)
    return catalog


def hybrid_rerank(candidates: pd.DataFrame, semantic_df: pd.DataFrame, top_n: int) -> pd.DataFrame:
    """Blend scores at one row per valid App ID, preserving raw input fields."""
    if candidates.empty:
        return candidates

    base = candidates.copy().reset_index(drop=True)
    ids = pd.Series(
        [_business_key(value) for value in base.get("steam_appid", pd.Series(index=base.index, dtype=object))],
        index=base.index, dtype=object,
    )
    base = base.loc[ids.notna()].copy()
    if base.empty:
        return base
    if "faiss_structured" not in base.columns:
        if "similarity_score" in base.columns:
            base["faiss_structured"] = base["similarity_score"].astype(float).clip(0.0, 1.0)
        elif "relevance" in base.columns:
            max_rel = max(1.0, float(base["relevance"].max()))
            base["faiss_structured"] = (base["relevance"].astype(float) / max_rel).clip(0.0, 1.0)
        else:
            base["faiss_structured"] = 0.0

    # A single best semantic score per canonical ID prevents one-to-many joins.
    semantic_scores = {}
    if semantic_df is not None and {"steam_appid", "semantic_score"}.issubset(semantic_df.columns):
        for value, score in zip(semantic_df["steam_appid"], semantic_df["semantic_score"]):
            appid = _business_key(value)
            if appid is not None:
                semantic_scores[appid] = max(semantic_scores.get(appid, 0.0), _safe_ratio(score))
    base["semantic_score"] = ids.loc[base.index].map(semantic_scores).fillna(0.0).astype(float)

    # Prefer valid Gold/lexical ratios per row, then the FAISS record alias.
    review_quality = pd.Series(index=base.index, dtype=float)
    for column in ("steam_review_positive_ratio", "review_positive_ratio"):
        if column in base:
            ratios = base[column].map(_finite_number)
            review_quality = review_quality.fillna(ratios.where(ratios.between(0.0, 1.0)))
    base["review_quality"] = review_quality.fillna(0.0)

    base["final_score"] = (
        HYBRID_WEIGHTS["faiss_structured"] * base["faiss_structured"]
        + HYBRID_WEIGHTS["semantic"] * base["semantic_score"]
        + HYBRID_WEIGHTS["review_quality"] * base["review_quality"]
    )

    ordered = base.sort_values("final_score", ascending=False, kind="stable")
    unique = ~ids.loc[ordered.index].duplicated()
    ranked = ordered.loc[unique].head(top_n).reset_index(drop=True)
    ranked["rank"] = range(1, len(ranked) + 1)
    return ranked


def search_hybrid(query: str, indexes: Dict[int, dict], top_n: int = 10) -> pd.DataFrame:
    """Use lexical matches as the candidate pool, then rerank them with semantic retrieval."""
    lexical = search_across_all_chunks(query, indexes, max_results=max(40, top_n * 4))
    semantic_df = semantic_search(query, top_n=max(50, top_n * 4))

    if lexical.empty and semantic_df is not None and "steam_appid" in semantic_df and not semantic_df.empty:
        # Recover catalog candidates for semantic-only hits.
        catalog = build_catalog(indexes)
        if "steam_appid" in catalog:
            hit_ids = {_business_key(value) for value in semantic_df["steam_appid"]}
            hit_ids.discard(None)
            matches = [_business_key(value) in hit_ids for value in catalog["steam_appid"]]
            lexical = catalog.loc[matches].copy()

    return hybrid_rerank(lexical, semantic_df, top_n=top_n)


def recommend_hybrid(seed_game: str, indexes: Dict[int, dict], top_n: int = 10) -> Tuple[pd.Series, pd.DataFrame]:
    if not seed_game.strip():
        return None, pd.DataFrame()
    source_game, recs = recommend_with_faiss(seed_game, indexes, top_n=max(30, top_n * 3))
    if recs is None:
        return None, pd.DataFrame()

    rec_df = pd.DataFrame(recs)
    if rec_df.empty:
        return source_game, rec_df

    semantic_df = semantic_search(build_profile_text(source_game), top_n=max(50, top_n * 4))
    reranked = hybrid_rerank(rec_df, semantic_df, top_n=top_n)
    return source_game, reranked


def _heuristic_intent(message: str) -> str:
    text = _fold_accents_keep_symbols(str(message or "")).casefold()
    # A title after an explicit seed cue may itself contain "vs" or "compare".
    if extract_seed_game_name(message):
        return "recommend_from_game"
    if re.search(r"\b(?:so sanh|compare|vs)\b", text):
        return "compare_games"
    if re.search(r"\b(?:phan tich|analyze|thong ke|xu huong)\b", text):
        return "analyze_data"
    return "search_game"


def _llm_chat(messages: List[dict], max_tokens: int = 300) -> str:
    if not is_ollama_available():
        return ""

    try:
        url = f"{OLLAMA_BASE_URL}/api/chat"
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": messages,
            "stream": False,
            "options": {
                "num_predict": max_tokens,
                "temperature": 0.2,
            },
        }


        resp = requests.post(url, json=payload, timeout=60)


        resp.raise_for_status()

        data = resp.json()
        content = data.get("message", {}).get("content", "").strip()
        done_reason = data.get("done_reason", "")


        return content

    except Exception:
        return ""


def _llm_chat_with_meta(messages: List[dict], max_tokens: int = 300) -> tuple[str, str]:
    if not is_ollama_available():
        return "", ""

    try:
        url = f"{OLLAMA_BASE_URL}/api/chat"
        payload = {
            "model": OLLAMA_CHAT_MODEL,
            "messages": messages,
            "stream": False,
            "options": {
                "num_predict": max_tokens,
                "temperature": 0.2,
            },
        }


        resp = requests.post(url, json=payload, timeout=60)


        resp.raise_for_status()
        data = resp.json()
        content = data.get("message", {}).get("content", "").strip()
        done_reason = str(data.get("done_reason", "") or "")


        return content, done_reason
    except Exception:
        return "", ""


def route_intent(message: str, *, catalog: pd.DataFrame | None = None) -> str:
    """Use the optional catalog to resolve bare 'A voi B' comparisons safely."""
    baseline = _heuristic_intent(message)
    if (baseline == "search_game" and catalog is not None
            and {"name", "steam_appid"}.issubset(catalog.columns)
            and _normalize_text(message).count(" voi ") == 1):
        left, right = parse_compare_names(message)
        if left and right and left != right:
            names = catalog["name"].fillna("").astype(str).map(_normalize_text)
            if names.eq(left).sum() == names.eq(right).sum() == 1:
                matched_ids = catalog.loc[names.isin([left, right]), "steam_appid"]
                ids = {_business_key(value) for value in matched_ids}
                if len(ids) == 2 and None not in ids:
                    baseline = "compare_games"

    # Explicit tools take precedence. An LLM label alone cannot supply a seed
    # or turn a search constraint into a comparison.
    if baseline in {"compare_games", "recommend_from_game"}:
        return baseline
    if not USE_LLM_INTENT_PARSING or not is_ollama_available():
        return baseline

    parser_prompt = [
        {
            "role": "system",
            "content": "Trả về DUY NHẤT một nhãn intent: search_game hoặc analyze_data.",
        },
        {"role": "user", "content": message},
    ]
    label = _llm_chat(parser_prompt, max_tokens=15).strip().casefold()
    return label if label in {"search_game", "analyze_data"} else baseline


def _compact_candidate_context(results: pd.DataFrame, limit: int = 4) -> str:
    lines = []
    cols = ["name", "tags", "genres", "price_category", "review_positive_ratio"]
    sample = results.head(limit)

    for idx, row in sample.iterrows():
        tags = str(row.get("tags", "") or "")
        top_tags = [t.strip() for t in tags.split(",") if t.strip()][:4]
        genres = str(row.get("genres", "") or "")
        price_category = str(row.get("price_category", "") or "")
        review_ratio = _safe_ratio(row.get("review_positive_ratio", 0.0))

        lines.append(
            f"- {row.get('name', '')}: "
            f"tags={', '.join(top_tags)}, "
            f"genres={genres}, "
            f"gia={price_category}, "
            f"review={review_ratio * 100:.0f}%"
        )

    return "\n".join(lines)


def _clean_explanation_output(text: str) -> str:
    if not text:
        return text

    banned_prefixes = ("user query:", "source game:", "top candidates:")
    cleaned_lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith(banned_prefixes):
            continue
        cleaned_lines.append(line)

    cleaned = "\n".join(cleaned_lines).strip()
    return cleaned or text.strip()


def _looks_incomplete(text: str) -> bool:
    if not text:
        return True
    stripped = text.strip()
    if len(stripped) < 30:
        return True
    if stripped.endswith((".", "!", "?", '"', "'.")):
        return False

    last_word = stripped.split()[-1].lower()
    dangling_words = {
        "cho", "với", "la", "là", "để", "và", "hoặc", "nhưng", "rằng", "trong", "về"
    }
    return last_word in dangling_words


def _deterministic_explanation(source_game_name: str, top_rows: pd.DataFrame) -> str:
    top_rows = top_rows.head(4)
    game_names = [str(x) for x in top_rows["name"].tolist()]
    if not game_names:
        return "Hiện chưa có đủ dữ liệu để giải thích gợi ý."

    mention_names = ", ".join(game_names[:3])
    avg_review = top_rows["review_positive_ratio"].apply(_safe_ratio).mean()

    tag_tokens = []
    for _, row in top_rows.iterrows():
        tags = str(row.get("tags", "") or "")
        tag_tokens.extend([t.strip() for t in tags.split(",") if t.strip()][:4])

    common_tags = []
    seen = set()
    for tag in tag_tokens:
        key = tag.lower()
        if key in seen:
            continue
        seen.add(key)
        common_tags.append(tag)
        if len(common_tags) >= 3:
            break

    tag_phrase = ", ".join(common_tags) if common_tags else "lối chơi và nhóm thể loại tương đồng"

    return (
        f"Từ game gốc {source_game_name}, hệ thống gợi ý các lựa chọn như {mention_names} vì có độ tương đồng cao về tag và phong cách chơi, "
        f"đặc biệt ở các đặc trưng như {tag_phrase}. Ngoài ra, các game này cũng giữ mức đánh giá tích cực khá tốt "
        f"(trung bình khoảng {avg_review * 100:.0f}%), nên phù hợp để mở rộng trải nghiệm từ sở thích ban đầu. "
        "Danh sách được xếp hạng theo mô hình hybrid, kết hợp giữa tương đồng thuộc tính và tương đồng ngữ nghĩa."
    )


def explain_recommendations(user_query: str, source_game_name: str, results: pd.DataFrame) -> str:
    if results.empty:
        return "Không tìm thấy kết quả phù hợp."

    top_rows = results.head(5)[["name", "final_score", "tags", "genres", "price_category", "review_positive_ratio"]]
    compact_candidates = _compact_candidate_context(top_rows, limit=4)

    messages = [
        {
            "role": "system",
            "content": (
                "Bạn là trợ lý gợi ý game. "
                "Nhiệm vụ của bạn là TÓM TẮT LÝ DO GỢI Ý, KHÔNG PHẢI SO SÁNH GAME. "
                "Viết 1 đoạn văn tự nhiên gồm 4-5 câu, đủ để giải thích rõ lý do gợi ý. "
                "KHÔNG được viết theo kiểu hỏi đáp, KHÔNG lập bảng so sánh, KHÔNG viết tiêu đề. "
                "KHÔNG được mở đầu bằng các câu như 'Tôi có thể giúp bạn so sánh'. "
                "KHÔNG lặp lại nguyên văn nội dung của truy vấn người dùng hoặc tên game gốc. "
                "Hãy nêu ít nhất 2 lý do chung từ dữ liệu, ví dụ: tag, thể loại, mức giá hoặc đánh giá người chơi. "
                "Hãy nhắc tên nhiều nhất 3 game cụ thể trong danh sách gợi ý. "
                "Hãy viết tự nhiên, mạch lạc, ngắn gọn, kết thúc bằng dấu câu."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Ngữ cảnh truy vấn: {user_query}\n"
                f"Game gốc: {source_game_name}\n"
                f"Ứng viên để gợi ý:\n{compact_candidates}\n\n"
                "Yêu cầu bắt buộc: "
                "Viết 1 đoạn tổng hợp recommendation 4-5 câu, giải thích lí do các game được gợi ý phù hợp với game gốc, không so sánh từng cặp game, "
                "không lặp lại danh sách ý nguyên, chỉ nêu điểm chung và mức độ phù hợp."
            ),
        },
    ]

    text, done_reason = _llm_chat_with_meta(messages, max_tokens=230)
    text = _clean_explanation_output(text)
    if text and done_reason != "length" and not _looks_incomplete(text):
        return text

    # Retry once with a tighter completion instruction if the first answer is truncated.
    retry_messages = [
        {
            "role": "system",
            "content": "Viết duy nhất 3 câu hoàn chỉnh bằng tiếng Việt tự nhiên, không dùng format so sánh.",
        },
        {
            "role": "user",
            "content": (
                f"Game gốc: {source_game_name}\n"
                f"Ứng viên:\n{compact_candidates}\n"
                "Hãy viết lại đoạn recommendation summary hoàn chỉnh, tự nhiên, mạch lạc, ngắn gọn, kết thúc bằng dấu câu."
            ),
        },
    ]

    retry_text, retry_done_reason = _llm_chat_with_meta(retry_messages, max_tokens=180)
    retry_text = _clean_explanation_output(retry_text)
    if retry_text and retry_done_reason != "length" and not _looks_incomplete(retry_text):
        return retry_text

    return _deterministic_explanation(source_game_name, top_rows)


def _deterministic_compare_explanation(game1: pd.Series, game2: pd.Series) -> str:
    name1 = str(game1.get("name", "Game 1"))
    name2 = str(game2.get("name", "Game 2"))

    price1 = float(game1.get("price_vnd", 0) or 0)
    price2 = float(game2.get("price_vnd", 0) or 0)
    review1 = _safe_ratio(game1.get("steam_review_positive_ratio", 0.0))
    review2 = _safe_ratio(game2.get("steam_review_positive_ratio", 0.0))
    year1 = int(game1.get("release_year", 0) or 0)
    year2 = int(game2.get("release_year", 0) or 0)

    cheaper = name1 if price1 < price2 else (name2 if price2 < price1 else "cả hai")
    better_review = name1 if review1 > review2 else (name2 if review2 > review1 else "cả hai")
    newer = name1 if year1 > year2 else (name2 if year2 > year1 else "cả hai")

    return (
        f"So sánh nhanh giữa {name1} và {name2}: mức giá hiện tại cho thấy {cheaper} dễ tiếp cận hơn về chi phí. "
        f"Về đánh giá người chơi, {better_review} đang có tỷ lệ tích cực nổi trội hơn. "
        f"Xét thời điểm phát hành, {newer} mới hơn về mặt thế hệ nội dung. "
        "Bạn có thể chọn theo ưu tiên chính: tiết kiệm ngân sách, độ hài lòng cộng đồng, hoặc trải nghiệm mới hơn."
    )


def explain_comparison(user_query: str, game1: pd.Series, game2: pd.Series) -> str:
    name1 = str(game1.get("name", ""))
    name2 = str(game2.get("name", ""))
    price1 = int(float(game1.get("price_vnd", 0) or 0))
    price2 = int(float(game2.get("price_vnd", 0) or 0))
    review1 = _safe_ratio(game1.get("steam_review_positive_ratio", 0.0))
    review2 = _safe_ratio(game2.get("steam_review_positive_ratio", 0.0))
    year1 = int(game1.get("release_year", 0) or 0)
    year2 = int(game2.get("release_year", 0) or 0)

    messages = [
        {
            "role": "system",
            "content": (
                "Bạn là trợ lý phân tích game. "
                "Viết đúng 1 đoạn tiếng Việt tự nhiên gồm 4-5 câu để tóm tắt kết quả so sánh 2 game. "
                "Không dùng markdown, không dùng bullet, không lập bảng, không mở đầu bằng 'Tôi có thể'. "
                "Nêu rõ ít nhất 3 ý: giá, tỷ lệ review tích cực, năm phát hành. "
                "GIỮ NGUYÊN giá trị VND như dữ liệu đầu vào, KHÔNG đổi đơn vị, KHÔNG chia 10, KHÔNG thêm dấu thập phân kiểu 146.9755. "
                "Kết thúc bằng 1 câu gợi ý chọn game theo nhu cầu."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Ngữ cảnh người dùng: {user_query}\n"
                f"Game A: {name1}, price_vnd_raw={price1}, price_vnd_text={price1:,}, review_positive_ratio={review1:.4f}, release_year={year1}\n"
                f"Game B: {name2}, price_vnd_raw={price2}, price_vnd_text={price2:,}, review_positive_ratio={review2:.4f}, release_year={year2}\n"
                "Hãy tóm tắt ngắn gọn điểm khác biệt chính bằng văn phong tự nhiên cho người dùng cuối."
            ),
        },
    ]

    text, done_reason = _llm_chat_with_meta(messages, max_tokens=220)
    text = _clean_explanation_output(text)
    if text and done_reason != "length" and not _looks_incomplete(text):
        return text

    return _deterministic_compare_explanation(game1, game2)


def _deterministic_analysis_explanation(user_query: str, data: pd.DataFrame) -> str:
    if data.empty:
        return "Không có dữ liệu phù hợp để phân tích theo điều kiện hiện tại."

    sample_size = len(data)
    avg_review = data["steam_review_positive_ratio"].apply(_safe_ratio).mean()
    median_review = data["steam_review_positive_ratio"].apply(_safe_ratio).median()
    avg_price = float(data["price_vnd"].fillna(0).mean())
    median_price = float(data["price_vnd"].fillna(0).median())

    categories = data["categories_cleaned"].fillna("").astype(str).str.split(",").explode().str.strip()
    categories = categories[categories != ""]
    top_categories = categories.value_counts().head(3).index.tolist() if not categories.empty else []
    category_text = ", ".join(top_categories) if top_categories else "không có category nổi trội"

    high_review_share = (data["steam_review_positive_ratio"].apply(_safe_ratio) >= 0.9).mean() * 100

    return (
        f"Theo điều kiện bạn đưa ra, tập dữ liệu phân tích hiện có {sample_size} game. "
        f"Mặt bằng review đang khá tích cực với trung bình {avg_review * 100:.0f}% và trung vị {median_review * 100:.0f}%, trong đó khoảng {high_review_share:.0f}% game đạt từ 90% review tích cực trở lên. "
        f"Về giá, mức trung bình khoảng {avg_price:,.0f} VND và trung vị khoảng {median_price:,.0f} VND, phản ánh tương đối rõ độ phân tán theo ngân sách. "
        f"Nhóm category nổi bật nhất hiện tại là {category_text}; bạn có thể tiếp tục khoanh vùng theo 1 category cụ thể để phân tích sâu hơn."
    )


def explain_analysis(user_query: str, data: pd.DataFrame) -> str:
    if data.empty:
        return "Không có dữ liệu phù hợp để phân tích theo điều kiện hiện tại."

    review_series = data["steam_review_positive_ratio"].apply(_safe_ratio)
    price_series = data["price_vnd"].fillna(0)
    categories = data["categories_cleaned"].fillna("").astype(str).str.split(",").explode().str.strip()
    categories = categories[categories != ""]
    top_categories = categories.value_counts().head(5)
    top_category_lines = "\n".join(
        [f"- {name}: {count}" for name, count in top_categories.items()]
    ) if not top_categories.empty else "- (none)"

    compact = (
        f"sample_size={len(data)}\n"
        f"avg_review={review_series.mean():.4f}\n"
        f"median_review={review_series.median():.4f}\n"
        f"share_review_ge_0_9={(review_series >= 0.9).mean():.4f}\n"
        f"avg_price_vnd={float(price_series.mean()):.0f}\n"
        f"median_price_vnd={float(price_series.median()):.0f}\n"
        f"share_free_to_play={(price_series <= 0).mean():.4f}\n"
        f"top_categories:\n{top_category_lines}"
    )

    messages = [
        {
            "role": "system",
            "content": (
                "Bạn là trợ lý phân tích dữ liệu game. "
                "Viết đúng 1 đoạn tiếng Việt tự nhiên gồm 4-5 câu để tóm tắt kết quả phân tích thị trường game theo tập dữ liệu đã lọc. "
                "Không dùng markdown, không bullet, không lập bảng. "
                "Phải nêu: quy mô mẫu, xu hướng review, xu hướng giá, và category nổi bật. "
                "Không tập trung vào việc giới thiệu game cụ thể. "
                "Kết thúc bằng 1 câu gợi ý bước phân tích tiếp theo cho người dùng."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Ngữ cảnh truy vấn: {user_query}\n"
                f"Chỉ số tổng hợp:\n{compact}\n"
                "Hãy viết đoạn summary tự nhiên, dễ đọc cho người dùng cuối."
            ),
        },
    ]

    text, done_reason = _llm_chat_with_meta(messages, max_tokens=240)
    text = _clean_explanation_output(text)
    if text and done_reason != "length" and not _looks_incomplete(text):
        return text

    return _deterministic_analysis_explanation(user_query, data)


def extract_analyze_game_name(message: str) -> str:
    folded = _fold_accents_keep_symbols(message)
    triggers = [
        "phan tich game",
        "phan tich ve game",
        "danh gia game",
        "review game",
        "analyze game",
    ]

    for phrase in triggers:
        marker = f"{phrase} "
        if marker in folded:
            return folded.split(marker, 1)[1].strip(" :,-")
        if folded.startswith(phrase):
            return folded[len(phrase):].strip(" :,-")

    return ""


def _deterministic_single_game_analysis(game: pd.Series) -> str:
    name = str(game.get("name", "game này"))
    review = _safe_ratio(game.get("steam_review_positive_ratio", 0.0))
    release_year = int(game.get("release_year", 0) or 0)
    price_vnd = int(float(game.get("price_vnd", 0) or 0))
    price_category = str(game.get("price_category_vnd", game.get("price_category", "")) or "")

    categories = str(game.get("categories_cleaned", "") or "")
    genres = str(game.get("genres_cleaned", game.get("genres", "")) or "")
    tags = str(game.get("tags_cleaned", game.get("tags", "")) or "")

    key_tags = [t.strip() for t in tags.split(",") if t.strip()][:3]
    tag_text = ", ".join(key_tags) if key_tags else "không có tag nổi bật"

    return (
        f"{name} là một tựa game phát hành vào năm {release_year}, thuộc nhóm thể loại {genres or categories or 'đa thể loại'}. "
        f"Mức giá hiện tại khoảng {price_vnd:,.0f} VND ({price_category or 'chưa phân nhóm giá rõ'}), phù hợp để cân nhắc theo ngân sách cá nhân. "
        f"Tỷ lệ đánh giá tích cực của người chơi đang ở mức {review * 100:.0f}%, cho thấy mức độ hài lòng cộng đồng khá rõ rệt. "
        f"Về phong cách trải nghiệm, game nổi bật ở các tag như {tag_text}; bạn có thể dựa trên các đặc trưng này để so sánh với các game tương tự."
    )


def explain_single_game_analysis(user_query: str, game: pd.Series) -> str:
    name = str(game.get("name", ""))
    review = _safe_ratio(game.get("steam_review_positive_ratio", 0.0))
    release_year = int(game.get("release_year", 0) or 0)
    price_vnd = float(game.get("price_vnd", 0) or 0)
    price_category = str(game.get("price_category_vnd", game.get("price_category", "")) or "")
    categories = str(game.get("categories_cleaned", "") or "")
    genres = str(game.get("genres_cleaned", game.get("genres", "")) or "")
    tags = str(game.get("tags_cleaned", game.get("tags", "")) or "")
    developers = str(game.get("developers_cleaned", game.get("developers", "")) or "")
    publishers = str(game.get("publishers_cleaned", game.get("publishers", "")) or "")

    messages = [
        {
            "role": "system",
            "content": (
                "Bạn là trợ lý phân tích 1 game cụ thể. "
                "Viết đúng 1 đoạn tiếng Việt tự nhiên 4-5 câu, không markdown, không bullet. "
                "Nêu rõ: định vị thể loại/phong cách, giá, review cộng đồng, và một nhận định ngắn về độ phù hợp người chơi."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Ngữ cảnh truy vấn: {user_query}\n"
                f"name={name}\n"
                f"release_year={release_year}\n"
                f"price_vnd={price_vnd:.0f}\n"
                f"price_category={price_category}\n"
                f"review_positive_ratio={review:.4f}\n"
                f"genres={genres}\n"
                f"categories={categories}\n"
                f"tags={tags}\n"
                f"developers={developers}\n"
                f"publishers={publishers}\n"
                "Hãy viết phân tích ngắn gọn, dễ hiểu cho người dùng cuối."
            ),
        },
    ]

    text, done_reason = _llm_chat_with_meta(messages, max_tokens=220)
    text = _clean_explanation_output(text)
    if text and done_reason != "length" and not _looks_incomplete(text):
        return text

    return _deterministic_single_game_analysis(game)


def parse_compare_names(message: str) -> Tuple[str, str]:
    normalized = re.sub(
        r"^(?:(?:hay|lam on|please)\s+)?(?:so sanh(?: giua| game)?|compare|giua)\s+",
        "", _normalize_text(message),
    )

    for sep in [" vs ", " voi ", " with ", " va ", " and "]:
        if sep in normalized:
            left, right = normalized.split(sep, 1)
            return left.strip(), right.strip()
    return "", ""


def extract_seed_game_name(message: str) -> str:
    """Return only a seed after an explicit cue. Discovery prompts have no seed."""
    folded = _fold_accents_keep_symbols(message)
    prefix = re.match(
        r"^(?:(?:hay|lam on|please)\s+)?"
        r"(?:goi y tu game|[dđ]e xuat tu game|recommend from game|similar to"
        r"|(?:(?:tim|goi y) )?game tuong tu(?: voi| nhu)?"
        r"|tuong (?:tu|dong)(?: voi| nhu)?|giong(?: game| nhu)?)\b",
        folded,
    )
    return folded[prefix.end():].strip(" :,-") if prefix else ""


def pick_game_by_name(catalog: pd.DataFrame, name_query: str) -> pd.Series:
    if not name_query:
        return None

    query = str(name_query).strip()
    if not query:
        return None

    names = catalog["name"].fillna("").astype(str)
    normalized_query = _normalize_text(query)
    normalized_names = names.map(_normalize_text)
    matches = catalog[names.str.casefold().eq(query.casefold())]
    if matches.empty and normalized_query:
        matches = catalog[normalized_names.eq(normalized_query)]
    if matches.empty:
        matches = catalog[names.str.contains(query, case=False, na=False, regex=False)]
    if matches.empty and normalized_query:
        matches = catalog[normalized_names.str.contains(normalized_query, na=False, regex=False)]
    if matches.empty:
        return None
    return matches.iloc[0]
