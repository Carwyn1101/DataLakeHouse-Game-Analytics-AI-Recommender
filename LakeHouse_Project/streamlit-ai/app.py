"""Hybrid Game Recommender + Chatbot."""

import altair as alt
import pandas as pd
import streamlit as st

from src.auto_builder import auto_build_indexes_if_needed
from src.faiss_manager import (
    get_catalog_dataframe,
    load_all_faiss_indexes,
    recommend_with_faiss,
    search_across_all_chunks,
)
from src.hybrid_chat import (
    apply_structured_filters,
    explain_analysis,
    explain_comparison,
    explain_recommendations,
    explain_single_game_analysis,
    extract_seed_game_name,
    extract_analyze_game_name,
    parse_user_filters,
    parse_compare_names,
    pick_game_by_name,
    recommend_hybrid,
    route_intent,
    search_hybrid,
)
from src.semantic_manager import ensure_semantic_index


def _records(df: pd.DataFrame, columns: list[str]) -> list[dict]:
    if df is None or df.empty:
        return []
    safe_cols = [col for col in columns if col in df.columns]
    if not safe_cols:
        return []
    return df[safe_cols].to_dict(orient="records")


def _prepare_recommend_display(df: pd.DataFrame) -> pd.DataFrame:
    """Format a copy; explanations and ranking keep raw ratios in [0, 1]."""
    display = df.copy()
    if "final_score" not in display and "similarity_score" in display:
        display["final_score"] = display["similarity_score"]
    if "review_positive_ratio" in display:
        display["review_positive_ratio"] = pd.to_numeric(
            display["review_positive_ratio"], errors="coerce"
        ) * 100
    return display


def _prepare_analysis_display(rows: list[dict]) -> pd.DataFrame:
    """Localize presentation only; rows remain numeric input to charts/tools."""
    display = pd.DataFrame(rows).copy()
    if display.empty or "metric" not in display:
        return display
    labels = {
        "name": "Tên Game", "release_year": "Năm Phát Hành",
        "price_vnd": "Giá (VND)", "price_category": "Phân Khúc Giá",
        "review_positive_ratio": "Review Tích Cực", "genres": "Thể Loại",
        "categories": "Categories", "sample_size": "Số lượng game (mẫu)",
        "avg_review_positive_ratio": "Trung bình Review Tích cực",
        "median_review_positive_ratio": "Trung vị Review Tích cực",
        "share_review_ge_0_9": "Tỷ lệ game >= 90% Review",
        "share_free_to_play": "Tỷ lệ Game Miễn phí",
        "avg_price_vnd": "Trung bình Giá (VND)",
        "median_price_vnd": "Trung vị Giá (VND)",
    }
    ratios = {"review_positive_ratio", "avg_review_positive_ratio",
              "median_review_positive_ratio", "share_review_ge_0_9", "share_free_to_play"}
    prices = {"price_vnd", "avg_price_vnd", "median_price_vnd"}
    value_columns = [c for c in display if c not in {"metric", "label"}]
    for idx, row in display.iterrows():
        metric = str(row["metric"])
        for column in value_columns:
            value = row[column]
            if pd.isna(value):
                formatted = "N/A"
            elif metric in ratios:
                formatted = f"{float(value) * 100:.1f}%"
            elif metric in prices:
                formatted = f"{float(value):,.0f}"
            elif metric in {"release_year", "sample_size"} or metric.startswith("top_category_"):
                formatted = f"{float(value):.0f}"
            else:
                formatted = str(value)
            # Object dtype allows formatted strings with pandas 1.x and 3.x.
            if display[column].dtype != object:
                display[column] = display[column].astype(object)
            display.at[idx, column] = formatted
        display.at[idx, "metric"] = labels.get(metric, metric.replace("top_category_", "Top Category "))
    return display


def _render_analyze_charts(summary_rows: list[dict]):
    if not summary_rows:
        return

    summary_df = pd.DataFrame(summary_rows)
    if summary_df.empty or "metric" not in summary_df.columns or "value" not in summary_df.columns:
        return

    ratio_metrics = [
        "avg_review_positive_ratio",
        "median_review_positive_ratio",
        "share_review_ge_0_9",
        "share_free_to_play",
    ]
    ratio_df = summary_df[summary_df["metric"].isin(ratio_metrics)].copy()
    if not ratio_df.empty:
        ratio_df["value"] = pd.to_numeric(ratio_df["value"], errors="coerce").fillna(0.0)
        ratio_df["value"] = ratio_df["value"].clip(0.0, 1.0)
        ratio_df["metric_label"] = ratio_df["metric"].replace(
            {
                "avg_review_positive_ratio": "Avg review ratio",
                "median_review_positive_ratio": "Median review ratio",
                "share_review_ge_0_9": "Share review >= 0.9",
                "share_free_to_play": "Share free-to-play",
            }
        )
        st.caption("Tỷ lệ chính trong segment")
        ratio_line = alt.Chart(ratio_df).mark_line(point=True, strokeWidth=3).encode(
            x=alt.X("metric_label:N", axis=alt.Axis(labelAngle=0, labelLimit=240, title=None), sort=None),
            y=alt.Y("value:Q", title="ratio", scale=alt.Scale(domain=[0, 1])),
            tooltip=["metric_label:N", alt.Tooltip("value:Q", format=".4f")],
        )
        ratio_area = alt.Chart(ratio_df).mark_area(opacity=0.2).encode(
            x=alt.X("metric_label:N", sort=None),
            y=alt.Y("value:Q", scale=alt.Scale(domain=[0, 1])),
        )
        st.altair_chart((ratio_area + ratio_line), use_container_width=True)

    price_metrics = ["avg_price_vnd", "median_price_vnd"]
    price_df = summary_df[summary_df["metric"].isin(price_metrics)].copy()
    if not price_df.empty:
        price_df["value"] = pd.to_numeric(price_df["value"], errors="coerce").fillna(0)
        price_df["metric_label"] = price_df["metric"].replace(
            {"avg_price_vnd": "Average price", "median_price_vnd": "Median price"}
        )
        st.caption("Mặt bằng giá (VND)")
        price_bar = alt.Chart(price_df).mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6).encode(
            x=alt.X("metric_label:N", axis=alt.Axis(labelAngle=0, labelLimit=240, title=None), sort=None),
            y=alt.Y("value:Q", title="VND"),
            color=alt.Color("metric_label:N", legend=None),
            tooltip=["metric_label:N", alt.Tooltip("value:Q", format=",.0f")],
        )

        price_share_df = price_df[["metric_label", "value"]].copy()
        total_price = float(price_share_df["value"].sum())
        if total_price > 0:
            price_share_df["share"] = price_share_df["value"] / total_price
        else:
            price_share_df["share"] = 0.0

        price_donut = alt.Chart(price_share_df).mark_arc(innerRadius=45).encode(
            theta=alt.Theta("value:Q"),
            color=alt.Color("metric_label:N", title="Price metric"),
            tooltip=["metric_label:N", alt.Tooltip("value:Q", format=",.0f"), alt.Tooltip("share:Q", format=".1%")],
        )

        price_col1, price_col2 = st.columns(2)
        with price_col1:
            st.altair_chart(price_bar, use_container_width=True)
        with price_col2:
            st.altair_chart(price_donut, use_container_width=True)

    category_df = summary_df[summary_df["metric"].str.startswith("top_category_", na=False)].copy()
    if not category_df.empty:
        if "label" in category_df.columns:
            category_df["count"] = pd.to_numeric(category_df["value"], errors="coerce").fillna(0).astype(int)
            category_df = category_df.rename(columns={"label": "category"})
            category_df = category_df[["category", "count"]]
            total_count = int(category_df["count"].sum())
            if total_count > 0:
                category_df["share"] = category_df["count"] / total_count
            else:
                category_df["share"] = 0.0
            st.caption("Category nổi bật")
            category_chart = alt.Chart(category_df).mark_bar(cornerRadiusTopLeft=6, cornerRadiusTopRight=6).encode(
                x=alt.X("category:N", axis=alt.Axis(labelAngle=0, labelLimit=240, title=None), sort=None),
                y=alt.Y("count:Q", title="count"),
                color=alt.Color("category:N", legend=None),
                tooltip=["category:N", "count:Q", alt.Tooltip("share:Q", format=".1%")],
            )

            category_pie = alt.Chart(category_df).mark_arc(innerRadius=20).encode(
                theta=alt.Theta("count:Q"),
                color=alt.Color("category:N", title="Category"),
                tooltip=["category:N", "count:Q", alt.Tooltip("share:Q", format=".1%")],
            )

            cat_col1, cat_col2 = st.columns(2)
            with cat_col1:
                st.altair_chart(category_chart, use_container_width=True)
            with cat_col2:
                st.altair_chart(category_pie, use_container_width=True)


def render_chat_entry(entry: dict):
    with st.chat_message(entry.get("role", "assistant")):
        if entry.get("role") == "user":
            st.markdown(entry.get("content", ""))
            return
        intent = entry.get("intent")
        if intent:
            st.markdown(f"<span class='chat-pill'>tool: {intent}</span>", unsafe_allow_html=True)
        if entry.get("content"):
            st.write(entry["content"])
        if entry.get("seed_game"):
            st.caption(f"Seed game: {entry['seed_game']}")
        rows = entry.get("table_rows") or []
        if rows:
            display = (_prepare_analysis_display(rows)
                       if intent in {"analyze_data", "compare_games"} else pd.DataFrame(rows))
            with st.expander(entry.get("table_title", "Chi tiết kết quả"), expanded=False):
                st.dataframe(display, hide_index=True, use_container_width=True,
                             column_config=entry.get("column_config"))
                if intent == "analyze_data":
                    try:
                        _render_analyze_charts(rows)
                    except Exception:
                        # Do not expose data-bearing exception text to the UI or logs.
                        st.warning("Không hiển thị được biểu đồ phân tích.")


def _queue_chat_submit():
    msg = str(st.session_state.get("chatbot_input", "")).strip()
    if msg:
        st.session_state.pending_user_msg = msg
        st.session_state.chat_submit_requested = True

st.set_page_config(
    page_title="Hybrid Game Recommender",
    page_icon="??",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
.main-header {
    font-size: 2.6rem;
    font-weight: 800;
    text-align: center;
    color: #0f172a;
    margin-bottom: 0.2rem;
}
.sub-header {
    text-align: center;
    color: #334155;
    margin-bottom: 1.3rem;
}
.chat-pill {
    padding: 0.45rem 0.7rem;
    border-radius: 999px;
    background: #e2e8f0;
    color: #0f172a;
    font-size: 0.85rem;
    display: inline-block;
    margin-right: 0.4rem;
}
</style>
""",
    unsafe_allow_html=True,
)

st.markdown('<div class="main-header">Hybrid Recommender + Chatbot</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="sub-header">Structured FAISS retrieval + Semantic search + AI explainability</div>',
    unsafe_allow_html=True,
)

build_success = auto_build_indexes_if_needed()
if not build_success:
    st.error("Cannot build indexes. Check Hive/MinIO and Gold layer data.")
    st.stop()

indexes = load_all_faiss_indexes()
if indexes is None:
    st.stop()

catalog = get_catalog_dataframe(indexes)
semantic_stats = (
    ensure_semantic_index(catalog) if not catalog.empty
    else {"indexed": 0, "new": 0, "status": "empty", "message": "Catalog is empty."}
)

with st.sidebar:
    st.subheader("System")
    total_games = len(catalog)
    st.success(
        f"Loaded chunks: {len(indexes)}\n\n"
        f"Catalog games: {total_games:,}\n\n"
        f"Semantic indexed: {semantic_stats['indexed']:,}"
    )
    if semantic_stats["new"] > 0:
        st.info(f"New semantic vectors added: {semantic_stats['new']:,}")
    if semantic_stats.get("status") == "ollama_unavailable":
        st.warning(semantic_stats.get("message", "Semantic backend unavailable."))
    elif semantic_stats.get("status") == "partial":
        st.info(semantic_stats.get("message", "Semantic index is partially built."))
    elif semantic_stats.get("message"):
        st.caption(semantic_stats.get("message"))

    with st.expander("Chunk details", expanded=False):
        for chunk_id in sorted(indexes.keys()):
            idx = indexes[chunk_id]
            st.write(
                f"Chunk {chunk_id}: {idx['chunk_size']:,} games | "
                f"dim={idx['dimension']} | vocab={idx['vocab_size']:,}"
            )


if "chat_messages" not in st.session_state:
    st.session_state.chat_messages = []
if "pending_user_msg" not in st.session_state:
    st.session_state.pending_user_msg = ""
if "chat_submit_requested" not in st.session_state:
    st.session_state.chat_submit_requested = False

user_msg = None

search_tab, recommend_tab, chatbot_tab = st.tabs(["Search", "Recommend", "Chatbot"])

with search_tab:
    st.header("Search Games")
    with st.form("search_form"):
        col1, col2 = st.columns([3, 1])
        with col1:
            search_query = st.text_input(
                "Query",
                placeholder="Nhập thông tin liên quan đến game cần tìm : tên, tag, thể loại, nhà phát triển, nhà xuất bản,...",
                key="search_query",
            )
        with col2:
            top_n = st.number_input("Top N", min_value=5, max_value=50, value=20)

        do_lexical = st.form_submit_button("Lexical Search", type="primary", use_container_width=True)

    if do_lexical and search_query:
        raw_results = search_across_all_chunks(search_query, indexes, max_results=int(top_n))
        if raw_results.empty:
            st.warning("No lexical match found.")
        else:
            st.success(f"Lexical results: {len(raw_results)} games")

            display_df = raw_results.copy()

            def get_top_3(text):
                if pd.isna(text) or not str(text).strip():
                    return "N/A"
                items = [item.strip() for item in str(text).split(',')]
                return ", ".join(items[:3]) # Chỉ lấy từ vị trí 0 đến 2

            display_df["tags_short"] = display_df["tags_cleaned"].apply(get_top_3)
            display_df["pubs_short"] = display_df["publishers_cleaned"].apply(get_top_3)

            st.dataframe(
                display_df[["name", "tags_short", "pubs_short", "chunk_id", "relevance_label"]],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "name": st.column_config.TextColumn("Tên Game"),
                    "tags_short": st.column_config.TextColumn("Top Tags"),
                    "pubs_short": st.column_config.TextColumn("Nhà Phát Hành"),
                    "chunk_id": st.column_config.NumberColumn(
                        "Phân Vùng",
                        format="%d"
                    ),
                    "relevance_label": st.column_config.TextColumn("Độ Phù Hợp")
                }
            )

with recommend_tab:
    st.header("Recommend from Seed Game")
    col1, col2 = st.columns([3, 1])
    with col1:
        seed_game = st.text_input(
            "Seed game name",
            placeholder="elden ring, stardew valley, counter-strike",
            key="seed_game",
        )
    with col2:
        rec_top_n = st.slider("Recommendations", 5, 20, 10)

    if st.button("Get FAISS Recommendations", type="primary", use_container_width=True):
        if not seed_game:
            st.warning("Please enter seed game name.")
        else:
            source_game, recs = recommend_with_faiss(seed_game, indexes, top_n=rec_top_n)
            ranked = pd.DataFrame(recs or [])
            if ranked.empty or source_game is None:
                st.error("Seed game not found. Try Search tab first.")
            else:
                ranked["final_score"] = ranked["similarity_score"]
                display_ranked = _prepare_recommend_display(ranked)
                st.success(f"Source: {source_game['name']}")
                st.dataframe(
                        display_ranked[[
                            "rank", "name", "price_category", "release_year",
                            "review_positive_ratio", "final_score"
                        ]],
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "rank": st.column_config.NumberColumn("Top"),
                            "price_category" : st.column_config.TextColumn("Phân Khúc Giá"),
                            "name": st.column_config.TextColumn("Tên Game"),
                            "release_year": st.column_config.NumberColumn(
                                "Năm phát hành",
                                format="%d"
                            ),
                            "review_positive_ratio": st.column_config.NumberColumn(
                                "Review Tích Cực",
                                format="%.0f%%"
                            ),
                            "final_score": st.column_config.NumberColumn(
                                "Điểm Phù Hợp",
                                format="%.3f"
                            )
                        }
                    )
                explanation = explain_recommendations(seed_game, source_game['name'], ranked)
                st.markdown("### AI Explanation")
                st.write(explanation)

with chatbot_tab:
    st.header("Chatbot (Tool Routing)")
    st.caption("Tools: search_game | recommend_from_game | analyze_data (segment/single_game) | compare_games")

    def get_top_3(text):
        if pd.isna(text) or not str(text).strip(): return "N/A"
        return ", ".join([item.strip() for item in str(text).split(',')][:3])

    if st.session_state.chat_submit_requested and st.session_state.pending_user_msg:
        user_msg = st.session_state.pending_user_msg
        st.session_state.chat_messages.append({"role": "user", "content": user_msg})

        intent = route_intent(user_msg)
        assistant_payload = {
            "role": "assistant",
            "intent": intent,
            "content": "",
            "seed_game": "",
            "table_title": "",
            "table_rows": [],
            "column_config": {}
        }

        if intent == "search_game":
            result = search_hybrid(user_msg, indexes, top_n=8)
            if result.empty:
                assistant_payload["content"] = "Không tìm thấy game phù hợp."
            else:
                # Xử lý top 3 tag & publisher giống tab Search
                if "tags_cleaned" in result.columns:
                    result["tags_short"] = result["tags_cleaned"].apply(get_top_3)
                if "publishers_cleaned" in result.columns:
                    result["pubs_short"] = result["publishers_cleaned"].apply(get_top_3)


                assistant_payload["content"] = "Đã tìm thấy kết quả theo hybrid retrieval."
                assistant_payload["table_title"] = "Chi tiết kết quả tìm kiếm"

                # Trích xuất các cột an toàn
                safe_cols = [c for c in ["name", "tags_short", "pubs_short", "chunk_id", "final_score"] if c in result.columns]
                assistant_payload["table_rows"] = _records(result, safe_cols)

                # Áp dụng column_config theo yêu cầu của bạn
                assistant_payload["column_config"] = {
                    "name": st.column_config.TextColumn("Tên Game"),
                    "tags_short": st.column_config.TextColumn("Top Tags"),
                    "pubs_short": st.column_config.TextColumn("Nhà Phát Hành"),
                    "chunk_id": st.column_config.NumberColumn("Phân Vùng", format="%d"),
                    "final_score": st.column_config.NumberColumn("Độ Phù Hợp", format="%.3f")
                }

        elif intent == "recommend_from_game":
            seed_name = extract_seed_game_name(user_msg)

            source, result = recommend_hybrid(seed_name, indexes, top_n=8)

            if result.empty or source is None:
                assistant_payload["content"] = "Không tìm thấy tên game (game seed), bạn có thể thử lại với tên game khác hoặc kiểm tra chính tả."
            else:
                display_result = _prepare_recommend_display(result)

                assistant_payload["seed_game"] = str(source["name"])
                assistant_payload["content"] = explain_recommendations(seed_name, source["name"], result)

                assistant_payload["table_title"] = "Chi tiết gợi ý từ game gốc"
                assistant_payload["table_rows"] = _records(
                    display_result,
                    ["rank", "name", "price_category", "review_positive_ratio", "final_score"],
                )
                assistant_payload["column_config"] = {
                    "rank": st.column_config.NumberColumn("Top"),
                    "name": st.column_config.TextColumn("Tên Game"),
                    "price_category": st.column_config.TextColumn("Phân Khúc Giá"),
                    "review_positive_ratio": st.column_config.NumberColumn("Review Tích Cực", format="%.0f%%"),
                    "final_score": st.column_config.NumberColumn("Điểm Phù Hợp", format="%.3f")
                }

        elif intent == "analyze_data":
            filters = parse_user_filters(user_msg)
            has_segment_filters = filters.get("is_coop") or any(
                filters.get(key) is not None
                for key in ["price_min", "price_max", "review_min", "review_max", "year_min", "year_max"]
            )
            lower_msg = user_msg.lower()
            force_segment_mode = any(
                key in lower_msg
                for key in [
                    "thị trường", "thi truong", "phân khúc", "phan khuc", "xu hướng", "xu huong",
                    "thống kê", "thong ke", "toàn cảnh", "toan canh", "mặt bằng", "mat bang",
                ]
            )

            candidate_game_name = extract_analyze_game_name(user_msg)
            game_row = pick_game_by_name(catalog, candidate_game_name) if candidate_game_name else None

            if game_row is not None and not has_segment_filters and not force_segment_mode:
                assistant_payload["content"] = explain_single_game_analysis(user_msg, game_row)
                assistant_payload["table_title"] = "Hồ sơ game"

                assistant_payload["table_rows"] = [
                    {"metric": "name", "value": str(game_row.get("name", ""))},
                    {"metric": "release_year", "value": int(game_row.get("release_year", 0) or 0)},
                    {"metric": "price_vnd", "value": int(float(game_row.get("price_vnd", 0) or 0))},
                    {"metric": "price_category", "value": str(game_row.get("price_category_vnd", "") or "")},
                    {"metric": "review_positive_ratio", "value": round(float(game_row.get("steam_review_positive_ratio", 0) or 0), 4)},
                    {"metric": "genres", "value": str(game_row.get("genres_cleaned", "") or "")},
                    {"metric": "categories", "value": str(game_row.get("categories_cleaned", "") or "")},
                ]
                assistant_payload["column_config"] = {
                    "metric": st.column_config.TextColumn("Chỉ Số"),
                    "value": st.column_config.TextColumn("Giá Trị")
                }
            else:
                data = apply_structured_filters(catalog, filters)

                if data.empty:
                    assistant_payload["content"] = "Không có dữ liệu thỏa điều kiện phân tích."
                else:
                    assistant_payload["content"] = explain_analysis(user_msg, data)
                    review_series = data["steam_review_positive_ratio"].fillna(0).clip(0, 1)
                    price_series = data["price_vnd"].fillna(0)
                    categories = data["categories_cleaned"].fillna("").astype(str).str.split(",").explode().str.strip()
                    categories = categories[categories != ""]
                    top_categories = categories.value_counts().head(3)

                    summary_rows = [
                        {"metric": "sample_size", "value": int(len(data))},
                        {"metric": "avg_review_positive_ratio", "value": round(float(review_series.mean()), 4)},
                        {"metric": "median_review_positive_ratio", "value": round(float(review_series.median()), 4)},
                        {"metric": "share_review_ge_0_9", "value": round(float((review_series >= 0.9).mean()), 4)},
                        {"metric": "avg_price_vnd", "value": int(float(price_series.mean()))},
                        {"metric": "median_price_vnd", "value": int(float(price_series.median()))},
                        {"metric": "share_free_to_play", "value": round(float((price_series <= 0).mean()), 4)},
                    ]
                    for idx, (cat_name, cat_count) in enumerate(top_categories.items(), start=1):
                        summary_rows.append({"metric": f"top_category_{idx}", "label": str(cat_name), "value": int(cat_count)})

                    assistant_payload["table_title"] = "Bảng chỉ số phân tích"
                    assistant_payload["table_rows"] = summary_rows
                    assistant_payload["column_config"] = {
                        "metric": st.column_config.TextColumn("Chỉ Số Phân Tích"),
                        "value": st.column_config.TextColumn("Kết Quả")
                    }

        elif intent == "compare_games":
            g1, g2 = parse_compare_names(user_msg)
            row1 = pick_game_by_name(catalog, g1)
            row2 = pick_game_by_name(catalog, g2)
            if row1 is None or row2 is None:
                assistant_payload["content"] = "Cần 2 tên game để so sánh, ví dụ: elden ring vs sekiro"
            else:
                price1 = int(float(row1.get("price_vnd", 0) or 0))
                price2 = int(float(row2.get("price_vnd", 0) or 0))
                year1 = int(float(row1.get("release_year", 0) or 0))
                year2 = int(float(row2.get("release_year", 0) or 0))
                comp = pd.DataFrame([
                    {
                        "metric": "price_vnd",
                        row1["name"]: price1,
                        row2["name"]: price2,
                    },
                    {
                        "metric": "review_positive_ratio",
                        row1["name"]: row1.get("steam_review_positive_ratio", 0),
                        row2["name"]: row2.get("steam_review_positive_ratio", 0),
                    },
                    {
                        "metric": "release_year",
                        row1["name"]: year1,
                        row2["name"]: year2,
                    },
                ])
                assistant_payload["content"] = explain_comparison(user_msg, row1, row2)
                assistant_payload["table_title"] = "Bảng so sánh game"
                assistant_payload["table_rows"] = comp.to_dict(orient="records")
                assistant_payload["column_config"] = {
                    "metric": st.column_config.TextColumn("Tiêu Chí So Sánh")
                }
        else:
            assistant_payload["content"] = "Hiện chatbot chỉ hỗ trợ: recommend từ game, compare, và analyze (segment/single-game)."

        st.session_state.chat_messages.append(assistant_payload)
        st.session_state.pending_user_msg = ""
        st.session_state.chat_submit_requested = False

    for msg in st.session_state.chat_messages:
        render_chat_entry(msg)

    with st.form("chatbot_form", clear_on_submit=True):
        st.text_input(
            "Hỏi bot",
            placeholder="Ví dụ: phân tích game Elden Ring hoặc so sánh Elden Ring vs Sekiro",
            key="chatbot_input",
        )
        st.form_submit_button("Send", on_click=_queue_chat_submit)
