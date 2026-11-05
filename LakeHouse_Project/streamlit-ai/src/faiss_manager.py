"""
Load and query the structured FAISS indexes.

Module này chỉ làm việc với artifact đã được `auto_builder.py` tạo ra:
`faiss_chunk_*.index` và `metadata_chunk_*.pkl`. Nó cung cấp lexical search cho
tab Search, catalog hợp nhất cho chatbot/semantic index, và recommendation từ
một seed game. Vì mỗi chunk được fit TF-IDF riêng, recommendation cross-chunk
phải transform lại source-game features bằng vectorizer của từng chunk trước
khi gọi FAISS search.
"""
import streamlit as st
import faiss
import pickle
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize
from .settings import INDEX_DIR, TOTAL_CHUNKS


def _discover_chunks():
    """Return chunk ids that have both FAISS binary and metadata files on disk."""
    discovered = []
    if not os.path.exists(INDEX_DIR):
        return discovered
    for chunk_id in range(1, TOTAL_CHUNKS + 1):
        faiss_file = f"{INDEX_DIR}/faiss_chunk_{chunk_id}.index"
        meta_file = f"{INDEX_DIR}/metadata_chunk_{chunk_id}.pkl"
        if os.path.exists(faiss_file) and os.path.exists(meta_file):
            discovered.append(chunk_id)
    return discovered

@st.cache_resource
def load_all_faiss_indexes():
    """
    Load TẤT CẢ FAISS indexes vào memory
    ~3-5s để load, query instant!
    """
    indexes = {}
    discovered_chunks = _discover_chunks()
    chunk_ids = discovered_chunks or list(range(1, TOTAL_CHUNKS + 1))
    show_load_logs = len(discovered_chunks) < TOTAL_CHUNKS
    
    with st.spinner("Đang load FAISS indexes (3-5s)..."):
        for chunk_id in chunk_ids:
            faiss_file = f"{INDEX_DIR}/faiss_chunk_{chunk_id}.index"
            meta_file = f"{INDEX_DIR}/metadata_chunk_{chunk_id}.pkl"
            
            if not os.path.exists(faiss_file) or not os.path.exists(meta_file):
                st.warning(f"Index chunk {chunk_id} không tồn tại!")
                continue
            
            try:
                # Load FAISS index
                faiss_index = faiss.read_index(faiss_file)
                
                # Load metadata
                with open(meta_file, 'rb') as f:
                    metadata = pickle.load(f)
                
                indexes[chunk_id] = {
                    'faiss_index': faiss_index,
                    **metadata
                }
                
                if show_load_logs:
                    st.info(f"Loaded Chunk {chunk_id}: {metadata['chunk_size']:,} games ({metadata['index_type']})")
            
            except Exception as e:
                st.error(f"Lỗi load chunk {chunk_id}: {e}")
    
    if len(indexes) == 0:
        st.error("""
        Không tìm thấy FAISS indexes!
        
        **Cách fix:**
        ```bash
        docker exec -it game-recommender-ai python /app/scripts/build_faiss_indexes.py
        ```
        """)
        return None
    
    if show_load_logs:
        st.success(f"Loaded {len(indexes)}/{TOTAL_CHUNKS} FAISS chunks")
    
    return indexes


def get_catalog_dataframe(indexes):
    """Merge chunk metadata into one catalog for chatbot filtering and semantic indexing."""
    frames = []
    for chunk_id, index_data in indexes.items():
        df = index_data['games_df'].copy()
        df['chunk_id'] = chunk_id
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates(subset=['steam_appid'], keep='first')


def search_across_all_chunks(query, indexes, max_results=20):
    """Search literal text across chunks, keeping metadata and score schemas stable."""
    query_lower = str(query or "").strip().casefold()
    if not query_lower or max_results <= 0:
        return pd.DataFrame()

    all_results = []
    columns = ["name", "tags_cleaned", "genres_cleaned", "developers_cleaned", "publishers_cleaned"]
    labels = {
        3.0: "Mức 3 - Tương đồng trong Tên Game",
        2.5: "Mức 2.5 - Tương đồng trong Tag",
        2.0: "Mức 2.0 - Tương đồng trong Thể loại",
        1.0: "Mức 1.0 - Tương đồng Dev/Publisher",
    }
    for chunk_id, index_data in indexes.items():
        df = index_data['games_df']
        text = {
            column: (df[column] if column in df else pd.Series("", index=df.index))
                .fillna("").astype(str).str.casefold()
            for column in columns
        }
        matches_by_field = {column: values.str.contains(query_lower, regex=False, na=False)
                            for column, values in text.items()}
        mask = pd.Series(False, index=df.index)
        for field_mask in matches_by_field.values():
            mask |= field_mask
        if not mask.any():
            continue
        matches = df.loc[mask].copy()
        matches['chunk_id'] = chunk_id
        relevance = pd.Series(1.0, index=df.index)
        for column, score in [('genres_cleaned', 2.0), ('tags_cleaned', 2.5), ('name', 3.0)]:
            relevance.loc[matches_by_field[column]] = score
        matches['relevance'] = relevance.loc[mask]
        matches['relevance_label'] = matches['relevance'].map(labels)
        all_results.append(matches)

    if not all_results:
        return pd.DataFrame()
    return pd.concat(all_results, ignore_index=True).sort_values(
        'relevance', ascending=False, kind='stable'
    ).head(max_results)

def _metadata_text(value, default=""):
    """Normalize nullable scalar metadata before string/vectorizer operations."""
    return default if pd.isna(value) else str(value)


def _metadata_number(value):
    try:
        number = float(value)
        return number if np.isfinite(number) else 0.0
    except (TypeError, ValueError):
        return 0.0


def recommend_with_faiss(game_name, indexes, top_n=10):
    """
    Recommend với FAISS - CROSS-CHUNK SEARCH!
    Search across ALL chunks để tìm game tương đồng nhất.
    """
    game_name_lower = game_name.lower()

    # ================================================================
    # Step 1: Tìm source game
    # ================================================================
    found_chunk = None
    game_id = None

    for chunk_id, index_data in indexes.items():
        name_to_id = index_data['name_to_id']
        if game_name_lower in name_to_id:
            found_chunk = chunk_id
            game_id = name_to_id[game_name_lower]
            break
        for name, gid in name_to_id.items():
            if game_name_lower in name:
                found_chunk = chunk_id
                game_id = gid
                break
        if found_chunk is not None:
            break

    if found_chunk is None:
        return None, None

    # ================================================================
    # Step 2: Build query vector từ chunk chứa source game
    # ================================================================
    index_data   = indexes[found_chunk]
    df           = index_data['games_df']
    source_game  = df.iloc[game_id]

    tags_top7_str      = _metadata_text(source_game['tags_top7_str'], 'unknown')
    tags_remaining_str = _metadata_text(source_game['tags_remaining_str'], 'unknown') or 'unknown'

    # Lưu lại raw features của source game để dùng lại cho từng chunk
    src_tags_top7_str      = tags_top7_str
    src_tags_remaining_str = tags_remaining_str
    src_developers         = _metadata_text(source_game['developers_cleaned']).lower()
    src_categories         = _metadata_text(source_game['categories_cleaned']).lower()
    src_price              = _metadata_text(source_game['price_category_vnd']).lower()
    src_publishers         = _metadata_text(source_game['publishers_cleaned']).lower()
    src_review             = _metadata_number(source_game['steam_review_positive_ratio'])

    # ================================================================
    # Step 3: Search TẤT CẢ chunks — CROSS-CHUNK
    # Mỗi chunk có TF-IDF vocabulary riêng, nên phải rebuild query
    # vector bằng TF-IDF của từng chunk trước khi search.
    # ================================================================
    all_candidates = []

    for cid, cdata in indexes.items():
        chunk_faiss = cdata['faiss_index']
        chunk_df    = cdata['games_df']
        c_weights   = cdata['weights']

        # Rebuild query vector bằng TF-IDF của chunk này
        c_tags_top7_vec = normalize(
            cdata['tfidf_tags_top7'].transform([src_tags_top7_str]).toarray().astype('float32'),
            norm='l2', axis=1) * np.sqrt(c_weights['tags_top7'])

        c_tags_remaining_vec = normalize(
            cdata['tfidf_tags_remaining'].transform([src_tags_remaining_str]).toarray().astype('float32'),
            norm='l2', axis=1) * np.sqrt(c_weights['tags_remaining'])

        c_developers_vec = normalize(
            cdata['tfidf_developers'].transform([src_developers]).toarray().astype('float32'),
            norm='l2', axis=1) * np.sqrt(c_weights['developers'])

        c_categories_vec = normalize(
            cdata['tfidf_categories'].transform([src_categories]).toarray().astype('float32'),
            norm='l2', axis=1) * np.sqrt(c_weights['categories'])

        c_price_vec = normalize(
            cdata['tfidf_price'].transform([src_price]).toarray().astype('float32'),
            norm='l2', axis=1) * np.sqrt(c_weights['price_category'])

        c_publishers_vec = normalize(
            cdata['tfidf_publishers'].transform([src_publishers]).toarray().astype('float32'),
            norm='l2', axis=1) * np.sqrt(c_weights['publishers'])

        c_review_vec = cdata['review_scaler'].transform(
            np.array([[src_review]]).astype('float32')
        ).astype('float32') * np.sqrt(c_weights['review_positive_ratio'])

        c_query_vector = normalize(np.hstack([
            c_tags_top7_vec, c_tags_remaining_vec, c_developers_vec,
            c_categories_vec, c_price_vec, c_publishers_vec, c_review_vec
        ]).astype('float32'), norm='l2', axis=1)

        # Search chunk này với query vector đã được encode đúng vocabulary
        k_per_chunk = top_n * 2 + 1
        distances, indices = chunk_faiss.search(c_query_vector, k_per_chunk)

        for dist, idx in zip(distances[0], indices[0]):
            if idx < 0:  # FAISS trả -1 nếu không đủ vector
                continue
            # Bỏ qua chính source game
            if cid == found_chunk and idx == game_id:
                continue

            game = chunk_df.iloc[idx]
            if int(game['steam_appid']) == int(source_game['steam_appid']):
                continue
            all_candidates.append({
                'similarity_score': float(dist),
                'chunk_id': cid,
                'steam_appid': int(game['steam_appid']),
                'name': game['name'],
                'genres': game['genres_cleaned'],
                'tags': game['tags_cleaned'],
                'developers': game['developers_cleaned'],
                'publishers': game['publishers_cleaned'],
                'price_vnd': int(_metadata_number(game['price_vnd'])),
                'price_category': game['price_category_vnd'],
                'release_year': int(_metadata_number(game['release_year'])),
                'review_positive_ratio': _metadata_number(game['steam_review_positive_ratio']),
            })

    # ================================================================
    # Step 4: Merge + Dedup + sort toàn bộ candidates, lấy top_n
    # ================================================================
    unique_candidates = {}
    for cand in all_candidates:
        key = cand['steam_appid']
        if key not in unique_candidates or cand['similarity_score'] > unique_candidates[key]['similarity_score']:
            unique_candidates[key] = cand

    all_candidates = list(unique_candidates.values())
    all_candidates.sort(key=lambda x: x['similarity_score'], reverse=True)

    recommendations = []
    for rank, candidate in enumerate(all_candidates[:top_n], start=1):
        candidate['rank'] = rank
        recommendations.append(candidate)

    return source_game, recommendations
