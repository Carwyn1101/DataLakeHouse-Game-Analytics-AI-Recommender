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
                faiss_index = faiss.read_index(faiss_file)
                
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
    """
    Tìm kiếm ACROSS ALL CHUNKS (string matching).
    Đây là lexical retrieval nhanh, dùng cho tab Search và làm candidate pool cho hybrid rerank.
    """
    all_results = []
    query_lower = query.lower()
    
    for chunk_id, index_data in indexes.items():
        df = index_data['games_df']
        
        mask = (
            df['name'].str.lower().str.contains(query_lower, na=False) |
            df['tags_cleaned'].str.lower().str.contains(query_lower, na=False) | 
            df['genres_cleaned'].str.lower().str.contains(query_lower, na=False) |
            df['developers_cleaned'].str.lower().str.contains(query_lower, na=False) |
            df['publishers_cleaned'].str.lower().str.contains(query_lower, na=False)
        )
        
        matches = df[mask].copy()
        
        if len(matches) > 0:
            matches['chunk_id'] = chunk_id
            matches['relevance'] = matches.apply(
                lambda row: 3.0 if query_lower in row['name'].lower() 
                else 2.5 if query_lower in str(row['tags_cleaned']).lower()
                else 2.0 if query_lower in row['genres_cleaned'].lower()
                else 1.0,
                axis=1
            )
            all_results.append(matches)
    
    if len(all_results) == 0:
        return pd.DataFrame()
    
    combined = pd.concat(all_results, ignore_index=True)
    combined = combined.sort_values('relevance', ascending=False)
    
    return combined.head(max_results)

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
        if found_chunk:
            break

    if not found_chunk:
        return None, None

    # ================================================================
    # Step 2: Build query vector từ chunk chứa source game
    # ================================================================
    index_data   = indexes[found_chunk]
    df           = index_data['games_df']
    source_game  = df.iloc[game_id]

    tags_top7_str      = source_game['tags_top7_str']
    tags_remaining_str = source_game['tags_remaining_str'] or 'unknown'

    # Lưu lại raw features của source game để dùng lại cho từng chunk
    src_tags_top7_str      = tags_top7_str
    src_tags_remaining_str = tags_remaining_str
    src_developers         = source_game['developers_cleaned'].lower()
    src_categories         = source_game['categories_cleaned'].lower()
    src_price              = source_game['price_category_vnd'].lower()
    src_publishers         = source_game['publishers_cleaned'].lower()
    src_review             = float(source_game['steam_review_positive_ratio']) \
                             if source_game['steam_review_positive_ratio'] else 0.0

    # ================================================================
    # Step 3: Search TẤT CẢ chunks — CROSS-CHUNK
    # Mỗi chunk có TF-IDF vocabulary riêng, nên cùng một source game phải
    # được transform lại bằng vectorizer của chunk đích trước khi FAISS search.
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
            all_candidates.append({
                'similarity_score': float(dist),
                'chunk_id': cid,
                'steam_appid': int(game['steam_appid']),
                'name': game['name'],
                'genres': game['genres_cleaned'],
                'tags': game['tags_cleaned'],
                'developers': game['developers_cleaned'],
                'publishers': game['publishers_cleaned'],
                'price_vnd': int(game['price_vnd']) if game['price_vnd'] else 0,
                'price_category': game['price_category_vnd'],
                'release_year': int(game['release_year']) if game['release_year'] else 0,
                'review_positive_ratio': float(game['steam_review_positive_ratio']) if game['steam_review_positive_ratio'] else 0.0,
            })

    # ================================================================
    # Step 4: Merge + sort toàn bộ candidates, lấy top_n
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

