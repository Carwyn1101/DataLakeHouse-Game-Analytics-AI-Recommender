# streamlit-ai/src/faiss_manager.py
"""
FAISS Index Manager - Load & Query với FAISS
Lightning-fast similarity search!
"""
import streamlit as st
import faiss
import pickle
import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import normalize

INDEX_DIR = "/app/faiss_indexes"
TOTAL_CHUNKS = 6

@st.cache_resource
def load_all_faiss_indexes():
    """
    Load TẤT CẢ FAISS indexes vào memory
    ~3-5s để load, query instant!
    """
    indexes = {}
    
    with st.spinner("Đang load FAISS indexes (3-5s)..."):
        for chunk_id in range(1, TOTAL_CHUNKS + 1):
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
    
    st.success(f"Loaded {len(indexes)}/{TOTAL_CHUNKS} FAISS chunks")
    
    return indexes

def search_across_all_chunks(query, indexes, max_results=20):
    """
    Tìm kiếm ACROSS ALL CHUNKS (string matching)
    """
    all_results = []
    query_lower = query.lower()
    
    for chunk_id, index_data in indexes.items():
        df = index_data['games_df']
        
        # String search
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

# def recommend_with_faiss(game_name, indexes, top_n=10):
    # """
    # Recommend với FAISS - LIGHTNING FAST!
    # Search across ALL chunks để tìm game tương đồng nhất.
    # """
    # game_name_lower = game_name.lower()
    
    # # Step 1: Tìm game trong chunk nào
    # found_chunk = None
    # game_id = None
    
    # for chunk_id, index_data in indexes.items():
    #     name_to_id = index_data['name_to_id']
        
    #     # Exact match
    #     if game_name_lower in name_to_id:
    #         found_chunk = chunk_id
    #         game_id = name_to_id[game_name_lower]
    #         break
        
    #     # Partial match
    #     for name, gid in name_to_id.items():
    #         if game_name_lower in name:
    #             found_chunk = chunk_id
    #             game_id = gid
    #             break
        
    #     if found_chunk:
    #         break
    
    # if not found_chunk:
    #     return None, None
    
    # # Step 2: Query FAISS index
    # index_data = indexes[found_chunk]
    # faiss_index = index_data['faiss_index']
    # df = index_data['games_df']
    
    # # Get all TF-IDF vectorizers and weights
    # tfidf_tags_top7      = index_data['tfidf_tags_top7']
    # tfidf_tags_remaining = index_data['tfidf_tags_remaining']
    # tfidf_developers     = index_data['tfidf_developers']
    # tfidf_categories     = index_data['tfidf_categories']
    # tfidf_price          = index_data['tfidf_price']
    # tfidf_publishers     = index_data['tfidf_publishers']
    # review_scaler        = index_data['review_scaler']
    # weights              = index_data['weights']
    # top_n_tags           = index_data.get('top_tags_count', 7)  # fallback = 7
    
    # # Get source game — dùng tags đã được normalize & tách sẵn từ lúc build
    # source_game = df.iloc[game_id]
    
    # # Dùng lại tags_top7_str và tags_remaining_str đã tách sẵn khi build
    # # KHÔNG cần tách lại — đảm bảo nhất quán với lúc fit TF-IDF
    # tags_top7_str      = source_game['tags_top7_str']
    # tags_remaining_str = source_game['tags_remaining_str'] or 'unknown'
    
    # # Build weighted query vector
    # # 1. Tags Top-7 (49%)
    # tags_top7_vec = tfidf_tags_top7.transform([tags_top7_str]).toarray().astype('float32')
    # tags_top7_vec = normalize(tags_top7_vec, norm='l2', axis=1) * np.sqrt(weights['tags_top7'])
    
    # # 2. Tags Remaining (21%)
    # tags_remaining_vec = tfidf_tags_remaining.transform([tags_remaining_str]).toarray().astype('float32')
    # tags_remaining_vec = normalize(tags_remaining_vec, norm='l2', axis=1) * np.sqrt(weights['tags_remaining'])
    
    # # 3. Developers (15%)
    # developers_vec = tfidf_developers.transform([source_game['developers_cleaned'].lower()]).toarray().astype('float32')
    # developers_vec = normalize(developers_vec, norm='l2', axis=1) * np.sqrt(weights['developers'])
    
    # # 4. Categories (8%)
    # categories_vec = tfidf_categories.transform([source_game['categories_cleaned'].lower()]).toarray().astype('float32')
    # categories_vec = normalize(categories_vec, norm='l2', axis=1) * np.sqrt(weights['categories'])
    
    # # 5. Price (3%)
    # price_vec = tfidf_price.transform([source_game['price_category_vnd'].lower()]).toarray().astype('float32')
    # price_vec = normalize(price_vec, norm='l2', axis=1) * np.sqrt(weights['price_category'])
    
    # # 6. Publishers (2%)
    # publishers_vec = tfidf_publishers.transform([source_game['publishers_cleaned'].lower()]).toarray().astype('float32')
    # publishers_vec = normalize(publishers_vec, norm='l2', axis=1) * np.sqrt(weights['publishers'])
    
    # # 7. Review Ratio (3%)
    # review_ratio = np.array([[source_game['steam_review_positive_ratio']]]).astype('float32')
    # review_ratio = review_scaler.transform(review_ratio).astype('float32') * np.sqrt(weights['review_positive_ratio'])
    
    # # Concatenate — PHẢI ĐÚNG THỨ TỰ với auto_builder.py
    # query_vector = np.hstack([
    #     tags_top7_vec,       # 49%
    #     tags_remaining_vec,  # 21%
    #     developers_vec,      # 15%
    #     categories_vec,      # 8%
    #     price_vec,           # 3%
    #     publishers_vec,      # 2%
    #     review_ratio         # 3%
    # ]).astype('float32')
    
    # query_vector = normalize(query_vector, norm='l2', axis=1)


    # # FAISS search (k+1 vì bao gồm game gốc)
    # k = top_n + 1
    # distances, indices = faiss_index.search(query_vector, k)
    
    # # Build recommendations
    # recommendations = []
    # rank = 0
    
    # for dist, idx in zip(distances[0], indices[0]):
    #     # Skip game gốc
    #     if idx == game_id:
    #         continue
        
    #     rank += 1
    #     if rank > top_n:
    #         break
        
    #     game = df.iloc[idx]
        
    #     # Convert distance to similarity
    #     # FAISS IndexFlatIP returns inner product (for normalized = cosine)
    #     similarity = float(dist)  # Already cosine similarity
        
    #     recommendations.append({
    #         'rank': rank,
    #         'steam_appid': int(game['steam_appid']),
    #         'name': game['name'],
    #         'genres': game['genres_cleaned'],
    #         'tags': game['tags_cleaned'],
    #         'developers': game['developers_cleaned'],
    #         'publishers': game['publishers_cleaned'],
    #         'price_vnd': int(game['price_vnd']) if game['price_vnd'] else 0,
    #         'price_category': game['price_category_vnd'],
    #         'release_year': int(game['release_year']) if game['release_year'] else 0,
    #         'review_positive_ratio': float(game['steam_review_positive_ratio']) if game['steam_review_positive_ratio'] else 0.0,
    #         'similarity_score': similarity,
    #         'chunk_id': found_chunk
    #     })
    
    # return source_game, recommendations

def recommend_with_faiss(game_name, indexes, top_n=10):
    """
    Recommend với FAISS - CROSS-CHUNK SEARCH!
    Search across ALL chunks để tìm game tương đồng nhất.
    """
    game_name_lower = game_name.lower()

    # ================================================================
    # Step 1: Tìm source game — giữ nguyên
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
    all_candidates.sort(key=lambda x: x['similarity_score'], reverse=True)

    recommendations = []
    for rank, candidate in enumerate(all_candidates[:top_n], start=1):
        candidate['rank'] = rank
        recommendations.append(candidate)

    return source_game, recommendations

def get_all_chunks_stats(indexes):
    """
    Thống kê tổng hợp
    """
    total_games = 0
    all_genres = set()
    all_prices = []
    all_years = []
    total_faiss_size = 0
    
    for index_data in indexes.values():
        df = index_data['games_df']
        
        total_games += len(df)
        
        # Genres
        for genres in df['genres_cleaned'].dropna():
            for genre in str(genres).split(','):
                genre = genre.strip()
                if genre:
                    all_genres.add(genre.lower())
        
        # Prices & Years
        all_prices.extend(df['price_vnd'].dropna().tolist())
        all_years.extend(df['release_year'].dropna().tolist())
        
        # FAISS size
        faiss_index = index_data['faiss_index']
        # Estimate FAISS memory usage
        total_faiss_size += faiss_index.ntotal * index_data['dimension'] * 4 / 1024 / 1024  # float32
    
    stats = {
        'total_games': total_games,
        'total_chunks': len(indexes),
        'unique_genres': len(all_genres),
        'price_range': {
            'min': min(all_prices) if all_prices else 0,
            'max': max(all_prices) if all_prices else 0,
            'avg': sum(all_prices) / len(all_prices) if all_prices else 0
        },
        'year_range': {
            'min': int(min(all_years)) if all_years else 0,
            'max': int(max(all_years)) if all_years else 0
        },
        'faiss_size_mb': total_faiss_size
    }
    
    return stats