"""
Build and persist the structured FAISS retrieval layer.

Module này chạy khi Streamlit khởi động nếu thiếu index trên disk. Nó đọc dữ liệu
từ gold/silver lakehouse, chuẩn hóa feature text, tạo vector TF-IDF có trọng số,
ghi FAISS binary index và metadata pickle cho từng chunk. Metadata lưu kèm
vectorizer/scaler để `faiss_manager.py` có thể rebuild query vector đúng theo
vocabulary riêng của từng chunk.
"""

import streamlit as st
from pyspark.sql import SparkSession
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import normalize, MinMaxScaler
import faiss
import pickle
import time
import os
from .weights_config import FEATURE_WEIGHTS, FEATURE_DIMENSIONS
from .settings import CHUNK_SIZE, TOTAL_GAMES, TOTAL_CHUNKS, INDEX_DIR
from .semantic_manager import build_profile_text

# ===== CONFIG =====
HIVE_URI = "thrift://hive-metastore:9083"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS = "hongphuc"
MINIO_SECRET = "hongphuc"

# ===== TAG HELPERS =====
TOP_TAGS_COUNT = 7  # Số tags đầu tiên được coi là "core identity" của game

def normalize_tag(tag: str) -> str:
    """
    Chuẩn hóa 1 tag: lowercase + thay dấu - và space bằng underscore
    "Souls-like"   → "souls_like"
    "Open World"   → "open_world"  
    "Action RPG"   → "action_rpg"
    "Dark Fantasy" → "dark_fantasy"
    """
    return tag.strip().lower().replace('-', '_').replace(' ', '_')

def split_tags(tags_str: str, top_n: int = TOP_TAGS_COUNT):
    """
    Tách tags thành 2 nhóm: top N và phần còn lại
    
    Input:  "Souls-like, Open World, Dark Fantasy, RPG, Difficult, Action RPG, 
             Third Person, Multiplayer, Fantasy, Singleplayer"
    
    Output: 
        top7_str      = "souls_like open_world dark_fantasy rpg difficult action_rpg third_person"
        remaining_str = "multiplayer fantasy singleplayer online_co_op action co_op atmospheric"
    
    Trả về dạng string để TF-IDF có thể xử lý (space-separated tokens)
    """
    if not tags_str or pd.isna(tags_str):
        return '', ''
    
    all_tags = [normalize_tag(t) for t in str(tags_str).split(',') if t.strip()]
    
    top_tags = all_tags[:top_n]
    remaining_tags = all_tags[top_n:]
    
    # Join bằng space — TF-IDF sẽ tách theo space
    # Mỗi tag đã được nối bằng underscore nên không bị vỡ
    return ' '.join(top_tags), ' '.join(remaining_tags)


def check_indexes_exist():
    """
    Kiểm tra xem indexes đã được build chưa
    """
    if not os.path.exists(INDEX_DIR):
        return False
    
    for chunk_id in range(1, TOTAL_CHUNKS + 1):
        faiss_file = f"{INDEX_DIR}/faiss_chunk_{chunk_id}.index"    # FAISS binary index
        meta_file = f"{INDEX_DIR}/metadata_chunk_{chunk_id}.pkl"    # Metadata pickle file
        
        if not os.path.exists(faiss_file) or not os.path.exists(meta_file):
            return False
    
    return True # Load từng chunks thay vì 80k games cùng lúc, Có thể build chunks song song, Update 1 chunk không ảnh hưởng chunks khác

def get_spark_session():
    """
    Tạo Spark session (reuse existing logic)
    """
    spark = SparkSession.builder \
        .appName("AutoIndexBuilder") \
        .master("local[*]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.jars.packages", 
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "com.amazonaws:aws-java-sdk-bundle:1.11.1026,"
                "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.3.1") \
        .config("spark.sql.catalog.hive", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hive.type", "hive") \
        .config("spark.sql.catalog.hive.uri", HIVE_URI) \
        .config("spark.sql.catalog.hive.warehouse", "s3a://lakehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()
    
    return spark


def load_chunk_dataframe(spark, start_row, end_row):
    """Load one deterministic row range from the current gold/silver model."""
    query = f"""
        WITH games_current AS (
            SELECT *
            FROM hive.silver.games
            WHERE COALESCE(is_current, true) = true
        ),
        reviews_current AS (
            SELECT *
            FROM hive.silver.reviews
            WHERE COALESCE(is_current, true) = true
        ),
        numbered_games AS (
            SELECT
                g.steam_appid,
                g.name,
                g.release_year,
                gc.tags_cleaned,
                gc.genres_cleaned,
                gc.categories_cleaned,
                gc.developers_cleaned,
                gc.publishers_cleaned,
                d.price_vnd,
                d.price_category_vnd,
                rc.steam_review_positive_ratio,
                CAST(NULL AS STRING) AS profile_text,
                ROW_NUMBER() OVER (ORDER BY g.steam_appid) as row_num
            FROM hive.gold.dim_game g
            JOIN hive.gold.dim_detail_info d ON g.steam_appid = d.steam_appid
            LEFT JOIN games_current gc ON g.steam_appid = gc.steam_appid
            LEFT JOIN reviews_current rc ON g.steam_appid = rc.steam_appid
        )
        SELECT
            steam_appid,
            name,
            release_year,
            tags_cleaned,
            genres_cleaned,
            categories_cleaned,
            developers_cleaned,
            publishers_cleaned,
            price_vnd,
            price_category_vnd,
            steam_review_positive_ratio,
            profile_text
        FROM numbered_games
        WHERE row_num > {start_row} AND row_num <= {end_row}
        """

    return spark.sql(query).toPandas()

def build_chunk_index(spark, chunk_id, progress_callback=None):
    """
    Build index cho 1 chunk
    
    Args:
        spark: SparkSession
        chunk_id: 1-4
        progress_callback: function(message) để update UI
    """
    def log(msg):
        if progress_callback:
            progress_callback(msg)
        print(msg)
    
    start_row = (chunk_id - 1) * CHUNK_SIZE
    end_row = min(chunk_id * CHUNK_SIZE, TOTAL_GAMES)
    chunk_size = end_row - start_row
    
    log(f"Building Chunk {chunk_id}/{TOTAL_CHUNKS} ({chunk_size:,} games)")
    
    # ===== LOAD DATA =====
    log(f"   [1/5] Loading data...")
    
    df = load_chunk_dataframe(spark, start_row, end_row)
    
    df['genres_cleaned'] = df['genres_cleaned'].fillna('') #Empty string thay vì None nếu có
    df['categories_cleaned'] = df['categories_cleaned'].fillna('')
    df['developers_cleaned'] = df['developers_cleaned'].fillna('')
    df['publishers_cleaned'] = df['publishers_cleaned'].fillna('')
    df['tags_cleaned'] = df['tags_cleaned'].fillna('')
    df['steam_review_positive_ratio'] = df['steam_review_positive_ratio'].fillna(0.0)
    if 'profile_text' not in df.columns:
        df['profile_text'] = ''
    df['profile_text'] = df['profile_text'].fillna('')
    missing_profile = df['profile_text'].str.strip() == ''
    if missing_profile.any():
        # The current gold model does not materialize profile_text, so build it
        # here to keep semantic indexing deterministic with the same source data.
        df.loc[missing_profile, 'profile_text'] = df.loc[missing_profile].apply(build_profile_text, axis=1)
    
    log(f"   Loaded {len(df):,} games")
    
    # ===== BUILD FEATURES =====
    log(f"   [2/5] Building weighted features...")
    
    weights = FEATURE_WEIGHTS
    dimensions = FEATURE_DIMENSIONS
    
    # ── Tách tags thành 2 nhóm ──────────────────────────────────────
    log(f"   Splitting tags into top-{TOP_TAGS_COUNT} and remaining...")
    
    tag_splits = df['tags_cleaned'].apply(lambda x: pd.Series(split_tags(x, TOP_TAGS_COUNT)))
    df['tags_top7_str']      = tag_splits[0]   # "souls_like open_world dark_fantasy ..."
    df['tags_remaining_str'] = tag_splits[1]   # "multiplayer fantasy singleplayer ..."
    
    sample_idx = df[df['tags_cleaned'].str.len() > 10].index[0] if len(df) > 0 else 0
    log(f"   Sample tags_top7:     '{df.loc[sample_idx, 'tags_top7_str']}'")
    log(f"   Sample tags_remaining:'{df.loc[sample_idx, 'tags_remaining_str']}'")

    # 1. Tags Top-7 (49%) ── Core identity của game
    tfidf_tags_top7 = TfidfVectorizer(
        max_features=dimensions['tags_top7'],   # 150 dims
        stop_words=None,        # Không lọc — tags là nhãn có chủ đích
        ngram_range=(1, 1),     # Unigram — tags đã được nối bằng underscore
        min_df=1,
        max_df=0.3,
        token_pattern=r'[^\s,]+',  # Tách theo space/comma, GIỮ underscore
        dtype=np.float32
    )
    tags_top7_matrix = tfidf_tags_top7.fit_transform(df['tags_top7_str']).toarray().astype('float32')
    tags_top7_matrix = normalize(tags_top7_matrix, norm='l2', axis=1) * np.sqrt(weights['tags_top7'])
    
    # 2. Tags Remaining (21%) ── Supporting context
    tfidf_tags_remaining = TfidfVectorizer(
        max_features=dimensions['tags_remaining'],  # 100 dims
        stop_words=None,
        ngram_range=(1, 1),
        min_df=1,
        max_df=0.95,
        token_pattern=r'[^\s,]+',
        dtype=np.float32
    )
    # Handle case khi remaining rỗng (game có ít hơn 7 tags)
    remaining_docs = df['tags_remaining_str'].replace('', 'unknown')
    tags_remaining_matrix = tfidf_tags_remaining.fit_transform(remaining_docs).toarray().astype('float32')
    tags_remaining_matrix = normalize(tags_remaining_matrix, norm='l2', axis=1) * np.sqrt(weights['tags_remaining'])
    
    # 3. Developers (15%)
    tfidf_developers = TfidfVectorizer(
        max_features=dimensions['developers'],
        stop_words='english',
        ngram_range=(1, 1),
        min_df=2,
        max_df=0.8,
        dtype=np.float32
    )
    developers_matrix = tfidf_developers.fit_transform(df['developers_cleaned'].str.lower()).toarray().astype('float32')
    developers_matrix = normalize(developers_matrix, norm='l2', axis=1) * np.sqrt(weights['developers'])
    
    # 4. Categories (8%)
    tfidf_categories = TfidfVectorizer(
        max_features=dimensions['categories'],
        stop_words='english',
        ngram_range=(1, 1),
        min_df=2,
        max_df=0.8,
        dtype=np.float32
    )
    categories_matrix = tfidf_categories.fit_transform(df['categories_cleaned'].str.lower()).toarray().astype('float32')
    categories_matrix = normalize(categories_matrix, norm='l2', axis=1) * np.sqrt(weights['categories'])
    
    # 5. Price Category (3%)
    tfidf_price = TfidfVectorizer(
        max_features=dimensions['price_category'],
        stop_words='english',
        ngram_range=(1, 1),
        dtype=np.float32
    )
    price_matrix = tfidf_price.fit_transform(df['price_category_vnd'].str.lower()).toarray().astype('float32')
    price_matrix = normalize(price_matrix, norm='l2', axis=1) * np.sqrt(weights['price_category'])
    
    # 6. Publishers (2%)
    tfidf_publishers = TfidfVectorizer(
        max_features=dimensions['publishers'],
        stop_words='english',
        ngram_range=(1, 1),
        min_df=2,
        max_df=0.8,
        dtype=np.float32
    )
    publishers_matrix = tfidf_publishers.fit_transform(df['publishers_cleaned'].str.lower()).toarray().astype('float32')
    publishers_matrix = normalize(publishers_matrix, norm='l2', axis=1) * np.sqrt(weights['publishers'])
    
    # 7. Review Positive Ratio (3%) — Numerical
    review_ratio = df['steam_review_positive_ratio'].values.reshape(-1, 1).astype('float32')
    scaler = MinMaxScaler()
    review_ratio = scaler.fit_transform(review_ratio).astype('float32') * np.sqrt(weights['review_positive_ratio'])
    
    # Concatenate — THỨ TỰ PHẢI KHỚP VỚI faiss_manager.py khi rebuild query vector
    vectors = np.hstack([
        tags_top7_matrix,       # 49%
        tags_remaining_matrix,  # 21%
        developers_matrix,      # 15%
        categories_matrix,      # 8%
        price_matrix,           # 3%
        publishers_matrix,      # 2%
        review_ratio            # 3%
    ]).astype('float32')
    
    vectors = normalize(vectors, norm='l2', axis=1)
    
    dimension = vectors.shape[1]
    log(f"   Weighted Features: {vectors.shape[0]:,} x {dimension}")
    log(f"      - Tags Top-7:    {tags_top7_matrix.shape[1]} dims (49%)")
    log(f"      - Tags Remaining:{tags_remaining_matrix.shape[1]} dims (21%)")
    log(f"      - Developers:    {developers_matrix.shape[1]} dims (15%)")
    log(f"      - Categories:    {categories_matrix.shape[1]} dims (8%)")
    log(f"      - Price:         {price_matrix.shape[1]} dims (3%)")
    log(f"      - Publishers:    {publishers_matrix.shape[1]} dims (2%)")
    log(f"      - Review Ratio:  1 dim (3%)")


    # ===== BUILD FAISS INDEX =====
    log(f"   [3/5] Building FAISS index...")
    
    index = faiss.IndexFlatIP(dimension)
    index.add(vectors)
    
    log(f"   FAISS index built ({index.ntotal:,} vectors)")
    
    # ===== BUILD INVERTED INDEX =====
    log(f"   [4/5] Building inverted index...")
    
    name_to_id = {name.lower(): idx for idx, name in enumerate(df['name'])}
    
    genre_index = {}
    for idx, genres in enumerate(df['genres_cleaned']):
        for genre in str(genres).split(','):
            genre = genre.strip().lower()
            if genre:
                if genre not in genre_index:
                    genre_index[genre] = []
                genre_index[genre].append(idx)
    
    log(f"   Indexed {len(name_to_id)} names, {len(genre_index)} genres")
    
    # ===== SAVE =====
    log(f"   [5/5] Saving...")
    
    os.makedirs(INDEX_DIR, exist_ok=True)
    
    faiss_file = f"{INDEX_DIR}/faiss_chunk_{chunk_id}.index"
    faiss.write_index(index, faiss_file)
    
    metadata = {
        'chunk_id': chunk_id,
        'chunk_range': (start_row, end_row),
        'chunk_size': chunk_size,
        'dimension': dimension,
        'index_type': "IndexFlatIP (exact)",
        'games_df': df[[
            'steam_appid', 'name', 'release_year',
            'price_vnd', 'price_category_vnd',
            'tags_cleaned',           # raw tags — để hiển thị UI
            'tags_top7_str',          # đã normalize — để rebuild query vector
            'tags_remaining_str',     # đã normalize — để rebuild query vector
            'genres_cleaned',         # giữ lại để hiển thị UI (không dùng trong vector)
            'categories_cleaned',
            'developers_cleaned',
            'publishers_cleaned',
            'steam_review_positive_ratio',
            'profile_text'
        ]],
        # Store vectorizers needed to rebuild query vectors per chunk.
        'tfidf_tags_top7': tfidf_tags_top7,
        'tfidf_tags_remaining': tfidf_tags_remaining,
        'tfidf_developers': tfidf_developers,
        'tfidf_categories': tfidf_categories,
        'tfidf_price': tfidf_price,
        'tfidf_publishers': tfidf_publishers,
        'review_scaler': scaler,
        'weights': weights,
        'top_tags_count': TOP_TAGS_COUNT,   # lưu lại để biết dùng bao nhiêu tags
        'vocab_size': (len(tfidf_tags_top7.vocabulary_) +
                       len(tfidf_tags_remaining.vocabulary_) +
                       len(tfidf_developers.vocabulary_) +
                       len(tfidf_categories.vocabulary_) +
                       len(tfidf_price.vocabulary_) +
                       len(tfidf_publishers.vocabulary_) + 1),
        'name_to_id': name_to_id,
        'genre_index': genre_index,
        'build_time': 0,
        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
    }


    metadata_file = f"{INDEX_DIR}/metadata_chunk_{chunk_id}.pkl"
    with open(metadata_file, 'wb') as f:
        pickle.dump(metadata, f, protocol=pickle.HIGHEST_PROTOCOL)
    
    faiss_size = os.path.getsize(faiss_file) / 1024 / 1024
    meta_size = os.path.getsize(metadata_file) / 1024 / 1024
    
    log(f" Saved: {faiss_size:.1f} MB (FAISS) + {meta_size:.1f} MB (meta)")
    
    return True

@st.cache_resource(show_spinner=False)
def auto_build_indexes_if_needed():
    """
    Kiểm tra và auto-build indexes nếu chưa có
    Hàm này được gọi khi Streamlit start
    """
    if check_indexes_exist():
        return True

    progress_bar = st.progress(0)
    status_text = st.empty()
    log_container = st.expander("Build Logs", expanded=False)
    
    def update_progress(message):
        """Callback to update UI"""
        with log_container:
            st.text(message)
    
    try:
        status_text.text("Connecting to Lakehouse...")
        spark = get_spark_session()
        status_text.text("Connected to Lakehouse")
        
        total_start = time.time()
        
        for chunk_id in range(1, TOTAL_CHUNKS + 1):
            progress = (chunk_id - 1) / TOTAL_CHUNKS
            progress_bar.progress(progress)
            status_text.text(f"Building Chunk {chunk_id}/{TOTAL_CHUNKS}...")
            
            chunk_start = time.time()
            
            build_chunk_index(spark, chunk_id, update_progress)
            
            chunk_elapsed = time.time() - chunk_start
            update_progress(f"   Chunk {chunk_id} completed in {chunk_elapsed:.1f}s\n")
        
        progress_bar.progress(1.0)
        total_elapsed = time.time() - total_start
        
        status_text.text(f"All indexes built in {total_elapsed/60:.1f} minutes")
        
        spark.stop()
        
        return True
    
    except Exception as e:
        st.error(f"Error building indexes: {e}")
        return False
