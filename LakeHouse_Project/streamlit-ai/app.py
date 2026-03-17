# streamlit-ai/app.py
"""
🎮 GAME RECOMMENDATION AI - AUTO-BUILD ON STARTUP
FAISS indexes được build tự động nếu chưa tồn tại
"""

import streamlit as st
import plotly.graph_objects as go
import pandas as pd

# ==================== PAGE CONFIG ====================
st.set_page_config(
    page_title="Game Recommender AI",
    page_icon="🎮",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ==================== CUSTOM CSS ====================
st.markdown("""
<style>
    .main-header {
        font-size: 3.5rem;
        font-weight: bold;
        text-align: center;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .sub-header {
        text-align: center;
        color: #888;
        font-size: 1.2rem;
        margin-bottom: 2rem;
    }
</style>
""", unsafe_allow_html=True)

# ==================== HEADER ====================
st.markdown('<div class="main-header">🎮 Game Recommendation AI</div>', unsafe_allow_html=True)

# ==================== AUTO-BUILD INDEXES ====================

# Import auto builder
from src.auto_builder import auto_build_indexes_if_needed

# Auto build nếu cần (hàm này có cache, chỉ chạy 1 lần)
build_success = auto_build_indexes_if_needed()

if not build_success:
    st.error("""
    **Cannot build indexes**
    
    Please check:
    1. Hive Metastore is running
    2. MinIO is accessible
    3. Gold layer has data
    
    Or build manually:
    ```bash
    docker exec -it game-recommender-ai python /app/scripts/build_faiss_indexes.py
    ```
    """)
    st.stop()

# ==================== LOAD INDEXES ====================

from src.faiss_manager import (
    load_all_faiss_indexes,
    search_across_all_chunks, 
    recommend_with_faiss,
)

# ==================== SIDEBAR ====================
with st.sidebar:
    st.image("https://media2.dev.to/dynamic/image/width=1000,height=420,fit=cover,gravity=auto,format=auto/https%3A%2F%2Fdev-to-uploads.s3.amazonaws.com%2Fuploads%2Farticles%2Ftmxbvuil5vq7ekv6j44o.jpg", width=100)
    st.title("System Control")
    
    # Load indexes
    st.subheader("Loading Indexes")
    
    indexes = load_all_faiss_indexes()
    
    if indexes is None:
        st.stop()
    
    # System info
    st.markdown("---")
    st.subheader("System Info")
    
    total_games = sum(idx['chunk_size'] for idx in indexes.values())
    total_chunks = len(indexes)
    
    st.success(f"""
    **Mode:** Auto-Build   
    **Total Games:** {total_games:,}  
    **Loaded Chunks:** {total_chunks}/6  
    **Coverage:** 100%  
    **Build:** Automatic on startup  
    """)
    
    # Chunk details
    st.markdown("---")
    st.subheader("Chunk Details")
    
    for chunk_id in sorted(indexes.keys()):
        idx = indexes[chunk_id]
        with st.expander(f"Chunk {chunk_id}: {idx['chunk_size']:,} games"):
            st.write(f"**Range:** {idx['chunk_range'][0]:,} - {idx['chunk_range'][1]:,}")
            st.write(f"**Dimension:** {idx['dimension']}")
            st.write(f"**Vocabulary:** {idx['vocab_size']:,} terms")
            st.write(f"**Timestamp:** {idx['timestamp']}")

# ==================== MAIN CONTENT ====================

# Tabs
tab1, tab2 = st.tabs(["Search", "Recommend"])

# ===== TAB 1: SEARCH =====
with tab1:
    st.header("Search Games")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_query = st.text_input(
            "Enter game name, genre, or developer:",
            placeholder="e.g., Counter-Strike, RPG, Valve...",
            key="search_input"
        )
    
    with col2:
        max_results = st.number_input("Max results:", min_value=5, max_value=50, value=20)
    
    if st.button("Search", type="primary", use_container_width=True):
        if search_query:
            with st.spinner("Searching..."):
                results = search_across_all_chunks(search_query, indexes, max_results)
            
            if len(results) == 0:
                st.warning(f"No results found for: **{search_query}**")
            else:
                st.success(f"Found {len(results)} games across {results['chunk_id'].nunique()} chunks")
                
                for chunk_id in sorted(results['chunk_id'].unique()):
                    chunk_results = results[results['chunk_id'] == chunk_id]
                    
                    with st.expander(f"Chunk {chunk_id}: {len(chunk_results)} results", expanded=True):
                        for _, game in chunk_results.iterrows():
                            col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                            
                            with col1:
                                st.markdown(f"**{game['name']}**")
                                st.caption(f"Genre: {game['genres_cleaned'][:50]}...")
                                if 'publishers_cleaned' in game and game['publishers_cleaned']:
                                    st.caption(f"Publisher: {game['publishers_cleaned'][:40]}")
                            
                            with col2:
                                st.write(f"{game['price_category_vnd']}")
                            
                            with col3:
                                st.write(f"{game['release_year']}")
                            
                            with col4:
                                if 'steam_review_positive_ratio' in game:
                                    ratio = game['steam_review_positive_ratio']
                                    if ratio > 0:
                                        st.write(f"{ratio*100:.0f}%")
                            with col5:                        # ← THÊM
                                st.caption(f"Chunk {game['chunk_id']}")  # ← THÊM
                            st.markdown("---")

# ===== TAB 2: RECOMMEND =====
with tab2:
    st.header("Get Recommendations")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        game_input = st.text_input(
            "Enter game name:",
            placeholder="e.g., Counter-Strike, Witcher, Stardew...",
            key="recommend_input"
        )
    
    with col2:
        top_n = st.slider("Recommendations:", 5, 20, 10)
    
    if st.button("Get Recommendations", type="primary", use_container_width=True):
        if game_input:
            with st.spinner("Finding similar games with FAISS..."):
                source_game, recommendations = recommend_with_faiss(
                    game_input, indexes, top_n
                )
            
            if recommendations is None:
                st.error(f"Game not found: **{game_input}**")
                st.info("Try using the Search tab to find the exact name!")
            else:
                st.success(f"Found: **{source_game['name']}** (Chunk {recommendations[0]['chunk_id']})")
                
                col1, col2, col3, col4 = st.columns(4)
                with col1:
                    st.metric("Genre", source_game['genres_cleaned'][:30])
                with col2:
                    st.metric("Price", f"{source_game['price_vnd']:,} VND")
                with col3:
                    st.metric("Year", source_game['release_year'])
                with col4:
                    if 'steam_review_positive_ratio' in source_game and source_game['steam_review_positive_ratio']:
                        ratio = source_game['steam_review_positive_ratio']
                        st.metric("Reviews", f"{ratio*100:.0f}%")
                
                if 'publishers_cleaned' in source_game and source_game['publishers_cleaned']:
                    st.caption(f"Publisher: {source_game['publishers_cleaned']}")
                
# ← THÊM: Hiển thị top 7 tags của source game
                if 'tags_cleaned' in source_game and source_game['tags_cleaned']:
                    all_tags = [t.strip() for t in str(source_game['tags_cleaned']).split(',') if t.strip()]
                    top_tags = all_tags[:7]
                    remaining_tags = all_tags[7:]
                    
                    top_tags_html = ''.join([
                        f'<span style="background: linear-gradient(135deg, #667eea, #764ba2); '
                        f'color: white; padding: 4px 12px; border-radius: 20px; '
                        f'margin: 3px; font-size: 0.85rem; font-weight: bold; '
                        f'display: inline-block;">⭐ {tag}</span>'
                        for tag in top_tags
                    ])
                    remaining_tags_html = ''.join([
                        f'<span style="background: #2d2d2d; color: #aaa; '
                        f'padding: 3px 10px; border-radius: 20px; '
                        f'margin: 3px; font-size: 0.78rem; display: inline-block;">{tag}</span>'
                        for tag in remaining_tags
                    ])
                    
                    st.markdown(f"""
                    <div style="margin: 0.5rem 0 1rem 0;">
                        <p style="color: #888; font-size: 0.85rem; margin-bottom: 6px;">
                            🏷️ <strong>Tags</strong> (top 7 được highlight):
                        </p>
                        <div>{top_tags_html}{remaining_tags_html}</div>
                    </div>
                    """, unsafe_allow_html=True)

                st.markdown("---")
                st.subheader(f"Top {len(recommendations)} Similar Games:")
                
                # Chart
                fig = go.Figure()
                fig.add_trace(go.Bar(
                    x=[rec['name'][:20] for rec in recommendations],
                    y=[rec['similarity_score'] for rec in recommendations],
                    marker_color='rgb(102, 126, 234)',
                    text=[f"{rec['similarity_score']:.3f}" for rec in recommendations],
                    textposition='auto',
                ))
                
                fig.update_layout(
                    title="FAISS Similarity Scores",
                    xaxis_title="Game",
                    yaxis_title="Similarity",
                    height=400,
                    showlegend=False
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Game cards
                for rec in recommendations:
                    publishers_info = f"<p><strong>Publisher:</strong> {rec.get('publishers', 'N/A')}</p>" if rec.get('publishers') else ""
                    review_info = f"<p><strong>Reviews:</strong> {rec.get('review_positive_ratio', 0)*100:.0f}% Positive</p>" if rec.get('review_positive_ratio', 0) > 0 else ""
                    developers_info = f"<p><strong>Developer:</strong> {rec.get('developers', 'N/A')}</p>" if rec.get('developers') else ""
                    
                    # # ← THÊM: Build tags HTML cho từng recommended game
                    # tags_html = ""
                    # if rec.get('tags'):
                    #     all_tags = [t.strip() for t in str(rec['tags']).split(',') if t.strip()]
                    #     top_tags = all_tags[:7]
                    #     remaining_tags = all_tags[7:]

                    #     top_tags_str = ''.join([
                    #         f'<span style="background: rgba(255,255,255,0.25); '
                    #         f'color: white; padding: 3px 10px; border-radius: 15px; '
                    #         f'margin: 2px; font-size: 0.78rem; font-weight: bold; '
                    #         f'display: inline-block;">⭐ {tag}</span>'
                    #         for tag in top_tags
                    #     ])
                    #     remaining_str = ''.join([
                    #         f'<span style="background: rgba(255,255,255,0.10); '
                    #         f'color: rgba(255,255,255,0.7); padding: 2px 8px; border-radius: 12px; '
                    #         f'margin: 2px; font-size: 0.72rem; display: inline-block;">{tag}</span>'
                    #         for tag in remaining_tags
                    #     ])
                    #     tags_html = f"""
                    #     <div style="margin-top: 8px;">
                    #         <span style="font-size:0.8rem; opacity:0.8;">🏷️ Tags: </span>
                    #         {top_tags_str}{remaining_str}
                    #     </div>
                    #     """

                    # st.markdown(f"""
                    # <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                    #             padding: 1.5rem; border-radius: 15px; color: white; margin: 1rem 0;">
                    #     <h3>#{rec['rank']} - {rec['name']}</h3>
                    #     <p><strong>Genre:</strong> {rec['genres']}</p>
                    #     {developers_info}
                    #     {publishers_info}
                    #     <p><strong>Price:</strong> {rec['price_vnd']:,} VND ({rec['price_category']})</p>
                    #     <p><strong>Year:</strong> {rec['release_year']}</p>
                    #     {review_info}
                    #     {tags_html}
                    # </div>
                    # """, unsafe_allow_html=True)

                    # Build tags HTML — tách hoàn toàn, không lồng f-string
                    tags_html = ""
                    raw_tags = str(rec.get('tags', '') or '')
                    if raw_tags.strip() and raw_tags.strip() not in ('nan', 'None'):
                        all_tags = [t.strip() for t in raw_tags.split(',') if t.strip()]
                        top_tags = all_tags[:7]
                        remaining_tags = all_tags[7:]

                        # Build từng phần riêng lẻ — KHÔNG lồng f-string
                        top_spans = ""
                        for tag in top_tags:
                            top_spans += '<span style="background:rgba(255,255,255,0.25);color:white;padding:3px 10px;border-radius:15px;margin:2px;font-size:0.78rem;font-weight:bold;display:inline-block;">⭐ ' + tag + '</span>'

                        remaining_spans = ""
                        for tag in remaining_tags:
                            remaining_spans += '<span style="background:rgba(255,255,255,0.10);color:rgba(255,255,255,0.7);padding:2px 8px;border-radius:12px;margin:2px;font-size:0.72rem;display:inline-block;">' + tag + '</span>'

                        tags_html = '<div style="margin-top:8px;"><span style="font-size:0.8rem;opacity:0.8;">🏷️ Tags: </span>' + top_spans + remaining_spans + '</div>'

                    st.markdown(
                        '<div style="background:linear-gradient(135deg,#667eea 0%,#764ba2 100%);padding:1.5rem;border-radius:15px;color:white;margin:1rem 0;">'
                        # + f'<h3>#{rec["rank"]} - {rec["name"]}</h3>'
                        + f'<h3>#{rec["rank"]} - {rec["name"]}'
                        + f'<span style="background:rgba(255,255,255,0.2);font-size:0.7rem;'
                        + f'padding:3px 10px;border-radius:10px;margin-left:10px;'
                        + f'vertical-align:middle;">📦 Chunk {rec["chunk_id"]}</span></h3>'
                        + f'<p><strong>Genre:</strong> {rec["genres"]}</p>'
                        + developers_info
                        + publishers_info
                        + f'<p><strong>Price:</strong> {rec["price_vnd"]:,} VND ({rec["price_category"]})</p>'
                        + f'<p><strong>Year:</strong> {rec["release_year"]}</p>'
                        + review_info
                        + tags_html
                        + '</div>',
                        unsafe_allow_html=True
                    )
                    
                    similarity_pct = rec['similarity_score'] * 100
                    st.markdown(f"""
                    <div style="background: #f0f0f0; border-radius: 10px; overflow: hidden; margin-bottom: 1rem;">
                        <div style="background: linear-gradient(90deg, #4ECDC4, #44A08D); 
                                    width: {similarity_pct}%; height: 30px; 
                                    display: flex; align-items: center; justify-content: center;
                                    color: white; font-weight: bold;">
                            Similarity: {similarity_pct:.1f}%
                        </div>
                    </div>
                    """, unsafe_allow_html=True)



