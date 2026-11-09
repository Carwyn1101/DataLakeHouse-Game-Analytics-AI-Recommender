# LakeHouse Game Analytics & AI Recommender

Dự án này xây dựng một lakehouse xử lý dữ liệu game theo kiến trúc Bronze/Silver/Gold, có pipeline Airflow, lưu trữ MinIO/S3, catalog Hive Metastore + Iceberg, truy vấn/phân tích qua Dremio, trực quan hóa bằng PowerBI và ứng dụng AI Streamlit để tìm kiếm, gợi ý, so sánh, phân tích game.

## Thành phần chính

| Thành phần | Vai trò |
| --- | --- |
| Airflow | Điều phối pipeline upload raw data, Bronze -> Silver, Silver -> Gold |
| MinIO | Object storage giả lập S3, lưu lakehouse bucket `lakehouse` |
| Spark + Iceberg | Xử lý ETL và ghi bảng Iceberg |
| Hive Metastore | Catalog metadata cho Iceberg tables |
| PostgreSQL | Metadata DB cho Airflow và Hive Metastore |
| Dremio | Truy vấn/phân tích dữ liệu lakehouse |
| Ollama | Local LLM/embedding backend cho AI app |
| ChromaDB | Vector database service trong compose; AI app hiện dùng Chroma local persistence trong `/app/chroma_db` |
| Streamlit AI | UI tìm kiếm, gợi ý, chatbot phân tích game |

## Cấu trúc thư mục

```text
KLTN/
├── README.md
├── data/                         # Raw input data được mount vào Airflow
└── LakeHouse_Project/
    ├── docker-compose.yml         # Toàn bộ stack lakehouse + AI
    ├── airflow/
    │   ├── dags/                  # DAG definitions
    │   ├── config/
    │   ├── logs/
    │   └── Dockerfile.airflow
    ├── scripts/                   # ETL business logic dùng bởi Airflow
    │   ├── rawData_to_bronze.py
    │   ├── bronze_to_silver_etl.py
    │   └── silver_to_gold_etl.py
    ├── minio/data/                # Persisted MinIO data
    ├── hive/                      # Hive Metastore image/config
    ├── spark/                     # Spark config/jars
    ├── dremio/                    # Dremio data/config
    └── streamlit-ai/
        ├── app.py                 # Streamlit UI
        ├── src/                   # AI retrieval/routing modules
        ├── faiss_indexes/         # FAISS index files persisted on host
        ├── chroma_db/             # Local Chroma persistence used by app
        └── AI_FEATURES_DOCUMENT.md
```

## Luồng dữ liệu

1. Raw files trong `KLTN/data` được Airflow đọc và upload vào MinIO Bronze layer.
2. Bronze -> Silver:
   - Làm sạch dữ liệu game/review/popularity.
   - Ghi bảng Iceberg vào `hive.silver`.
   - Có xử lý SCD Type 2 cho dữ liệu thay đổi.
3. Silver -> Gold:
   - Tạo dimensions: `dim_game`, `dim_date`, `dim_developer`, `dim_genre`, `dim_publisher`, `dim_category`, `dim_tag`, `dim_detail_info`, `dim_review_info`, `dim_popularity_info`.
   - Tạo bridge tables giữa game và developer/genre/publisher/category/tag.
   - Tạo fact snapshots: `fact_game_snapshot`, `fact_review_snapshot`, `fact_popularity_snapshot`.
4. Streamlit AI đọc Gold/Silver model để build FAISS index và Semantic index.
5. Tại giao diện ứng dụng, người dùng có thể sử dụng các bộ lọc đơn giản ở tab Search và Recommend, hoặc tận dụng AI Chatbot nhập liệu để thực hiện tra cứu, nhận đề xuất, so sánh và phân tích thông tin một cách toàn diện.

## Airflow DAGs

| DAG | File | Mục đích |
| --- | --- | --- |
| `auto_upload_bronze_to_minio` | `airflow/dags/auto_upload_data.py` | Phát hiện file raw mới và upload vào MinIO Bronze |
| `bronze_to_silver_etl` | `airflow/dags/bronze_to_silver_etl.py` | Chạy ETL Bronze -> Silver bằng Spark/Iceberg |
| `gold_layer_etl_complete` | `airflow/dags/silver_to_gold_etl.py` | Build Gold layer dimensions, bridges, facts và verify |

## Ứng dụng AI

Streamlit AI nằm trong `LakeHouse_Project/streamlit-ai`.

Các tab chính:

- `Search`: lexical search trực tiếp trên catalog/index.
- `Recommend`: gợi ý game từ seed game bằng FAISS/hybrid ranking.
- `Chatbot`: route câu hỏi vào tool phù hợp.

Chatbot hỗ trợ 4 intent:

- `search_game`: tìm game theo mô tả.
- `recommend_from_game`: gợi ý game tương tự từ game gốc.
- `compare_games`: so sánh 2 game.
- `analyze_data`: phân tích 1 game hoặc một phân khúc thị trường.

Chi tiết logic AI nằm ở:

```text
LakeHouse_Project/streamlit-ai/AI_FEATURES_DOCUMENT.md
```

## AI index persistence

FAISS và Chroma đang được persist trực tiếp ra thư mục host qua bind mount `./streamlit-ai:/app`:

```text
streamlit-ai/faiss_indexes/
streamlit-ai/chroma_db/
```

Lưu ý:

- `faiss_indexes/` chứa `faiss_chunk_*.index` và `metadata_chunk_*.pkl` sau khi build.
- `chroma_db/` chứa local Chroma files dùng bởi `chromadb.PersistentClient(path=CHROMA_DIR)`.
- Docker Compose vẫn có service `chromadb`, nhưng code hiện tại của app dùng local Chroma path thay vì HTTP Chroma server.

## Yêu cầu môi trường

- Docker Desktop hoặc Docker Engine có hỗ trợ Docker Compose.
- Máy đủ RAM cho Airflow + Spark + Dremio + Ollama + Streamlit.
- Raw data đặt trong:

```text
D:/TLCN_KLTN/KLTN/data
```

Nếu đổi vị trí raw data, cần cập nhật volume mount trong `LakeHouse_Project/docker-compose.yml`.

## Chạy dự án

Từ thư mục project:

```powershell
cd D:\TLCN_KLTN\KLTN\LakeHouse_Project
docker compose up -d --build
```

Kiểm tra container:

```powershell
docker compose ps
```

Xem log Streamlit AI:

```powershell
docker logs -f game-recommender-ai
```

## Cổng truy cập

| Service | URL |
| --- | --- |
| Airflow | http://localhost:8080 |
| MinIO API | http://localhost:9000 |
| MinIO Console | http://localhost:9001 |
| Dremio | http://localhost:9047 |
| Spark Master UI | http://localhost:18080 |
| Ollama | http://localhost:11434 |
| ChromaDB service | http://localhost:8000 |
| Streamlit AI | http://localhost:8501 |

Thông tin mặc định thường dùng trong compose:

- Airflow: `airflow` / `airflow`
- MinIO: `hongphuc` / `hongphuc`

## Quy trình vận hành đề xuất

1. Khởi động stack Docker.
2. Mở Airflow tại `http://localhost:8080`.
3. Chạy DAG `auto_upload_bronze_to_minio`.
4. Chạy DAG `bronze_to_silver_etl`.
5. Chạy DAG `gold_layer_etl_complete`.
6. Mở Streamlit AI tại `http://localhost:8501`.
7. Lần đầu app chạy, nếu thiếu FAISS index, app sẽ tự build index vào `streamlit-ai/faiss_indexes`.
8. Semantic index sẽ được build dần vào `streamlit-ai/chroma_db` nếu Ollama embedding backend khả dụng.

## Một số lệnh hữu ích

Dừng toàn bộ stack:

```powershell
docker compose down
```

Dừng và xóa named volumes:

```powershell
docker compose down -v
```

Rebuild riêng Streamlit AI:

```powershell
docker compose build streamlit-ai
docker compose up -d streamlit-ai
```

Xem log AI app:

```powershell
docker compose logs -f streamlit-ai
```

Khởi động lại AI app:

```powershell
docker-compose restart streamlit-ai
docker restart game-recommender-ai
```

## Ghi chú kỹ thuật

- FAISS index được chia chunk để giảm tải memory và cho phép load/search từng phần.
- Mỗi chunk có TF-IDF vocabulary riêng, nên khi recommend cross-chunk app phải rebuild query vector theo vectorizer của từng chunk.
- LLM không bắt buộc để app hoạt động. Nếu Ollama không sẵn sàng, chatbot dùng fallback deterministic ở các phần giải thích.
- `USE_LLM_INTENT_PARSING=false` mặc định, nên intent routing chủ yếu dùng heuristic.

## Tài liệu liên quan

- `LakeHouse_Project/streamlit-ai/AI_FEATURES_DOCUMENT.md`: tài liệu chi tiết tính năng AI/chatbot.
- [Insights.sql](LakeHouse_Project/Insights.sql): các view phân tích trên Gold layer. Xem [hướng dẫn SQL analytics](docs/sql-analytics.md).
- `LakeHouse_Project/scripts/`: logic ETL chính.
- `LakeHouse_Project/airflow/dags/`: DAG orchestration.
