# Bối cảnh triển khai luận văn

Ghi chú này phục hồi các nội dung kỹ thuật từ `CODE_FLOW_DOCUMENTATION.md` và `DEFENSE_QA.md` ở revision luận văn `7067e7c`, cùng phần khái niệm được bổ sung trong bản staged của `DEFENSE_QA.md`. Tài liệu gốc mô tả một lần triển khai trước đây. Phần bên dưới tách những quan sát lịch sử khỏi hành vi của source hiện tại. [Kiến trúc hiện tại](../architecture.md)

## Luồng thu thập và phục vụ

- Luận văn lấy metadata từ Steam và tín hiệu sử dụng từ SteamSpy. Collector games lịch sử xuất 15 cột. Tags được thu thập riêng theo `steam_appid`. Header của file games đã enrich trong thesis workspace có 16 cột, với `tags` ở cuối. Chưa có bằng chứng cho biết file này được nối bằng lệnh hoặc công cụ nào.
- Kiến trúc sử dụng Bronze để giữ CSV đầu vào, Silver để chuẩn hóa và quản lý phiên bản, Gold để phục vụ truy vấn BI và ứng dụng. SCD2 phản ánh lịch sử của các thuộc tính được theo dõi. Những số liệu review/popularity trong Gold phản ánh trạng thái hiện hành của pipeline.
- Dashboard luận văn dùng các view Dremio lịch sử theo thông tin từ Project Owner. Tên view và output contract được giữ trong [legacy SQL](Insights.legacy.sql.txt). Các giới hạn về đơn vị và grain được giải thích trong [SQL guide](../sql-analytics.md).
- Tài liệu luận văn mô tả cách kết hợp tín hiệu có cấu trúc, độ tương đồng ngữ nghĩa và chất lượng review trong ứng dụng AI. Công thức và điều kiện routing của bản phục hồi đã thay đổi. Đọc [AI guide](../../LakeHouse_Project/streamlit-ai/AI_FEATURES_DOCUMENT.md) để biết contract hiện tại và [evaluation guide](../../LakeHouse_Project/streamlit-ai/evaluation/README_RECOMMENDER_EVALUATION.md) để biết benchmark mới đo điều gì.

## Khái niệm từ phần bảo vệ

Data warehouse tổ chức dữ liệu có schema ổn định để truy vấn và báo cáo. Data lake lưu các tập dữ liệu với nhiều mức xử lý trên storage dùng chung. Lakehouse kết hợp object storage với metadata và table format hỗ trợ các thao tác như schema management, snapshot và cập nhật bảng. Trong dự án, MinIO giữ dữ liệu, Iceberg quản lý định dạng bảng, Hive Metastore quản lý catalog và Spark thực hiện biến đổi.

Snapshot và khả năng commit ở cấp bảng không bảo đảm một DAG nhiều task ghi nguyên tử qua toàn bộ bảng. [Reliability audit](../reliability-audit.md)

Ghi chú lược bỏ chi tiết máy triển khai, thông tin truy cập và số liệu thực nghiệm chưa được kiểm chứng trong Phase 4. Các artifact riêng tư và kết quả luận văn gốc được giữ tại nguồn.
