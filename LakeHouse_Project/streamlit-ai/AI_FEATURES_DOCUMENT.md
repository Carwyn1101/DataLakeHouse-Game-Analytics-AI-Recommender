# Tài Liệu Tính Năng AI Hiện Tại

## 1. Tổng quan chức năng

Ứng dụng hiện có 3 khu vực chính:
- Search tab: tìm kiếm lexical trực tiếp từ chỉ mục FAISS.
- Recommend tab: gợi ý hybrid từ một game seed.
- Chatbot tab: định tuyến câu hỏi vào 4 nhánh tool.

4 nhánh trong Chatbot:
- search_game
- recommend_from_game
- analyze_data
- compare_games

## 2. Điều kiện hệ thống để AI hoạt động

### 2.1 Điều kiện cần (bắt buộc)
- Chỉ mục dữ liệu phải build thành công qua auto_build_indexes_if_needed.
- Chỉ mục phải load được qua load_all_faiss_indexes.
- Catalog dữ liệu game phải có dữ liệu.

Nếu các điều kiện trên không đạt, app dừng sớm và không vào các nhánh AI.

### 2.2 Điều kiện đủ để có phản hồi
- Người dùng gửi câu hỏi trong Chatbot tab.
- route_intent xác định được 1 trong 4 intent (heuristic hoặc LLM parser).
- Nhánh tương ứng có dữ liệu đầu vào hợp lệ.

### 2.3 LLM là tùy chọn hay bắt buộc
- LLM không bắt buộc để hệ thống chạy.
- Nếu Ollama không sẵn sàng hoặc LLM trả lời lỗi/cụt, code dùng fallback deterministic.
- Các phần có LLM summary: recommend, compare, analyze segment, analyze single game.
- Intent parser bằng LLM chỉ chạy khi bật USE_LLM_INTENT_PARSING và Ollama available.

## 3. Điều kiện cần và đủ theo từng nhánh

## 3.1 Nhánh search_game

### Điều kiện cần
- Intent được route về search_game.

### Điều kiện đủ để có kết quả bảng
- search_hybrid trả DataFrame không rỗng.

### Khi nào route vào nhánh này
- Mặc định fallback nếu không khớp các từ khóa compare/analyze/recommend.
- Hoặc LLM parser trả search_game.

### Prompt mẫu để vào nhánh
- tim game soulslike duoi 500k
- game coop sinh ton review cao
- tactical shooter pvp

## 3.2 Nhánh recommend_from_game

### Điều kiện cần
- Intent được route về recommend_from_game.
- Trích được seed game từ câu hỏi hoặc từ toàn bộ câu.

### Điều kiện đủ để có gợi ý
- recommend_with_faiss tìm được source game.
- Danh sách recs không rỗng sau hybrid rerank.

### Từ khóa route chính
- Yêu cầu có cụm chỉ game gốc và tên seed, ví dụ `gợi ý từ game Portal`.
- Prompt khám phá như `gợi ý game co-op` đi vào search.

### Trigger phrases để tách seed
- goi y tu game
- de xuat tu game
- recommend from game
- similar to
- tim game tuong tu
- game tuong tu
- giong game

### Prompt mẫu để vào nhánh
- goi y tu game elden ring
- recommend from game stardew valley
- tim game tuong tu hades

## 3.3 Nhánh compare_games

### Điều kiện cần
- Intent được route về compare_games.
- Parse được 2 tên game từ câu hỏi.

### Điều kiện đủ để có so sánh đầy đủ
- Cả 2 game đều match được trong catalog qua pick_game_by_name.

### Từ khóa route chính
- so sanh
- compare
- vs
- Dạng `A với B` chỉ route compare khi mỗi vế khớp chính xác một tên game khác nhau trong catalog. Câu tìm kiếm chứa `với` vẫn route search khi điều kiện này không đạt.

### Quy tắc parse tên 2 game
- Bỏ tiền tố dạng so sanh giua, so sanh game, so sanh, giua.
- Tách theo các separator: vs, voi, with, va, and.

### Prompt mẫu để vào nhánh
- so sanh elden ring vs sekiro
- compare hades with dead cells
- so sanh game fifa 23 va efootball

## 3.4 Nhánh analyze_data

### Điều kiện cần
- Intent được route về analyze_data.

### Phân nhánh bên trong analyze_data
- Single-game analysis khi đồng thời thỏa:
- Trích được tên game từ mẫu analyze game hoặc phan tich game.
- Tìm thấy game trong catalog.
- Không có filter segment.
- Không có từ khóa ép chế độ thị trường.

- Segment analysis khi một trong các điều kiện đúng:
- Có filter segment (coop, price, review, year).
- Có từ khóa ép mode thị trường.
- Không match được game cụ thể.

### Từ khóa route chính
- phan tich
- analyze
- thong ke
- xu huong

### Từ khóa ép segment mode
- thi truong
- phan khuc
- xu huong
- thong ke
- toan canh
- mat bang

### Filter tự nhiên đang hỗ trợ
- Co-op:
- coop, co op, cooperative

- Price:
- tu 200k den 500k
- duoi 500k
- tren 200k
- under, from, to, <=, >=, max, min

- Review:
- review tren 85
- review >= 0.8
- review duoi 70
- review tot (heuristic -> min 0.8)

- Year:
- tu nam 2018 den 2022
- sau 2020
- truoc 2019

### Prompt mẫu để vào single-game
- phan tich game elden ring
- analyze game baldur gate 3
- review game hades

### Prompt mẫu để vào segment
- phan tich thi truong game coop duoi 500k
- thong ke phan khuc game review tren 85 tu nam 2020
- xu huong game tu 200k den 600k

## 4. Cách route intent hiện tại

### 4.1 Heuristic mặc định
Ưu tiên kiểm tra lần lượt:
1. recommend_from_game khi có cú pháp chỉ seed và tên game.
2. compare_games khi có cue rõ ràng.
3. analyze_data khi có cue phân tích.
4. Nếu chưa có intent, kiểm tra dạng hai game với catalog rồi fallback về search_game.

### 4.2 LLM intent parsing (tùy chọn)
Điều kiện chạy:
- USE_LLM_INTENT_PARSING = true
- Ollama available

LLM phải trả duy nhất một trong hai nhãn:
- search_game
- analyze_data

Routing compare và recommend có cú pháp rõ ràng được xử lý trước. LLM chỉ có thể chọn search hoặc analyze cho các trường hợp còn lại. Nhãn không hợp lệ fallback về heuristic.

## 5. Kết quả trả về từng nhánh

- search_game:
- Nội dung tóm tắt + tên game, tags và publisher rút gọn, chunk và final_score khi có.

- recommend_from_game:
- LLM/fallback explanation + seed game + bảng gợi ý.

- compare_games:
- LLM/fallback explanation + bảng so sánh giá, review ratio, năm phát hành.

- analyze_data:
- Single-game: phân tích 1 game + bảng hồ sơ game.
- Segment: phân tích thị trường + bảng metric để vẽ chart.

## 6. Gợi ý prompt nhanh theo mục tiêu

- Muốn tìm game:
- tim game roguelike coop duoi 300k

- Muốn gợi ý theo game gốc:
- goi y tu game hollow knight

- Muốn so sánh 2 game:
- so sanh cyberpunk 2077 vs starfield

- Muốn phân tích 1 game:
- phan tich game sekiro

- Muốn phân tích cả phân khúc:
- thong ke thi truong game coop review tren 90 duoi 400k tu nam 2020

## 7. Ghi chú vận hành

- Matching tên game dùng contains và normalize dấu tiếng Việt, nên chịu lỗi chính tả nhẹ tốt hơn trước.
- Nếu prompt không rõ ý định, hệ thống có thể rơi về search_game.
- Nếu dữ liệu lọc rỗng trong analyze segment, bot trả thông báo không có dữ liệu thỏa điều kiện.

### Contract hybrid reranking

- Mỗi dòng ưu tiên `steam_review_positive_ratio` hợp lệ từ Gold/lexical, rồi dùng `review_positive_ratio` từ FAISS. Nếu cả hai hợp lệ và khác nhau, ưu tiên Gold. Chỉ giá trị hữu hạn trong [0, 1] được dùng. Khi cả hai không hợp lệ, review quality bằng 0.
- Giữ các trường ratio gốc và trọng số hybrid. Điểm semantic dùng giá trị cao nhất theo App ID hợp lệ sau khi chặn điểm về [0, 1]. Thiếu điểm semantic dùng 0.
- App ID dùng cùng quy tắc chuẩn hóa với FAISS. Candidate trùng ID giữ dòng có final_score cao nhất. Khi điểm bằng nhau, giữ thứ tự đầu vào. Top-N được áp dụng sau dedup.
