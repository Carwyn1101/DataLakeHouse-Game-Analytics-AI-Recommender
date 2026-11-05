# Project instructions

## Repository identity

- The single source of truth is `Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender`:
  https://github.com/Carwyn1101/DataLakeHouse-Game-Analytics-AI-Recommender
- `Carwyn1101/KLTN` is the former name of this same repository, not a separate project.
- Before changing files, inspect the repository root, current branch, working-tree status, and remotes. Preserve all existing uncommitted work.
- If a remote uses the former URL, propose the canonical URL and ask for explicit approval before changing Git remote configuration.

## Development and review rules

- Work on a dedicated feature branch for each task. Never push directly to `main`.
- Create a separate, scoped Pull Request for review when the task requires repository changes.
- Never merge a Pull Request without explicit approval.
- Preserve the existing Airflow, Spark/PySpark, Iceberg, Hive Metastore, MinIO, and Bronze/Silver/Gold architecture unless changes are approved.
- Keep modifications within the assigned task scope. Do not implement later phases automatically.
- Do not modify or delete persisted production data.
- Never commit credentials, secrets, datasets, or generated runtime artifacts. Use synthetic fixtures for tests and keep runtime outputs outside commits.
- Do not reset, clean, stash, force-push, or delete existing work to prepare a task. Do not include another task's staged or untracked files in a commit.
- Run available tests appropriate to the change and report the actual commands and results. Never claim unexecuted or skipped tests passed.
- Distinguish confirmed defects, observed test results, assumptions, and potential risks. Do not claim runtime behavior was verified by static inspection alone.
- Stop after each assigned task and wait for Tech Lead review before starting the next task or phase. Only perform follow-up implementation when explicitly assigned or approved.

## Final task reports

- Write the entire final report in Vietnamese, retaining English technical terms, filenames, and code identifiers.
- Put the entire report as plain Markdown inside exactly one fenced code block. Do not put report content outside that block or nest additional fenced blocks inside it.
- Use exactly the headings and fields below, in this order. Use `COMPLETED`, `PARTIAL`, or `BLOCKED` accurately; required deliverables must exist before reporting `COMPLETED`.
- Keep reports concise but technically complete. Include direct GitHub links for the Pull Request and important files, actual test commands/results, limitations, remaining issues, priorities, and review requests.
- If a field does not apply, state that explicitly. Do not invent a branch, PR, test result, or approval.

Report template (place this entire template, filled in, inside one fenced Markdown block):

# TASK REPORT

## 1. Task Information

- Task ID:
- Task name:
- Status: COMPLETED / PARTIAL / BLOCKED
- Branch:
- Pull Request URL:

## 2. Summary

Mô tả ngắn gọn kết quả đã hoàn thành.

## 3. Changes

Liệt kê tất cả file đã tạo, sửa hoặc xóa, lý do và GitHub links tương ứng.

## 4. Technical Findings

Trình bày phát hiện, quyết định kỹ thuật, lỗi và rủi ro; phân biệt vấn đề đã xác nhận với giả định.

## 5. Testing

- Tests executed:
- Tests passed:
- Tests failed:
- Tests skipped:
- Tests not executed:
- Environment limitations:

Ghi lệnh thực tế khi phù hợp; không báo test chưa chạy là đã pass.

## 6. Remaining Issues

Liệt kê vấn đề chưa xử lý, giới hạn và blockers.

## 7. Recommendations

Đề xuất bước tiếp theo theo thứ tự ưu tiên; không tự triển khai phase kế tiếp.

## 8. Review Notes

Nêu các nội dung cần Tech Lead hoặc Project Owner phê duyệt; không tự merge PR.
