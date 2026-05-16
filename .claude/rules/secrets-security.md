# Secrets & API Key Security Rules

## Quy tắc bắt buộc (KHÔNG được vi phạm)
- **KHÔNG BAO GIỜ** đặt API key, password, secret trong bất kỳ file nào ngoài `docker/.env`
- **KHÔNG BAO GIỜ** hardcode credentials trong: source code, Dockerfile, docker-compose.yml, config files, scripts, notebooks, docs
- **KHÔNG BAO GIỜ** commit `.env` lên git — file này phải có trong `.gitignore`
- **KHÔNG BAO GIỜ** in API key ra stdout/log, kể cả khi debug

## Cách đúng
- Secrets → `docker/.env` (gitignored)
- Template không có giá trị thật → `docker/.env.example` (committed)
- Truy cập trong code: `os.getenv('VAR_NAME')` hoặc `${VAR_NAME}` trong docker-compose
- Truy cập trong Trino catalog properties: `${ENV:VAR_NAME}`

## Nếu key bị lộ
1. Thu hồi key ngay lập tức tại provider (Google AI Studio, AWS, etc.)
2. Tạo key mới
3. Cập nhật `docker/.env` — KHÔNG cập nhật file nào khác
4. Restart service liên quan: `docker compose up -d --force-recreate <service>`
5. Nếu đã commit lên git: xóa khỏi history bằng `git filter-branch` hoặc BFG Repo Cleaner

## Files được phép chứa secrets
| File | Được phép | Lý do |
|------|-----------|-------|
| `docker/.env` | ✅ | Gitignored, local only |
| `docker/.env.example` | ✅ placeholder | Chỉ chứa tên biến, không có giá trị thật |
| Bất kỳ file nào khác | ❌ | Có thể bị commit hoặc log |
