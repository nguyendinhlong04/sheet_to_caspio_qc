# Google Sheets to Caspio Transfer

Chuyển dữ liệu từ Google Sheets sang Caspio thông qua API.

## Cấu hình
- Đặt file JSON Google service account trong repo (hoặc sử dụng Secrets/ENV cho CI).
- Sửa các biến cấu hình trong `main()` ở file `sheet_caspio.py`.

## Chạy cục bộ
`pip install -r requirements.txt`
`python sheet_caspio.py`

## Chạy bằng GitHub Actions
- Đặt credentials Google và các biến Caspio vào GitHub Secrets hoặc chỉnh sửa workflow file.
