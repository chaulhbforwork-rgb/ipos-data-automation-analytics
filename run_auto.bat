@echo off
chcp 65001 > nul
set PYTHONIOENCODING=utf-8

:: Di chuyển vào ổ D
D:

:: Di chuyển vào thư mục chứa code
cd "D:\Làm việc - Bảo Châu\Project\Scraping_data_ban_Pos"

:: Sử dụng đường dẫn TUYỆT ĐỐI đến Python để chạy code
"D:\Làm việc - Bảo Châu\Python\python.exe" "Scraping_data_ban_Pos.py"

:: Không dùng lệnh pause để cửa sổ tự đóng sau khi xong