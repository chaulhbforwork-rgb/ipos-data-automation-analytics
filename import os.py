import os
import re
import ast
import json
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta

# Selenium & Webdriver
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

# --- ACCOUNT CONFIGURATION ---
MY_USER = "chaulhb@nsq.vn"
MY_PASS = "chaulhb@123"

tz_vn = timezone(timedelta(hours=7))

def get_fabi_headers(user, pwd):
    print("--- Step 1: Attempting login to retrieve Token ---")
    chrome_options = Options()
    chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    wait = WebDriverWait(driver, 25)

    try:
        driver.get("https://fabi.ipos.vn/login")
        time.sleep(5) 

        inputs = wait.until(EC.presence_of_all_elements_located((By.TAG_NAME, "input")))
        if len(inputs) >= 2:
            actions = ActionChains(driver)
            actions.move_to_element(inputs[0]).click().perform()
            inputs[0].send_keys(Keys.CONTROL + "a")
            inputs[0].send_keys(Keys.BACKSPACE)
            inputs[0].send_keys(user)
            
            actions.move_to_element(inputs[1]).click().perform()
            inputs[1].send_keys(pwd)
            
            try:
                login_btn = driver.find_element(By.XPATH, "//button[contains(., 'Đăng nhập')]")
            except:
                login_btn = driver.find_element(By.XPATH, "//button[@type='submit']")

            driver.execute_script("arguments[0].click();", login_btn)
        else:
            return None

        print("Waiting for Dashboard to load to capture Token...")
        time.sleep(15) 

        logs = driver.get_log('performance')
        for entry in logs:
            log_obj = json.loads(entry['message'])['message']
            if log_obj['method'] == 'Network.requestWillBeSent':
                request = log_obj.get('params', {}).get('request', {})
                url = request.get('url', '')
                headers_found = request.get('headers', {})

                if 'posapi.ipos.vn' in url:
                    h_lower = {k.lower(): v for k, v in headers_found.items()}
                    auth = h_lower.get('authorization')
                    acc_token = h_lower.get('access_token')

                    if auth and acc_token:
                        print("TOKEN CAPTURED SUCCESSFULLY!")
                        return {
                            'Authorization': auth,
                            'access_token': acc_token,
                            'fabi_type': 'pos-cms',
                            'accept': 'application/json, text/plain, */*',
                            'accept-language': 'vi',
                            'Origin': 'https://fabi.ipos.vn',
                            'Referer': 'https://fabi.ipos.vn/'
                        }
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        driver.quit()

def get_7_day_time_range():
    now = datetime.now(tz_vn)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    to_date_dt = today_start - timedelta(days=0, seconds=1)
    from_date_dt = today_start - timedelta(days=1)
    
    from_ts_ms = int(from_date_dt.timestamp() * 1000)
    to_ts_ms = int(to_date_dt.timestamp() * 1000)
    
    return from_ts_ms, to_ts_ms, from_date_dt.strftime('%d-%m-%Y'), to_date_dt.strftime('%d-%m-%Y')

STORE_MAP = {
    "7d2805d3-1433-41b2-9026-8b0971262cdd": "Ráng Chiều",
    "f8716c71-ebb4-49fa-abaa-17ddfc1c4c97": "Thương",
    "8a506f0b-70e6-4531-828c-f72d5786443d": "Chạng Vạng Trần Não",
    "4a5d2701-71d4-45d2-bc26-03c521ab9bfb": "Chạng Vạng Hàng Xanh",
    "5b8e8a32-017a-41eb-9a3b-33107b0d1a12": "Chênh Vênh"
}

def get_sale_change_log(headers, from_ts_ms, to_ts_ms):
    print(f"--- Step 2: Querying Sale Change Log ---")
    all_rows = [] 
    
    for s_uid in STORE_MAP.keys():
        print(f"Scanning logs for branch: {STORE_MAP[s_uid]}...")
        page = 1
        
        while True:
            url = "https://posapi.ipos.vn/api/v3/pos-cms/sale-change-log"
            params = {
                'company_uid': '85e55b55-4e0d-41ea-92be-549b254fd6f9',
                'brand_uid': '1207df20-9220-47f1-afff-90faa165fcc7',
                'store_uid': s_uid,
                'start_date': from_ts_ms,
                'end_date': to_ts_ms,
                'page': page,
                'page_size': 100,
                'store_open_at': 2
            }

            try:
                res = requests.get(url, headers=headers, params=params)
                if res.status_code == 200:
                    res_data = res.json()
                    
                    # --- Check data structure ---
                    current_page_items = []
                    if isinstance(res_data, list):
                        current_page_items = res_data
                    elif isinstance(res_data, dict):
                        # Try to get from 'data' -> 'items' or directly from 'data'
                        data_payload = res_data.get('data', [])
                        if isinstance(data_payload, dict):
                            current_page_items = data_payload.get('items', [])
                        else:
                            current_page_items = data_payload

                    if not current_page_items:
                        break

                    for log_entry in current_page_items:
                        change_data_str = log_entry.get('change_data', '{}')
                        try:
                            # Parse raw JSON from change_data column
                            if isinstance(change_data_str, str):
                                data_json = json.loads(change_data_str.replace("'", "\""))
                            else:
                                data_json = change_data_str
                            
                            sale_details = data_json.get('sale_detail', [])
                            common_data = {k: v for k, v in data_json.items() if k != 'sale_detail'}
                            
                            if sale_details:
                                for item in sale_details:
                                    new_row = log_entry.copy()
                                    new_row.update(common_data)
                                    new_row.update(item)
                                    all_rows.append(new_row)
                            else:
                                new_row = log_entry.copy()
                                new_row.update(common_data)
                                all_rows.append(new_row)
                        except:
                            all_rows.append(log_entry.copy())

                    if len(current_page_items) < 100:
                        break
                    page += 1
                else:
                    break
            except Exception as e:
                print(f"Error at page {page}: {e}")
                break

    if all_rows:
        return pd.DataFrame(all_rows)
    return None

def get_sale_by_date(headers, from_ts_ms, to_ts_ms):
    print("--- Step 2: Retrieving Invoice List ---")
    all_dates = []
    for s_uid in STORE_MAP.keys():
        page = 1
        while True:
            url = "https://posapi.ipos.vn/api/reports_v1/v3/pos-cms/report/sale-by-date"
            params = {
                'company_uid': '85e55b55-4e0d-41ea-92be-549b254fd6f9',
                'brand_uid': '1207df20-9220-47f1-afff-90faa165fcc7',
                'store_uid': s_uid,
                'start_date': from_ts_ms,
                'end_date': to_ts_ms,
                'page': page,
                'page_size': 100,
                'sort': 'dsc',
                'store_open_at': 2
            }
            try:
                res = requests.get(url, headers=headers, params=params)
                if res.status_code == 200:
                    data = res.json().get('data', [])
                    if not data: break
                    for day_data in data:
                        day_data['store_name'] = STORE_MAP[s_uid]
                        all_dates.append(day_data)
                    page += 1
                else: break
            except: break
    return pd.DataFrame(all_dates) if all_dates else None

def get_sale_detail_by_tran_id(headers, df_sales):
    print("--- Step 3: Retrieving item details ---")
    all_details = []
    for index, row in df_sales.iterrows():
        url = "https://posapi.ipos.vn/api/v1/reports/sales/get-sale-by-tran-id"
        params = {
            'tran_id': row['tran_id'], 
            'brand_uid': '1207df20-9220-47f1-afff-90faa165fcc7', 
            'store_uid': row['store_uid'], 
            'company_uid': '85e55b55-4e0d-41ea-92be-549b254fd6f9'
        }
        try:
            res = requests.get(url, headers=headers, params=params)
            if res.status_code == 200:
                items = res.json().get('data', {}).get('sale_detail', [])
                for item in items:
                    new_row = row.to_dict()
                    new_row.update(item)
                    all_details.append(new_row)
            time.sleep(0.05)
        except: continue
    return pd.DataFrame(all_details) if all_details else None

def main():
    # Keep data in memory after function execution
    global sale_change_log, sale_by_date, f_str, t_str
    headers = get_fabi_headers(MY_USER, MY_PASS)
    if not headers: return
    f_ms, t_ms, f_str, t_str = get_7_day_time_range() 

    # Base Directory declaration
    BASE_DIR = r"D:\Làm việc - Bảo Châu\Project\Scraping_data_ban_Pos"
    PATH_LUU_LOG = os.path.join(BASE_DIR, "Data_sale_change_log")
    PATH_LUU_HOA_DON = os.path.join(BASE_DIR, "Data_sale_by_date")
    
    os.makedirs(PATH_LUU_LOG, exist_ok=True)
    os.makedirs(PATH_LUU_HOA_DON, exist_ok=True)

    # Create dynamic file paths
    full_path_log = os.path.join(PATH_LUU_LOG, f"1_Log_Tho_{f_str}_{t_str}.xlsx")
    full_path_hd = os.path.join(PATH_LUU_HOA_DON, f"2_Hoa_Don_Tho_{f_str}_{t_str}.xlsx")

    # 1. Scraping and saving Log
    raw_log = get_sale_change_log(headers, f_ms, t_ms)
    if raw_log is not None:
        raw_log.to_excel(full_path_log, index=False)
        print(f"Raw Log saved at: {full_path_log}")
        
        # Reloading data from the saved file
        sale_change_log = pd.read_excel(full_path_log)
        print("sale_change_log loaded from saved file.")

    # 2. Scraping and saving Invoices
    raw_hd = get_sale_by_date(headers, f_ms, t_ms)
    if raw_hd is not None:
        raw_hd = get_sale_detail_by_tran_id(headers, raw_hd)
        if raw_hd is not None:
            raw_hd.to_excel(full_path_hd, index=False)
            print(f"Raw Invoices saved at: {full_path_hd}")
            
            # Reloading data from the saved file
            sale_by_date = pd.read_excel(full_path_hd)
            print("sale_by_date loaded from saved file.")

    # --- Continue processing logic (Split/Merge/Join) here ---
    
if __name__ == "__main__":
    main()

# # 1. Hàm parse an toàn
# def parse_toppings(x):
#     # 1. Nếu x đã là list rồi thì trả về luôn, không cần parse nữa
#     if isinstance(x, list):
#         return x
    
#     # 2. Kiểm tra nếu là NaN (dùng cách kiểm tra an toàn cho cả array)
#     if pd.isna(x) is True:
#         return []
        
#     # 3. Ép về string và xử lý
#     val = str(x).strip()
    
#     if val in ["", "[]", "None", "nan"]:
#         return []
    
#     try:
#         # Thử dùng ast để chuyển chuỗi "[...]" thành list thật
#         return ast.literal_eval(val)
#     except:
#         return []

# --- EXECUTION START ---

# 3. Data Preview (Check first 5 rows)
print("--- Sale Change Log Data ---")
print(sale_change_log.head())

print("\n--- Sale By Date Data ---")
print(sale_by_date.head())

# 1. Safe Parse Function
def parse_toppings(x):
    # Cast to string immediately to avoid float/None errors (missing strip attribute)
    val = str(x).strip() if pd.notna(x) else ""
    
    if val in ["", "[]", "None", "nan"]:
        return []
    
    try:
        # Attempt to parse if it's a JSON string
        import json
        # iPOS often uses single quotes for dicts; standard JSON requires double quotes
        fixed_json = val.replace("'", '"')
        data = json.loads(fixed_json)
        return data if isinstance(data, list) else []
    except:
        return []

# --- EXECUTION START ---

# Step 1: Parse toppings column
sale_by_date['toppings_list'] = sale_by_date['toppings'].apply(parse_toppings)

# Step 2: Add an empty item to keep the main item record
sale_by_date['toppings_to_explode'] = sale_by_date['toppings_list'].apply(lambda x: [None] + x)

# Step 3: Explode
sale_by_date_expanded = sale_by_date.explode('toppings_to_explode').reset_index(drop=True)

# Step 4: Normalize and RENAME TOPPING COLUMNS to avoid overwriting original IDs
toppings_flat = pd.json_normalize(sale_by_date_expanded['toppings_to_explode'])

# IMPORTANT: Only overwrite display columns, DO NOT overwrite system IDs
# List of columns to extract from topping to display in place of the main item
cols_to_update = ['item_name', 'price', 'quantity', 'amount', 'item_id', 'sku']

toppings_flat.index = sale_by_date_expanded.index

# Step 5: Smart Overwrite (Only overwrite display info columns)
for col in toppings_flat.columns:
    if col in cols_to_update and col in sale_by_date_expanded.columns:
        # If it's a topping row (not NaN), take the topping value; otherwise, keep main item info
        sale_by_date_expanded[col] = toppings_flat[col].fillna(sale_by_date_expanded[col])
    elif col not in sale_by_date_expanded.columns:
        # If the topping has a brand new column, add it
        sale_by_date_expanded[col] = toppings_flat[col]

# Step 6: Mark Toppings for visual clarity
is_topping = sale_by_date_expanded['toppings_to_explode'].notna()
sale_by_date_expanded.loc[is_topping, 'item_name'] = "+ " + sale_by_date_expanded.loc[is_topping, 'item_name'].astype(str)

# Step 7: Cleanup
sale_by_date_final = sale_by_date_expanded.drop(columns=['toppings_list', 'toppings_to_explode'])

# Step 8: ENSURE tran_id IS ALWAYS PRESENT (Fallback to original id column if missing)
# Based on the context, tran_id seems to map to the id column
sale_by_date_final['tran_id'] = sale_by_date_final['tran_id'].fillna(sale_by_date_final['id'])

# View results
sale_by_date_final

# 1. Safe extraction function for extra_data
def extract_extra_info(extra_str):
    # Initialize default values for errors or empty data
    results = {
        'peo_count': 0,
        'Membership_Type_Name': '',
        'customer_name': '',
        'customer_phone': ''
    }
    try:
        # Convert extra_data string into a dict
        data = ast.literal_eval(str(extra_str))
        if isinstance(data, dict):
            results['peo_count'] = data.get('peo_count', 0)
            results['Membership_Type_Name'] = data.get('Membership_Type_Name', '')
            results['customer_name'] = data.get('customer_name', '')
            results['customer_phone'] = data.get('customer_phone', '')
    except:
        pass
    return pd.Series(results)

# 2. Áp dụng trích xuất vào DataFrame
# Lưu ý: sale_by_date_final là bảng đã bung topping ở bước trước
extra_info = sale_by_date_final['extra_data'].apply(extract_extra_info)

# 3. Ghép 4 cột mới vào bảng chính (giữ nguyên tên gốc trong JSON)
sale_by_date_final[['peo_count', 'Membership_Type_Name', 'customer_name', 'customer_phone']] = extra_info

# --- KIỂM TRA KẾT QUẢ ---
print(sale_by_date_final[['item_name', 'peo_count', 'Membership_Type_Name', 'customer_name', 'customer_phone']].head())

# sale_by_date_final.to_excel("Sale_By_Date_Unified_Columns.xlsx", index=False)

# --- BƯỚC 1: CHỌN CỘT (Đảm bảo có 'tran_date') ---
cols_to_keep = [
    'store_name', 'tran_id', 'origin_tran_id', 'tran_no', 'tran_date',
    'start_hour', 'start_minute', 'end_hour', 'end_minute',
    'table_name', 'item_name', 'quantity', 'unit_id', 'price_org', 'amount',
    'peo_count', 'Membership_Type_Name', 'customer_name', 'customer_phone',
    'total_amount', 'amount_discount_detail'
]

# Lọc các cột hiện có để tránh lỗi nếu thiếu cột nào đó
existing_cols = [c for c in cols_to_keep if c in sale_by_date_final.columns]
Hoadontheothoigian = sale_by_date_final[existing_cols].copy()

# --- BƯỚC 2: ĐỊNH NGHĨA MAPPING ---
mapping_cols = {
    'store_name': 'Cửa hàng',
    'tran_id': 'Mã hoá đơn',
    'origin_tran_id': 'Mã hoá đơn gốc',
    'tran_no': 'Số hoá đơn',
    'tran_date': 'Ngày',
    'start_hour': 'Giờ vào',
    'start_minute': 'Phút vào',
    'end_hour': 'Giờ ra',
    'end_minute': 'Phút ra',
    'table_name': 'Bàn',
    'item_name': 'Tên hàng',
    'quantity': 'Số lượng',
    'unit_id': 'Đơn vị',
    'price_org': 'Đơn giá',
    'amount': 'Thành tiền',
    'peo_count': 'Số khách',
    'Membership_Type_Name': 'Loại thành viên',
    'customer_name': 'Tên khách',
    'customer_phone': 'SĐT',
    'total_amount': 'Tổng hóa đơn',
    'amount_discount_detail': 'Giảm giá'
}

# --- BƯỚC 3: ĐỔI TÊN ---
Hoadontheothoigian = Hoadontheothoigian.rename(columns=mapping_cols)

# --- BƯỚC 4: XỬ LÝ THỜI GIAN (Dùng unit='ms' để tránh lỗi OutOfBounds) ---
if 'Ngày' in Hoadontheothoigian.columns:
    # Chuyển đổi mili giây sang datetime
    Hoadontheothoigian['Ngày'] = pd.to_datetime(Hoadontheothoigian['Ngày'], unit='ms')

    # Chuyển sang múi giờ VN và định dạng lại
    Hoadontheothoigian['Ngày'] = (Hoadontheothoigian['Ngày']
                                  .dt.tz_localize('UTC')
                                  .dt.tz_convert('Asia/Ho_Chi_Minh')
                                  .dt.strftime('%Y/%m/%d'))

    # Đổi 'Tên khách' thành 'Tên' cho đúng ý bạn
    Hoadontheothoigian = Hoadontheothoigian.rename(columns={'Tên khách': 'Tên'})
else:
    print("Cảnh báo: Không tìm thấy cột 'Ngày' để chuyển đổi!")
# 1. Nếu 'Mã hoá đơn gốc' có giá trị (không NaN), lấy nó đè lên 'Mã hoá đơn'
# Nếu 'Mã hoá đơn gốc' là NaN, nó sẽ giữ nguyên 'Mã hoá đơn' hiện tại
Hoadontheothoigian['Mã hoá đơn'] = Hoadontheothoigian['Mã hoá đơn gốc'].fillna(Hoadontheothoigian['Mã hoá đơn'])

# 2. Xóa cột 'Mã hoá đơn gốc' đi cho sạch bảng
Hoadontheothoigian = Hoadontheothoigian.drop(columns=['Mã hoá đơn gốc'])
# 1. Hàm hỗ trợ định dạng số thành chuỗi 2 chữ số (ví dụ: 9 -> "09")
def format_time(h, m):
    try:
        # Chuyển về số nguyên trước để loại bỏ .0 (nếu có), sau đó định dạng 2 chữ số
        hh = str(int(float(h))).zfill(2)
        mm = str(int(float(m))).zfill(2)
        return f"{hh}:{mm}:00"
    except:
        return "00:00:00" # Trả về mặc định nếu dữ liệu lỗi

# 2. Tạo cột 'Giờ vào' mới từ 'Giờ vào' (cũ) và 'Phút vào'
Hoadontheothoigian['Giờ vào'] = Hoadontheothoigian.apply(
    lambda x: format_time(x['Giờ vào'], x['Phút vào']), axis=1
)

# 3. Tạo cột 'Giờ ra' mới từ 'Giờ ra' (cũ) và 'Phút ra'
Hoadontheothoigian['Giờ ra'] = Hoadontheothoigian.apply(
    lambda x: format_time(x['Giờ ra'], x['Phút ra']), axis=1
)

# 4. Xóa bỏ 2 cột Phút cho gọn bảng (vì đã gộp vào cột Giờ rồi)
Hoadontheothoigian = Hoadontheothoigian.drop(columns=['Phút vào', 'Phút ra'])

# Hiển thị kết quả
Hoadontheothoigian

"""## **Tạo cột Customer Type**"""

# 1. Định nghĩa các điều kiện kiểm tra "Trống"
def is_empty(col):
    return Hoadontheothoigian[col].isna() | (Hoadontheothoigian[col].astype(str).str.strip() == "")

# 2. Xây dựng danh sách điều kiện
condlist = [
    # Nhóm Khách lẻ
    (Hoadontheothoigian['Tên'] == 'iPOS-O2O') | is_empty('Tên') | is_empty('SĐT'),

    # Nhóm Khách mới
    (~is_empty('Tên') & (Hoadontheothoigian['Tên'] != 'iPOS-O2O')) &
    (~is_empty('SĐT')) &
    (is_empty('Loại thành viên') | (Hoadontheothoigian['Loại thành viên'] == 'Thành viên mặc định'))
]

# 3. Phân loại thẳng ra chữ "Loại khách hàng", không tạo cột số trung gian nữa
Hoadontheothoigian['Loại khách hàng'] = np.select(
    condlist=condlist,
    choicelist=['Khách lẻ', 'Khách mới'],
    default='Khách quay lại'
)

# Hiển thị kết quả
Hoadontheothoigian

"""## **Xử lý thông tin Invoices**"""

Hoadontheothoigian = Hoadontheothoigian[[ 'Cửa hàng','Mã hoá đơn','Số hoá đơn','Ngày','Giờ vào', 'Giờ ra', 'Bàn','Tên hàng','Số lượng','Đơn giá','Số khách','Tổng hóa đơn','Giảm giá','Loại thành viên','Tên','SĐT','Loại khách hàng']]
Hoadontheothoigian

# 1. Cột loại trừ: Chỉ những cột thông tin khách hàng riêng biệt không nên fill bừa bãi
cols_to_exclude = ['Loại thành viên', 'Tên', 'SĐT']

# 2. Danh sách cột cần fill: Phải bao gồm cả 'Mã hoá đơn'
# Ta groupby theo 'Số hoá đơn' nên không cần fill chính nó, nhưng cần fill 'Mã hoá đơn'
cols_to_fill = [c for c in Hoadontheothoigian.columns if c not in cols_to_exclude and c != 'Số hoá đơn']

# 3. Thực hiện điền dữ liệu
# Groupby theo Số hoá đơn để tìm những dòng cùng một bill và lấp đầy Mã hoá đơn trống
Hoadontheothoigian[cols_to_fill] = Hoadontheothoigian.groupby('Số hoá đơn')[cols_to_fill].ffill().bfill()

# 2. Sắp xếp dữ liệu theo Hóa đơn, Món và Thời gian (để đảm bảo dòng cũ đứng trước)
Hoadontheothoigian = Hoadontheothoigian.sort_values(by=['Số hoá đơn', 'Ngày', 'Tên hàng'])

# 3. Tạo cột 'Mark' đánh dấu thứ tự xuất hiện của món trong từng hóa đơn
# groupby(['Số hoá đơn', 'Món']) giúp gom nhóm các món giống nhau trong cùng 1 bill
# cumcount() sẽ đánh số 0, 1, 2... cho từng nhóm đó
Hoadontheothoigian['Mark'] = Hoadontheothoigian.groupby(['Số hoá đơn', 'Tên hàng','Số lượng']).cumcount() + 1

# Hiển thị kết quả kiểm tra
Hoadontheothoigian

"""# **Transfer Nhật ký order**"""

# 1. Hàm parse cột toppings (giữ nguyên logic bạn thích)
def parse_toppings(x):
    if pd.isna(x) or str(x).strip() in ["", "[]"]:
        return []
    try:
        # Xử lý chuỗi nháy đơn của iPOS
        return ast.literal_eval(str(x))
    except:
        return []

# --- BẮT ĐẦU XỬ LÝ ---

# Bước 1: Parse cột toppings có sẵn trong file Log
sale_change_log['toppings_list'] = sale_change_log['toppings'].apply(parse_toppings)

# Bước 2: Thêm None vào đầu list để giữ lại dòng món chính
# [None, topping1, topping2] -> Explode ra 3 dòng: 1 món chính, 2 topping
sale_change_log['toppings_to_explode'] = sale_change_log['toppings_list'].apply(lambda x: [None] + x)

# Bước 3: Explode (Từ 843 dòng sẽ ra khoảng 1200 dòng)
log_final = sale_change_log.explode('toppings_to_explode').reset_index(drop=True)

# Bước 4: Normalize (Bung các cột của topping ra)
toppings_flat = pd.json_normalize(log_final['toppings_to_explode'])
toppings_flat.index = log_final.index

# Bước 5: Ghi đè thông minh (Fillna)
# Nếu dòng đó là Topping thì lấy dữ liệu Topping, nếu là món chính (NaN) thì giữ nguyên gốc
for col in toppings_flat.columns:
    if col in log_final.columns:
        log_final[col] = toppings_flat[col].fillna(log_final[col])
    else:
        # Nếu topping có những cột lạ mà món chính không có thì thêm vào luôn
        log_final[col] = toppings_flat[col]

# Bước 6: Thêm dấu + để phân biệt Topping nằm dưới món chính
is_topping = log_final['toppings_to_explode'].notna()
if 'item_name' in log_final.columns:
    log_final.loc[is_topping, 'item_name'] = "+ " + log_final.loc[is_topping, 'item_name'].astype(str)

# Bước 7: Dọn dẹp cột phụ
log_final = log_final.drop(columns=['toppings_list', 'toppings_to_explode'])

# Kiểm tra kết quả
print(f"Tổng số dòng sau khi xử lý: {len(log_final)}")
print(log_final[['tran_id', 'item_name', 'quantity', 'price', 'amount']].head(10))
# log_final.to_excel("Log_Unified_Columns.xlsx", index=False)


# Giả sử df là DataFrame của ông
def get_modify_message(x):
    try:
        # Chuyển chuỗi thành dict
        data = ast.literal_eval(str(x))
        # Lấy đúng trường ông cần, nếu không có thì trả về trống
        return data.get('message_modify_table', '')
    except:
        return ''

# Tạo cột mới
log_final['message_modify_table'] = log_final['extra_data'].apply(get_modify_message)

def get_correct_tran_id(data_str):
    try:
        # Chuyển chuỗi thành dictionary
        data = ast.literal_eval(data_str)

        # Lấy tran_id ở cấp độ cao nhất của dictionary
        if isinstance(data, dict):
            return data.get('tran_id')
        # Nếu là list thì lấy phần tử cuối rồi lấy tran_id
        elif isinstance(data, list) and len(data) > 0:
            return data[-1].get('tran_id')
    except:
        return None

# Áp dụng vào DataFrame
log_final['tran_id'] = log_final['change_data'].apply(get_correct_tran_id).fillna(log_final['tran_id'])

# 1. Tạo cột tạm để xác định dấu (sign)
# Dùng .str.strip() để loại bỏ khoảng trắng thừa trước khi kiểm tra dấu '+'
is_topping = log_final['item_name'].str.strip().str.startswith('+', na=False)

# Nếu KHÔNG PHẢI topping -> Lấy dấu của quantity
# Nếu LÀ topping -> Để NaN để lát nữa điền từ dòng trên xuống
log_final['temp_sign'] = np.where(~is_topping, np.sign(log_final['quantity']), np.nan)

# 2. Điền dấu từ trên xuống dưới (ffill)
# Group theo 'tran_id' để đảm bảo không bị lẫn lộn giữa các hóa đơn
log_final['temp_sign'] = log_final.groupby('tran_id')['temp_sign'].ffill()

# 3. Cập nhật lại số lượng (Quantity)
# Topping sẽ có số lượng = (Giá trị tuyệt đối của nó) * (Dấu của món chính)
# fillna(1) để phòng trường hợp dòng đầu tiên của hóa đơn là topping (hiếm gặp)
log_final['quantity'] = log_final['quantity'].abs() * log_final['temp_sign'].fillna(1)

# log_final.to_excel("Log_Unified_Columns.xlsx", index=False)
log_final

# --- BƯỚC 4: XỬ LÝ THỜI GIAN (Dùng unit='ms' để tránh lỗi OutOfBounds) ---
if 'tran_date' in log_final.columns:
    # Chuyển đổi mili giây sang datetime
    log_final['tran_date'] = pd.to_datetime(log_final['tran_date'], unit='ms')

    # Chuyển sang múi giờ VN và định dạng lại
    log_final['tran_date'] = (log_final['tran_date']
                                  .dt.tz_localize('UTC')
                                  .dt.tz_convert('Asia/Ho_Chi_Minh')
                                  .dt.strftime('%Y/%m/%d %H:%M:%S'))

# 1. Định nghĩa mapping (Vừa chọn cột, vừa đổi tên)
mapping = {
    'tran_id': 'Mã hoá đơn', 'tran_no': 'Số hoá đơn', 'tran_date': 'Thời gian',
    'table_name': 'Bàn', 'employee_name': 'Nhân viên', 'log_type': 'Loại log',
    'message_modify_table': 'Ghi chú', 'item_name': 'Món', 'quantity': 'Số lượng order'
}

# 2. Rút ngắn: Lọc các cột tồn tại và đổi tên ngay lập tức
cols_to_use = [c for c in mapping.keys() if c in log_final.columns]
Nhatkyorder = log_final[cols_to_use].rename(columns=mapping)

# 1. Xác định danh sách các cột cần fillna (tất cả trừ Số hóa đơn và Ghi chú)
cols_to_exclude = ['Số hoá đơn', 'Ghi chú']
cols_to_fill = [c for c in Nhatkyorder.columns if c not in cols_to_exclude and c != 'Mã hoá đơn']

# 2. Groupby theo Mã hóa đơn và tiến hành fillna trong từng nhóm
# Chúng ta dùng ffill rồi bfill để đảm bảo dữ liệu được lấp đầy tối đa
Nhatkyorder[cols_to_fill] = Nhatkyorder.groupby('Mã hoá đơn')[cols_to_fill].ffill().bfill()


Nhatkyorder

"""## **Chỉ lấy Loại log gồm ["-","Sửa đơn","Gộp đơn"]**

### PHÂN LOẠI LOẠI LOG TRONG HỆ THỐNG

## 1. Nhóm cần xử lý (Tính giá trị order theo nhân viên)
Bao gồm: **Sửa đơn**, **Gộp đơn**, **Tách đơn**

* **1.1. Sửa đơn:**
    * Hành vi: Order trực tiếp của nhân viên.
    * Xử lý: Ghi nhận bình thường để tính giá trị thực tế trên hóa đơn.
* **1.2. Gộp đơn:**
    * Cơ chế: (1) Trừ món hóa đơn bị gộp -> (2) Cộng món vào hóa đơn nhận gộp.
    * Vấn đề: Dễ sai lệch doanh số cho người thực hiện thao tác gộp.
    * Xử lý: Truy vết từ mã hóa đơn gốc đến mã cuối để ghi nhận đúng nhân viên order đầu tiên.
* **1.3. Tách đơn:**
    * Cơ chế: Hệ thống chỉ giữ lại món "ở lại" trên đơn gốc.
    * Xử lý: So sánh danh sách món trước/sau khi tách (Inventory tracking) để tìm phần chênh lệch và phân bổ sang hóa đơn con.

---

## 2. Nhóm không cần xử lý
Bao gồm: **"-"**, **"in chốt đồ"**, **"in tạm tính"**, **"in lại hóa đơn"**

* **2.1. "-" (Chuyển bàn):** Chỉ thay đổi vị trí bàn, không đổi bản chất order.
* **2.2. "in chốt đồ":** Thao tác in bếp, không thay đổi món.
* **2.3. "in tạm tính":** Kiểm tra hóa đơn, không ảnh hưởng dữ liệu.
* **2.4. "in lại hóa đơn":** In ấn thuần túy.
"""

#Fill NaN mỗi Loại log trước (để lọc data theo loại log)
Nhatkyorder['Loại log'] = Nhatkyorder['Loại log'].ffill()
Nhatkyorder

Nhatkyorder = Nhatkyorder[Nhatkyorder['Loại log'].isin(['SALE_CHANGE','SALE_SPLIT_ORDER','SALE_MERGE_ORDER'])]
Nhatkyorder

# 1. Đếm số lượng Gộp đơn
so_luong_gop = Nhatkyorder['Loại log'].str.contains('SALE_MERGE_ORDER', na=False, regex=False).sum()

# 2. Đếm số lượng Tách đơn
so_luong_tach = Nhatkyorder['Loại log'].str.contains('SALE_SPLIT_ORDER', na=False, regex=False).sum()

# 3. Đếm số lượng Sửa đơn
so_luong_sua = Nhatkyorder['Loại log'].str.contains('SALE_CHANGE', na=False, regex=False).sum()

# In kết quả tổng hợp
print(f"--- THỐNG KÊ LOG HỆ THỐNG ---")
print(f"1. Số dòng [Gộp đơn]:  {so_luong_gop}")
print(f"2. Số dòng [Tách đơn]: {so_luong_tach}")
print(f"3. Số dòng [Sửa đơn]:  {so_luong_sua}")
print(f"-----------------------------")
print(f"Tổng số log cần xử lý: {so_luong_gop + so_luong_tach + so_luong_sua}")

"""Xử lý tách Data từ Nhatkyorder
Tách dữ liệu Nhatkyorder thành 2 nhóm để xử lý riêng:

1. Nhóm hóa đơn có "Gộp đơn" hoặc "Tách đơn"

Lọc các dòng có Loại log chứa "Gộp đơn" hoặc "Tách đơn"
Lấy danh sách Số hóa đơn tương ứng
Dùng danh sách này để truy ngược lại toàn bộ lịch sử của các hóa đơn đó (bao gồm cả "Sửa đơn", "Thanh toán", ...)
Mục đích: xử lý đầy đủ luồng biến động của các hóa đơn có thay đổi cấu trúc (gộp/tách)

2. Nhóm chỉ có "Sửa đơn"

Lọc các hóa đơn không thuộc nhóm trên
Chỉ giữ các dòng có Loại log = "Sửa đơn"
Đây là nhóm đơn giản, xử lý trực tiếp để ghi nhận order theo nhân viên

## **Nhatkyorder nhóm 1: Lấy những hóa đơn có chứa "Gộp đơn" hoặc "Tách đơn"**
"""

# Bước 1: Lọc các Số hoá đơn có chứa "SALE_SPLIT_ORDER" HOẶC "SALE_MERGE_ORDER"
# Sử dụng regex 'SALE_SPLIT_ORDER|SALE_MERGE_ORDER' để lấy cả hai trường hợp
target_hoadon_list = Nhatkyorder[
    Nhatkyorder['Loại log'].str.contains('SALE_SPLIT_ORDER|SALE_MERGE_ORDER', case=False, na=False)
][['Mã hoá đơn']].drop_duplicates()

# Bước 2: Merge với Nhatkyorder gốc để lấy TOÀN BỘ lịch sử của các hoá đơn này
# (Bao gồm cả các dòng Sửa đơn, Thanh toán... nếu chúng thuộc về Số hoá đơn đó)
Nhatkyorder_gop_tach = Nhatkyorder.merge(
    target_hoadon_list,
    on='Mã hoá đơn',
    how='inner'
)

# Bước 3: Sắp xếp theo Thời gian và Mã hoá đơn để dễ theo dõi luồng xử lý
Nhatkyorder_gop_tach = Nhatkyorder_gop_tach.sort_values(['Mã hoá đơn', 'Thời gian']).reset_index(drop=True)

# Xem kết quả
Nhatkyorder_gop_tach

"""### **Xử lý trong trường hợp có chứa "Gộp đơn"**"""

# # Tạo thêm cột mới lấy 6 ký tự đầu của "Số hoá đơn"
# Nhatkyorder_gop_tach['Mã hóa đơn rút gọn'] = Nhatkyorder_gop_tach['Mã hoá đơn'].astype(str).str[:6]
# Nhatkyorder_gop_tach

# Bước 1: Tạo cột Số hóa đơn mới (mặc định None)
Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'] = None

# Bước 2: Áp dụng cho những dòng có "Gộp đơn" trong Loại log hoặc Ghi chú
# Lưu ý: Bỏ ngoặc vuông để tìm chính xác cụm từ
# Bước 1: Tạo cột
Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'] = None

# Bước 2: Mask (Lọc những dòng có chứa cụm từ quan trọng)
mask = Nhatkyorder_gop_tach['Ghi chú'].str.contains('gộp vào', case=False, na=False)

# Bước 3: Hàm xử lý
def extract_new_invoice(text):
    if pd.isna(text):
        return None
    try:
        # Tách lấy phần sau chữ "gộp vào"
        after_phrase = text.split("gộp vào")[-1].strip()
        # Tách lấy phần trước dấu "-" đầu tiên tính từ đó
        result = after_phrase.split("-")[0].strip()
        return result
    except:
        return None

# Bước 4: Apply
Nhatkyorder_gop_tach.loc[mask, 'Mã hóa đơn sau khi gộp bàn'] = \
    Nhatkyorder_gop_tach.loc[mask, 'Ghi chú'].apply(extract_new_invoice)

# Xem kết quả
Nhatkyorder_gop_tach

# Nếu cột chưa có dữ liệu thì fill tạm = None
Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'] = Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'].replace('', None)

# Fill giá trị trong cùng 1 nhóm "Số hoá đơn" — giá trị đầu tiên (hoặc không rỗng)
Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'] = (
    Nhatkyorder_gop_tach.groupby('Số hoá đơn')['Mã hóa đơn sau khi gộp bàn']
      .transform(lambda x: x.ffill().bfill())
)
Nhatkyorder_gop_tach

# Loại bỏ những dòng loại log "gộp đơn", có chứa "bỏ món", vì đã xong mục đích lấy "Mã hóa đơn sau khi gộp bàn"
# Loại bỏ những dòng loại log "gộp đơn", có chứa "thêm món", vì mục đích là lấy các bạn nhân viên order đầu tiên
Nhatkyorder_gop_tach = Nhatkyorder_gop_tach[~Nhatkyorder_gop_tach['Ghi chú'].str.contains('[Gộp đơn]', na=False, regex=False)]
Nhatkyorder_gop_tach

# 1. Tạo mapping cho Mã hoá đơn (từ Gốc -> Sau khi gộp)
mapping_ma = dict(zip(Nhatkyorder_gop_tach['Mã hoá đơn'], Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn']))

# 2. Tạo mapping cho Số hoá đơn (để từ Mã hoá đơn truy ra Số hoá đơn tương ứng)
# Lấy dòng đầu tiên xuất hiện của mỗi Mã hoá đơn để làm chuẩn Số hoá đơn
mapping_so_hd = Nhatkyorder_gop_tach.drop_duplicates('Mã hoá đơn').set_index('Mã hoá đơn')['Số hoá đơn'].to_dict()

def find_final_ma(code):
    """Truy ngược đến mã hoá đơn cuối cùng."""
    visited = set()
    current_code = code
    while pd.notna(current_code) and current_code in mapping_ma:
        next_code = mapping_ma[current_code]
        # Kiểm tra tránh vòng lặp vô tận hoặc giá trị NaN/giống hệt nhau
        if pd.isna(next_code) or next_code == current_code or next_code in visited:
            break
        visited.add(current_code)
        current_code = next_code
    return current_code

# 3. Chạy truy vết để tìm Mã cuối cùng
Nhatkyorder_gop_tach['Mã hoá đơn cuối cùng'] = Nhatkyorder_gop_tach['Mã hoá đơn'].apply(find_final_ma)

# 4. Từ "Mã hoá đơn cuối cùng", ánh xạ ngược lại để lấy "Số hoá đơn cuối cùng"
Nhatkyorder_gop_tach['Số hoá đơn cuối cùng'] = Nhatkyorder_gop_tach['Mã hoá đơn cuối cùng'].map(mapping_so_hd)

# 5. Cập nhật đè lên cột cũ (nếu muốn) hoặc giữ cột mới để đối chiếu
# Ở đây mình giữ cột mới để bạn dễ kiểm tra, nếu muốn ghi đè thì dùng:
Nhatkyorder_gop_tach['Mã hoá đơn'] = Nhatkyorder_gop_tach['Mã hoá đơn cuối cùng']
Nhatkyorder_gop_tach['Số hoá đơn'] = Nhatkyorder_gop_tach['Số hoá đơn cuối cùng']
Nhatkyorder_gop_tach = Nhatkyorder_gop_tach.drop(columns=['Mã hóa đơn sau khi gộp bàn', 'Mã hoá đơn cuối cùng', 'Số hoá đơn cuối cùng'])
Nhatkyorder_gop_tach

Nhatkyorder_gop_tach = Nhatkyorder_gop_tach.sort_values(['Số hoá đơn', 'Thời gian']).reset_index(drop=True)

"""### **Xử lý trong trường hợp có chứa "Tách đơn"**"""

# --- PHẦN 1: PHÂN LOẠI LOG THEO LOẠI THAO TÁC ---

# 1. Lấy danh sách Số hóa đơn có ít nhất một thao tác "SALE_SPLIT_ORDER"
# (Dùng để tham chiếu nếu cần, hoặc có thể bỏ qua nếu không dùng đến)

# ds_hoadon_tach = Nhatkyorder_gop_tach[
#     Nhatkyorder_gop_tach['Loại log'].str.contains('SALE_SPLIT_ORDER', case=False, na=False)
# ]['Số hóa đơn'].unique()

# 2. Lọc riêng các dòng dữ liệu có nội dung là "SALE_SPLIT_ORDER"
Nhatkyorder_tachdon = Nhatkyorder_gop_tach[
    Nhatkyorder_gop_tach['Loại log'].str.contains('SALE_SPLIT_ORDER', case=False, na=False)
]

# 3. Lấy tất cả các dòng còn lại (Các thao tác không phải SALE_SPLIT_ORDER, ví dụ: Sửa đơn)
Nhatkyorder_con_lai = Nhatkyorder_gop_tach[
    ~Nhatkyorder_gop_tach['Loại log'].str.contains('SALE_SPLIT_ORDER', case=False, na=False)
]

# --- KIỂM TRA KẾT QUẢ ---
print(f"Tổng cộng dòng log ban đầu: {len(Nhatkyorder_gop_tach)}")
print(f"Số dòng thao tác SALE_SPLIT_ORDER:  {len(Nhatkyorder_tachdon)}")
print(f"Số dòng thao tác còn lại:   {len(Nhatkyorder_con_lai)}")

Nhatkyorder_tachdon = Nhatkyorder_gop_tach
# Bước 1: Tạo cột mới
Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'] = None

# Bước 2: Xác định mask lọc
mask = (Nhatkyorder_tachdon['Ghi chú'].str.contains('bỏ món', case=False, na=False)) & \
       (Nhatkyorder_tachdon['Ghi chú'].str.contains('tạo thành hóa đơn', case=False, na=False))

# Bước 3: Hàm xử lý lấy FULL mã đứng trước dấu "-"
def extract_full_invoice(text):
    if pd.isna(text):
        return None
    # Tìm cụm chữ/số (không bao gồm khoảng trắng) đứng trước dấu gạch ngang
    match = re.search(r'([A-Z0-9]{10,})\s*-', text) # Thêm {10,} để đảm bảo là mã dài
    if match:
        return match.group(1).strip()
    return None

# Bước 4: Áp dụng
Nhatkyorder_tachdon.loc[mask, 'Mã hóa đơn sau khi tách bàn'] = Nhatkyorder_tachdon.loc[mask, 'Ghi chú'].apply(extract_full_invoice)
Nhatkyorder_tachdon

# Bước 1: Thay thế chuỗi rỗng bằng None để dễ xử lý fill
Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'] = Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].replace('', None)

# Bước 2: Chỉ thực hiện Fill giá trị cho những dòng là "Tách đơn"
# Chúng ta tạo một bản tạm chứa các dòng Tách đơn để fill mã, sau đó gán ngược lại
mask_tach = Nhatkyorder_tachdon['Loại log'] == 'SALE_SPLIT_ORDER'

# Fill mã trong phạm vi từng Số hoá đơn nhưng chỉ áp dụng kết quả lên các dòng Tách đơn
Nhatkyorder_tachdon.loc[mask_tach, 'Mã hóa đơn sau khi tách bàn'] = (
    Nhatkyorder_tachdon.groupby('Mã hoá đơn')['Mã hóa đơn sau khi tách bàn']
    .transform(lambda x: x.ffill().bfill())
)

# Bước 3: Đảm bảo các dòng KHÔNG PHẢI "Tách đơn" thì mã phải là None
Nhatkyorder_tachdon.loc[~mask_tach, 'Mã hóa đơn sau khi tách bàn'] = None
Nhatkyorder_tachdon

# --- BƯỚC 1: TẠO BẢN ĐỒ QUAN HỆ (CHA -> CON) ---
# Lấy các cặp quan hệ từ dữ liệu bạn đã xử lý
# 'Mã hoá đơn rút gọn' lúc này là CHA, 'Mã hóa đơn sau khi tách bàn' là CON
mapping_df = Nhatkyorder_tachdon[Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].notna()][['Mã hoá đơn', 'Mã hóa đơn sau khi tách bàn']].drop_duplicates()

# Chuyển thành Dictionary để tra cứu ngược: {Con: Cha}
# Để khi đứng ở hóa đơn con, mình biết cha nó là ai
child_to_parent_map = dict(zip(mapping_df['Mã hóa đơn sau khi tách bàn'], mapping_df['Mã hoá đơn']))

# --- BƯỚC 2: HÀM ĐỆ QUY TÌM GỐC (ROOT) ---
def find_ultimate_root(invoice_code, p_map):
    current = invoice_code
    # Vòng lặp leo ngược cây gia phả cho đến khi không tìm thấy "cha" nữa
    while current in p_map:
        parent = p_map[current]
        # Tránh trường hợp bị lặp vô tận nếu dữ liệu lỗi (A là cha B, B là cha A)
        if parent == current:
            break
        current = parent
    return current

# --- BƯỚC 3: ÁP DỤNG VÀO DATAFRAME ---
# Tạo cột Group_ID_Goc cho toàn bộ dữ liệu
Nhatkyorder_tachdon['Group_ID_Goc'] = Nhatkyorder_tachdon['Mã hoá đơn'].apply(lambda x: find_ultimate_root(x, child_to_parent_map))

# --- BƯỚC 4: SẮP XẾP LẠI THEO NHÓM VÀ THỜI GIAN ---
# Sắp xếp theo Group_ID_Goc để các hóa đơn liên quan nằm cạnh nhau
# Sắp xếp Thời gian từ cũ đến mới (ascending=True) để thấy luồng tách đơn
Nhatkyorder_tachdon = Nhatkyorder_tachdon.sort_values(
    by=['Thời gian','Group_ID_Goc'],
    ascending=[True, True]
).reset_index(drop=True)

# Xem kết quả
Nhatkyorder_tachdon

# --- BƯỚC 1: TẠO BẢN ĐỒ QUAN HỆ & TRA CỨU SỐ HĐ ---
# Tạo map để tìm cha: {Con: Cha}
mapping_df = Nhatkyorder_tachdon[Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].notna()][['Mã hoá đơn', 'Mã hóa đơn sau khi tách bàn']].drop_duplicates()
child_to_parent_map = dict(zip(mapping_df['Mã hóa đơn sau khi tách bàn'], mapping_df['Mã hoá đơn']))

# Tạo map để tra cứu Số hoá đơn từ Mã hoá đơn: {Mã: Số HĐ}
# Cái này dùng để hiển thị "Số hoá đơn tách bàn" cho dễ đọc
invoice_to_no_map = dict(zip(Nhatkyorder_tachdon['Mã hoá đơn'], Nhatkyorder_tachdon['Số hoá đơn']))

# --- BƯỚC 2: HÀM ĐỆ QUY TÌM GỐC (ROOT) ---
def find_ultimate_root(invoice_code, p_map):
    visited = set()
    current = invoice_code
    while current in p_map:
        if current in visited: break
        visited.add(current)
        parent = p_map[current]
        if pd.isna(parent) or parent == current: break
        current = parent
    return current

# --- BƯỚC 3: ÁP DỤNG VÀO DATAFRAME ---
# 1. Tìm Group Gốc
Nhatkyorder_tachdon['Group_ID_Goc'] = Nhatkyorder_tachdon['Mã hoá đơn'].apply(lambda x: find_ultimate_root(x, child_to_parent_map))

# 2. Lấy Số hoá đơn của Group Gốc (để biết toàn bộ mớ này bắt nguồn từ Số HĐ nào)
Nhatkyorder_tachdon['Số HĐ Gốc'] = Nhatkyorder_tachdon['Group_ID_Goc'].map(invoice_to_no_map)

# 3. Lấy Số hoá đơn mục tiêu (Số hoá đơn sau khi tách bàn)
# Thay vì dùng hàm apply phức tạp, bạn dùng .map() trực tiếp sẽ nhanh hơn nhiều
Nhatkyorder_tachdon['Số hoá đơn tách bàn'] = Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].map(invoice_to_no_map)

# --- BƯỚC 4: SẮP XẾP ---
Nhatkyorder_tachdon = Nhatkyorder_tachdon.sort_values(
    by=['Group_ID_Goc', 'Thời gian'],
    ascending=[True, True]
).reset_index(drop=True)
Nhatkyorder_tachdon

# 1. Loại bỏ các dòng thông báo hệ thống không cần thiết
# Những dòng này chỉ mang tính chất thông báo, không có món hàng nên drop để báo cáo sạch
Nhatkyorder_tachdon = Nhatkyorder_tachdon[
    ~Nhatkyorder_tachdon['Ghi chú'].str.contains(r'hóa đơn được tạo mới', case=False, na=False)
].copy()

# 2. Tạo cột phụ để sắp xếp
# Dùng .fillna('') để tránh lỗi khi gặp dòng không phải là đơn tách (giá trị None)
# Việc sort theo 4 ký tự cuối giúp các đơn tách (VD: ...4428, ...4429) nằm đúng thứ tự
Nhatkyorder_tachdon['Ma_Sort_Phu'] = (
    Nhatkyorder_tachdon['Số hoá đơn tách bàn']
    .fillna('')
    .astype(str)
    .str[-4:]
)

# 3. Sắp xếp lại
# Ưu tiên Group_ID_Goc để gom gia đình hóa đơn,
# sau đó tới Thời gian và mã phụ để thấy luồng tách món
Nhatkyorder_tachdon = Nhatkyorder_tachdon.sort_values(
    by=['Group_ID_Goc', 'Thời gian', 'Ma_Sort_Phu'],
    ascending=[True, True, True]
).reset_index(drop=True)

# 4. Xóa cột phụ
Nhatkyorder_tachdon = Nhatkyorder_tachdon.drop(columns=['Ma_Sort_Phu'])

# Hiển thị kết quả kiểm tra
Nhatkyorder_tachdon

# Loại bỏ dòng mà (Mã hóa đơn ko NaN) VÀ (Số hóa đơn là NaN)
Nhatkyorder_tachdon = Nhatkyorder_tachdon[
    ~(Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].notna() &
      Nhatkyorder_tachdon['Số hoá đơn tách bàn'].isna())
]
Nhatkyorder_tachdon

import pandas as pd
import numpy as np

# --- BƯỚC 0: CHUẨN BỊ MAPPING ---
# Map này dùng để tra cứu Số HĐ từ Mã HĐ (đối với các mã đã tồn tại trong hệ thống)
invoice_to_no_map = dict(zip(Nhatkyorder_tachdon['Mã hoá đơn'], Nhatkyorder_tachdon['Số hoá đơn']))

# 1. Chuẩn bị dữ liệu sạch
Nhatkyorder_tachdon['Số lượng order'] = pd.to_numeric(Nhatkyorder_tachdon['Số lượng order'], errors='coerce').fillna(0)
Nhatkyorder_tachdon['Món'] = Nhatkyorder_tachdon['Món'].astype(str).str.strip()
Nhatkyorder_tachdon = Nhatkyorder_tachdon.replace({pd.NA: None, np.nan: None, "": None})

final_rows = []

# --- BƯỚC 1: XỬ LÝ THEO TỪNG NHÓM GIA ĐÌNH HÓA ĐƠN ---
for group_id, df_group in Nhatkyorder_tachdon.groupby('Group_ID_Goc'):

    current_inventory = []
    df_sorted = df_group.sort_values('Thời gian')
    processed_indices = set()

    for idx, row in df_sorted.iterrows():
        if idx in processed_indices:
            continue

        # A. NẾU LÀ MÓN THÊM VÀO (SALE_CHANGE / Sửa đơn)
        if row['Loại log'] in ['SALE_CHANGE', 'Sửa đơn', 'SALE_ORDER']:
            if row['Số lượng order'] > 0:
                current_inventory.append({
                    'data': row.to_dict(),
                    'Món': row['Món'],
                    'Số lượng': row['Số lượng order']
                })
            processed_indices.add(idx)

        # B. NẾU LÀ TÁCH ĐƠN (SALE_SPLIT_ORDER)
        elif 'SPLIT' in str(row['Loại log']) and row['Mã hóa đơn sau khi tách bàn']:
            ma_con_moi = row['Mã hóa đơn sau khi tách bàn']
            so_hd_con_moi = row['Số hoá đơn tách bàn'] # LẤY GIÁ TRỊ TỪ CỘT BẠN ĐÃ XỬ LÝ
            thoi_gian_tach = row['Thời gian']

            # Lấy danh sách các món "Ở LẠI" bàn cũ (số lượng âm trong log)
            df_staying_block = df_sorted[
                (df_sorted['Thời gian'] == thoi_gian_tach) &
                (df_sorted['Mã hóa đơn sau khi tách bàn'] == ma_con_moi)
            ]

            processed_indices.update(df_staying_block.index.tolist())
            staying_dict = df_staying_block.groupby('Món')['Số lượng order'].sum().abs().to_dict()

            new_inventory_for_root = []

            for item in current_inventory:
                ten_mon = item['Món']
                sl_trong_kho = item['Số lượng']
                sl_muon_o_lai = staying_dict.get(ten_mon, 0)

                # Món KHÔNG nằm trong danh sách ở lại -> TÁCH ĐI
                sl_tach_di = sl_trong_kho - sl_muon_o_lai

                if sl_tach_di > 0:
                    new_split_row = pd.Series(item['data'])
                    # GÁN MÃ VÀ SỐ HÓA ĐƠN THEO ĐÚNG THÔNG TIN TÁCH BÀN
                    new_split_row['Mã hoá đơn'] = ma_con_moi
                    new_split_row['Số hoá đơn'] = so_hd_con_moi
                    new_split_row['Số lượng order'] = sl_tach_di

                    # Làm sạch các cột kỹ thuật
                    new_split_row['Mã hóa đơn sau khi tách bàn'] = None
                    new_split_row['Số hoá đơn tách bàn'] = None
                    final_rows.append(new_split_row)

                # Món nằm trong danh sách ở lại -> GIỮ LẠI ĐƠN GỐC
                if sl_muon_o_lai > 0:
                    sl_thuc_te_o_lai = min(sl_trong_kho, sl_muon_o_lai)
                    item['Số lượng'] = sl_thuc_te_o_lai
                    new_inventory_for_root.append(item)
                    staying_dict[ten_mon] -= sl_thuc_te_o_lai

            current_inventory = new_inventory_for_root

    # --- BƯỚC 2: CHỐT SỔ CHO ĐƠN GỐC ---
    for item in current_inventory:
        if item['Số lượng'] > 0:
            final_root_row = pd.Series(item['data'])
            final_root_row['Số lượng order'] = item['Số lượng']
            final_root_row['Mã hóa đơn sau khi tách bàn'] = None
            final_root_row['Số hoá đơn tách bàn'] = None
            final_rows.append(final_root_row)

# 3. Kết quả cuối cùng
Nhatkyorder_final = pd.DataFrame(final_rows).reset_index(drop=True)
Nhatkyorder_final

cols_to_drop = ['Mã hóa đơn sau khi tách bàn', 'Group_ID_Goc', 'Số HĐ Gốc','Số hoá đơn tách bàn']
Nhatkyorder_final = Nhatkyorder_final.drop(columns=cols_to_drop)

# 3. Reset index để bảng nhìn gọn gàng hơn
Nhatkyorder_final = Nhatkyorder_final.reset_index(drop=True)

# Hiển thị kết quả kiểm tra
Nhatkyorder_final

Nhatkyorder_nhom_1 = pd.concat([Nhatkyorder_con_lai, Nhatkyorder_final], ignore_index=True)
Nhatkyorder_nhom_1

"""## **Nhatkyorder nhóm 2: Lấy những hóa đơn chỉ chứa "Sửa đơn"**

### **Xử lý trong trường hợp chỉ có "Sửa đơn"**
"""

# B1: Định nghĩa loại log chấp nhận (Chỉ lấy "Sửa đơn")
pattern_accept = 'SALE_CHANGE'

# B2: Đánh dấu các dòng thỏa mãn điều kiện "Sửa đơn"
mask_hop_le = Nhatkyorder['Loại log'].str.contains(pattern_accept, case=False, na=False)

# B3: Tìm danh sách các Số hóa đơn có chứa bất kỳ hành động nào khác "Sửa đơn"
# (Ví dụ: Gộp đơn, Tách đơn, In tạm tính,...) -> Những hóa đơn này sẽ bị loại khỏi nhóm này
hoa_don_co_hanh_dong_khac = Nhatkyorder.loc[~mask_hop_le, 'Số hoá đơn'].unique()

# B4: Lọc lấy các hóa đơn mà TOÀN BỘ lịch sử của nó chỉ có "Sửa đơn"
# Không nằm trong danh sách có hành động lạ ở trên
Nhatkyorder_nhom_2 = (Nhatkyorder[~Nhatkyorder['Mã hoá đơn'].isin(hoa_don_co_hanh_dong_khac)]
    .sort_values(['Mã hoá đơn', 'Thời gian'])
    .reset_index(drop=True))

# Xem kết quả
Nhatkyorder_nhom_2

"""## **Gộp Nhatkyorder nhóm 1 và nhóm 2 thành full data Nhatkyorder**"""

Nhatkyorder = pd.concat([Nhatkyorder_nhom_1, Nhatkyorder_nhom_2], ignore_index=True)
# Sắp xếp theo Số hóa đơn và Thời gian (giả sử cột Thời gian có dạng datetime)
Nhatkyorder = Nhatkyorder.sort_values(by=['Số hoá đơn', 'Thời gian'], ascending=True)
Nhatkyorder

"""## **Biến đổi dữ liệu Nhatkyorder để merge cho đúng**"""

# 1) Chuyển đổi thông minh: tự động nhận diện cả số lẫn chuỗi ngày tháng
# errors='coerce' để nếu có lỗi thì trả về NaT (không gây crash code)
if Nhatkyorder['Thời gian'].dtype == 'object':
    # Nếu là chuỗi ngày tháng: bỏ unit='ms'
    Nhatkyorder['Thời gian'] = pd.to_datetime(Nhatkyorder['Thời gian'], errors='coerce')
else:
    # Nếu là dạng số (timestamp): giữ unit='ms'
    Nhatkyorder['Thời gian'] = pd.to_datetime(Nhatkyorder['Thời gian'], unit='ms', errors='coerce')

# 2) Sau khi đã là định dạng datetime, các hàm .dt sẽ chạy bình thường
Nhatkyorder['Ngày'] = Nhatkyorder['Thời gian'].dt.strftime('%d/%m/%Y')
Nhatkyorder['Năm'] = Nhatkyorder['Thời gian'].dt.year
Nhatkyorder['Tháng'] = Nhatkyorder['Thời gian'].dt.strftime('%m-%Y')

# 3) Tính tuần
iso = Nhatkyorder['Thời gian'].dt.isocalendar()
Nhatkyorder['Tuần'] = iso.week.astype(str).str.zfill(2) + '-' + iso.year.astype(str)

# Kiểm tra lại
Nhatkyorder

# Giả sử Nhatkyorder là dữ liệu của bạn
# BƯỚC QUAN TRỌNG: Đảm bảo cột số lượng là kiểu số
Nhatkyorder['Số lượng order'] = pd.to_numeric(Nhatkyorder['Số lượng order'], errors='coerce').fillna(0)

# Tách riêng các dòng dương và âm
pos_df = Nhatkyorder[Nhatkyorder['Số lượng order'] > 0].copy()
neg_df = Nhatkyorder[Nhatkyorder['Số lượng order'] < 0].copy()

# Logic khấu trừ FIFO ngược
for _, neg_row in neg_df.iterrows():
    amount_to_deduct = abs(neg_row['Số lượng order'])
    bill_id = neg_row['Mã hoá đơn']
    item_name = neg_row['Món']

    mask = (pos_df['Số hoá đơn'] == bill_id) & (pos_df['Món'] == item_name)
    potential_indices = pos_df[mask].index.tolist()

    # Duyệt ngược từ dòng gần nhất lên trên
    for idx in reversed(potential_indices):
        if amount_to_deduct <= 0:
            break

        current_val = pos_df.at[idx, 'Số lượng order']

        if current_val <= amount_to_deduct:
            amount_to_deduct -= current_val
            pos_df.at[idx, 'Số lượng order'] = 0
        else:
            pos_df.at[idx, 'Số lượng order'] = current_val - amount_to_deduct
            amount_to_deduct = 0

# Lọc bỏ các dòng đã bị trừ hết
Nhatkyorder = pos_df[pos_df['Số lượng order'] > 0]
# Nhatkyorder.to_excel('Nhatkyorder_kiemtrasoluongorder_lucsau.xlsx', index=False)

# Bây giờ dòng này của bạn sẽ chạy mượt mà:
total_order = Nhatkyorder['Số lượng order'].sum()
print(f"Tổng số lượng order là: {total_order}")

# 1. Đảm bảo cột Thời gian đúng định dạng để sắp xếp chính xác
Nhatkyorder['Thời gian'] = pd.to_datetime(Nhatkyorder['Thời gian'])

# 2. Sắp xếp dữ liệu theo Hóa đơn, Món và Thời gian (để đảm bảo dòng cũ đứng trước)
Nhatkyorder = Nhatkyorder.sort_values(by=['Số hoá đơn', 'Món', 'Thời gian'])

# 3. Tạo cột 'Mark' đánh dấu thứ tự xuất hiện của món trong từng hóa đơn
# groupby(['Số hoá đơn', 'Món']) giúp gom nhóm các món giống nhau trong cùng 1 bill
# cumcount() sẽ đánh số 0, 1, 2... cho từng nhóm đó
Nhatkyorder['Mark'] = Nhatkyorder.groupby(['Mã hoá đơn', 'Món','Số lượng order']).cumcount() + 1

# Hiển thị kết quả kiểm tra
Nhatkyorder

Hoadontheothoigian

"""# **Tính KPI nhân viên**

## **Merge 2 bảng filtered_df & Nhatkyorder1**
"""

# 1. Ép kiểu cột 'Số lượng' ở bảng bên phải về kiểu số (giống bảng bên trái)
Hoadontheothoigian['Số lượng'] = pd.to_numeric(Hoadontheothoigian['Số lượng'], errors='coerce').fillna(0).astype(int)

# 2. Đảm bảo cột Mark ở cả 2 bên cũng là kiểu số (để chắc chắn)
Nhatkyorder['Mark'] = pd.to_numeric(Nhatkyorder['Mark'], errors='coerce').fillna(0).astype(int)
Hoadontheothoigian['Mark'] = pd.to_numeric(Hoadontheothoigian['Mark'], errors='coerce').fillna(0).astype(int)

# 3. Thực hiện Merge
Databanhang_1 = pd.merge(
    Nhatkyorder[['Mã hoá đơn','Thời gian','Món','Mark','Nhân viên','Loại log','Số lượng order','Ngày','Tuần','Tháng','Năm']],
    Hoadontheothoigian[['Cửa hàng','Tên hàng','Số lượng','Mark','Đơn giá','Mã hoá đơn','Số hoá đơn','Bàn','Số khách','Loại khách hàng','SĐT']],
    left_on = ['Mã hoá đơn','Món','Số lượng order','Mark'],
    right_on = ['Mã hoá đơn','Tên hàng','Số lượng','Mark'], #Vì ráng chiều cột Số hoá đơn ko có nên chỉ có 4 điều kiện
    how = 'right'
)

Databanhang_1

#Tách những dòng Món NaN
Databanhang_2 = Databanhang_1[Databanhang_1['Món'].isna()]
Databanhang_2

Databanhang_3 = Databanhang_1[~Databanhang_1['Món'].isna()]
# 1. Thực hiện Left Merge giữa bảng gốc Nhatkyorder và bảng đã khớp thành công
# Chúng ta dùng indicator=True để Pandas đánh dấu dòng nào có ở cả 2 bảng, dòng nào chỉ có ở bảng trái
check_missing = pd.merge(
    Nhatkyorder,
    Databanhang_3[['Số hoá đơn', 'Món', 'Số lượng order', 'Mark']],
    on=['Số hoá đơn', 'Món', 'Số lượng order', 'Mark'],
    how='left',
    indicator=True
)

# 2. Lọc những dòng chỉ xuất hiện ở bảng trái (Nhatkyorder) mà không có ở bảng đã merge
# Những dòng này có giá trị ở cột '_merge' là 'left_only'
Nhatkyorder_chua_thoa_dk = check_missing[check_missing['_merge'] == 'left_only']

# 3. Loại bỏ cột công cụ '_merge' để bảng sạch sẽ
Nhatkyorder_chua_thoa_dk = Nhatkyorder_chua_thoa_dk.drop(columns=['_merge'])

# Hiển thị kết quả
Nhatkyorder_chua_thoa_dk

Databanhang_2 = Databanhang_2.drop(columns=['Thời gian','Món','Nhân viên','Loại log','Số lượng order','Ngày','Tuần','Tháng','Năm'])
Databanhang_2
# Bước 1: Làm sạch bảng trái, đảm bảo mỗi Mã hóa đơn + Món chỉ xuất hiện 1 lần
# để tránh làm tăng số dòng của bảng phải khi merge
nhatky_clean = Nhatkyorder_chua_thoa_dk[['Mã hoá đơn','Món','Thời gian','Mark','Nhân viên','Loại log','Số lượng order','Ngày','Tuần','Tháng','Năm']].drop_duplicates(subset=['Mã hoá đơn', 'Món'])

# Bước 2: Thực hiện merge
Databanhang_4 = pd.merge(
    nhatky_clean,
    Databanhang_2[['Cửa hàng','Tên hàng','Số lượng','Mark','Đơn giá','Mã hoá đơn','Số hoá đơn','Bàn','Số khách','Loại khách hàng','SĐT']],
    left_on = ['Mã hoá đơn','Món'],
    right_on = ['Mã hoá đơn','Tên hàng'],
    how = 'right' # Giữ nguyên 5 dòng của bảng phải
)

# Bước 3: Loại bỏ các dòng rác (nếu có)
Databanhang_4 = Databanhang_4[Databanhang_4['Cửa hàng'].notna()]

Databanhang_4

total_order = Databanhang_4['Số lượng order'].sum()

# 2. In kết quả
print(f"--- THỐNG KÊ TỔNG CỘNG ---")
print(f"Tổng số lượng order toàn bộ là: {total_order}")
print(f"--------------------------")

# Hiển thị các dòng bị lỗi NaN để kiểm tra
kiemtraNan_1 = Databanhang_4[Databanhang_4['Món'].isna()]
kiemtraNan_1

# 2. Nhóm theo 'Cửa hàng', đếm số lượng 'Số hoá đơn' duy nhất
thong_ke_loi = kiemtraNan_1.groupby('Cửa hàng')['Số hoá đơn'].nunique().reset_index()

# 3. Đặt lại tên cột cho dễ hiểu
thong_ke_loi.columns = ['Cửa hàng', 'Số lượng hóa đơn lỗi']

print(thong_ke_loi)

Databanhang_4 = Databanhang_4[~Databanhang_4['Món'].isna()]
Databanhang_4 = Databanhang_4.drop(columns=['Mark_x','Mark_y'])
Databanhang_3  = Databanhang_3 .drop(columns=['Mark'])
Databanhang = pd.concat([Databanhang_4, Databanhang_3], ignore_index=True)
Databanhang

# Hiển thị các dòng bị lỗi NaN để kiểm tra
kiemtraNan_2 = Databanhang[Databanhang['Món'].isna()]
kiemtraNan_2

# Tính tổng để so sánh với hệ thống => check
# Ép kiểu cột "Số lượng order" về dạng số
Databanhang['Số lượng order'] = pd.to_numeric(Databanhang['Số lượng order'], errors='coerce')

# Danh sách các món cần tính tổng
cac_mon = ['Trà Kombucha nhiệt đới', 'Trà mãng cầu', 'Trà nhiệt đới','Trà Ổi Hoa Hồng','Trà dâu tây','Trà Kombucha mãng cầu']

for mon in cac_mon:
  tong = Databanhang.loc[
      Databanhang['Món'].str.strip() == mon.strip(),
      'Số lượng order'
  ].sum()
  print(f"Tổng {mon}: {tong}")

unique_values = Databanhang['Món'].unique()
print(unique_values)

# Thứ tự cột mới mong muốn (bạn có thể thay đổi theo ý muốn)
new_order = ['Ngày', 'Tuần', 'Tháng', 'Năm','Cửa hàng','Mã hoá đơn','Số hoá đơn','Loại khách hàng', 'Món', 'Đơn giá','Nhân viên', 'Bàn','Loại log','Số lượng order','Số khách', 'Thời gian','SĐT']

# Sắp xếp lại cột theo thứ tự mới
Databanhang = Databanhang.reindex(columns=new_order)
Databanhang = Databanhang.sort_values(by=['Thời gian'], ascending=True)
Databanhang['Số lượng order'] = pd.to_numeric(Databanhang['Số lượng order'], errors='coerce').astype(int)
Databanhang

# Tính tổng để so sánh với hệ thống => check
# Ép kiểu cột "Số lượng order" về dạng số
Databanhang['Số lượng order'] = pd.to_numeric(Databanhang['Số lượng order'], errors='coerce')

# Danh sách các món cần tính tổng
cac_mon = ['Trà Kombucha nhiệt đới', 'Trà mãng cầu', 'Trà nhiệt đới','Trà Ổi Hoa Hồng','Trà dâu tây','Trà Kombucha mãng cầu']

for mon in cac_mon:
  tong = Databanhang.loc[
      Databanhang['Món'].str.strip() == mon.strip(),
      'Số lượng order'
  ].sum()
  print(f"Tổng {mon}: {tong}")

# 1. Lọc các dòng mà tên món có chứa dấu "+"
# na=False để bỏ qua các dòng bị rỗng nếu có
topping_df = Databanhang[Databanhang['Món'].str.contains(r'\+', na=False)]

# 2. Lấy danh sách unique và chuyển về DataFrame cho dễ nhìn/xuất file
unique_toppings = pd.DataFrame(topping_df['Món'].unique(), columns=['Tên món Topping'])

# 3. Sắp xếp theo bảng chữ cái để bạn dễ tìm kiếm
unique_toppings = unique_toppings.sort_values(by='Tên món Topping').reset_index(drop=True)

# Hiển thị kết quả
print(unique_toppings)

# 1. Định nghĩa bảng giá (giữ nguyên như cũ)
bang_gia_topping = {
    '+ Topping Hạt đác': 15000,
    '+ Topping Trân Châu Ô Long': 10000,
    '+ Topping Trân Châu Trắng': 10000,
    '+ Không lấy Topping': 0
}

# 2. Sửa trực tiếp trên cột Đơn giá
for ten_topping, gia_moi in bang_gia_topping.items():
    # Kiểm tra nếu tên món có CHỨA cụm từ topping đó (bất kể dấu cách thừa phía trước)
    # flag re.ESCAPE để xử lý dấu "+" an toàn
    mask = Databanhang['Món'].str.contains(ten_topping.replace('+', r'\+'), na=False)
    Databanhang.loc[mask, 'Đơn giá'] = gia_moi

# 3. Tính lại cột Giá trị order ngay lập tức
Databanhang['Giá trị order'] = Databanhang['Số lượng order'] * Databanhang['Đơn giá']

BASE_DIR = r"D:\Làm việc - Bảo Châu\Project\Scraping_data_ban_Pos"

PATH_LUU_DATA_BAN_HANG = os.path.join(BASE_DIR, "Data_ban_hang")
  
    
os.makedirs(PATH_LUU_DATA_BAN_HANG, exist_ok=True)


# Tạo đường dẫn file ĐỘNG theo ngày f_str
full_path_log = os.path.join(PATH_LUU_DATA_BAN_HANG, f"Data_Ban_Hang_{f_str}_{t_str}.xlsx")
Databanhang.to_excel(full_path_log, index=False)

# Databanhang.to_excel("1.xlsx",index=False)
# Hiển thị kiểm tra 
print(Databanhang.head(50))
print(Databanhang.shape)
"""## **Tính các KPI liên quan**"""

# Giả sử dataframe của bạn tên là Databanhang

# Gom nhóm theo các lớp
group_by_ban_hang= (
    Databanhang.groupby(
        ['Ngày','Tuần','Tháng','Năm', 'Cửa hàng', 'Số hoá đơn', 'Số khách', 'Loại khách hàng', 'Nhân viên','SĐT'],
        as_index=False
    )['Giá trị order']
    .sum()
)

# Sắp xếp để dễ nhìn (tuỳ chọn)
group_by_ban_hang = group_by_ban_hang.sort_values(['Ngày', 'Số hoá đơn', 'Nhân viên']).reset_index(drop=True)

group_by_ban_hang