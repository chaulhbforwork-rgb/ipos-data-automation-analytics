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

# --- 2. Extract info from extra_data column ---
# Note: sale_by_date_final is the table where toppings were already exploded
def extract_extra_info(extra_str):
    # Initialize default results
    results = {
        'peo_count': 0,
        'Membership_Type_Name': '',
        'customer_name': '',
        'customer_phone': ''
    }
    try:
        # Safely convert extra_data string to dict
        data = ast.literal_eval(str(extra_str))
        if isinstance(data, dict):
            results['peo_count'] = data.get('peo_count', 0)
            results['Membership_Type_Name'] = data.get('Membership_Type_Name', '')
            results['customer_name'] = data.get('customer_name', '')
            results['customer_phone'] = data.get('customer_phone', '')
    except:
        pass
    return pd.Series(results)

# Apply extraction to the main DataFrame
extra_info = sale_by_date_final['extra_data'].apply(extract_extra_info)

# --- 3. Join the 4 new columns to the main table ---
sale_by_date_final[['peo_count', 'Membership_Type_Name', 'customer_name', 'customer_phone']] = extra_info

# --- VERIFY RESULTS ---
print(sale_by_date_final[['item_name', 'peo_count', 'Membership_Type_Name', 'customer_name', 'customer_phone']].head())

# --- STEP 1: SELECT RELEVANT COLUMNS ---
cols_to_keep = [
    'store_name', 'tran_id', 'origin_tran_id', 'tran_no', 'tran_date',
    'start_hour', 'start_minute', 'end_hour', 'end_minute',
    'table_name', 'item_name', 'quantity', 'unit_id', 'price_org', 'amount',
    'peo_count', 'Membership_Type_Name', 'customer_name', 'customer_phone',
    'total_amount', 'amount_discount_detail'
]

# Filter existing columns to prevent errors if any are missing
existing_cols = [c for c in cols_to_keep if c in sale_by_date_final.columns]
Hoadontheothoigian = sale_by_date_final[existing_cols].copy()

# --- STEP 2: DEFINE MAPPING (KEEPING VIETNAMESE NAMES) ---
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

# --- STEP 3: RENAME COLUMNS ---
Hoadontheothoigian = Hoadontheothoigian.rename(columns=mapping_cols)

# --- STEP 4: TIME PROCESSING ---
if 'Ngày' in Hoadontheothoigian.columns:
    # Convert milliseconds to datetime
    Hoadontheothoigian['Ngày'] = pd.to_datetime(Hoadontheothoigian['Ngày'], unit='ms')

    # Localize to UTC and convert to Vietnam Timezone (ICT)
    Hoadontheothoigian['Ngày'] = (Hoadontheothoigian['Ngày']
                                  .dt.tz_localize('UTC')
                                  .dt.tz_convert('Asia/Ho_Chi_Minh')
                                  .dt.strftime('%Y/%m/%d'))

    # Specific rename as requested
    Hoadontheothoigian = Hoadontheothoigian.rename(columns={'Tên khách': 'Tên'})

# Handle 'Mã hoá đơn gốc' logic: replace current ID with original ID if available
Hoadontheothoigian['Mã hoá đơn'] = Hoadontheothoigian['Mã hoá đơn gốc'].fillna(Hoadontheothoigian['Mã hoá đơn'])

# Drop 'Mã hoá đơn gốc' for a cleaner table
Hoadontheothoigian = Hoadontheothoigian.drop(columns=['Mã hoá đơn gốc'])

# --- TIME FORMATTING (HH:MM:SS) ---
def format_time(h, m):
    try:
        # Convert to int to remove .0 decimals, then pad to 2 digits
        hh = str(int(float(h))).zfill(2)
        mm = str(int(float(m))).zfill(2)
        return f"{hh}:{mm}:00"
    except:
        return "00:00:00"

# Merge Hour and Minute columns into unified string format
Hoadontheothoigian['Giờ vào'] = Hoadontheothoigian.apply(
    lambda x: format_time(x['Giờ vào'], x['Phút vào']), axis=1
)

Hoadontheothoigian['Giờ ra'] = Hoadontheothoigian.apply(
    lambda x: format_time(x['Giờ ra'], x['Phút ra']), axis=1
)

# Remove the redundant minute columns
Hoadontheothoigian = Hoadontheothoigian.drop(columns=['Phút vào', 'Phút ra'])

# --- CREATE CUSTOMER TYPE COLUMN ---
def is_empty(col):
    return Hoadontheothoigian[col].isna() | (Hoadontheothoigian[col].astype(str).str.strip() == "")

# Define logic for Retail vs New vs Returning customers
condlist = [
    # Case: Retail Guest
    (Hoadontheothoigian['Tên'] == 'iPOS-O2O') | is_empty('Tên') | is_empty('SĐT'),

    # Case: New Guest
    (~is_empty('Tên') & (Hoadontheothoigian['Tên'] != 'iPOS-O2O')) &
    (~is_empty('SĐT')) &
    (is_empty('Loại thành viên') | (Hoadontheothoigian['Loại thành viên'] == 'Thành viên mặc định'))
]

Hoadontheothoigian['Loại khách hàng'] = np.select(
    condlist=condlist,
    choicelist=['Khách lẻ', 'Khách mới'],
    default='Khách quay lại'
)

# --- REARRANGE AND FILL DATA ---
# Select and order the final display columns
Hoadontheothoigian = Hoadontheothoigian[[ 
    'Cửa hàng','Mã hoá đơn','Số hoá đơn','Ngày','Giờ vào', 'Giờ ra', 
    'Bàn','Tên hàng','Số lượng','Đơn giá','Số khách','Tổng hóa đơn',
    'Giảm giá','Loại thành viên','Tên','SĐT','Loại khách hàng'
]]

# Fill missing invoice data (ffill/bfill) within the same 'Số hoá đơn' group
cols_to_exclude = ['Loại thành viên', 'Tên', 'SĐT']
cols_to_fill = [c for c in Hoadontheothoigian.columns if c not in cols_to_exclude and c != 'Số hoá đơn']

Hoadontheothoigian[cols_to_fill] = Hoadontheothoigian.groupby('Số hoá đơn')[cols_to_fill].ffill().bfill()

# Sort data by Invoice, Date, and Item name to ensure chronological order
Hoadontheothoigian = Hoadontheothoigian.sort_values(by=['Số hoá đơn', 'Ngày', 'Tên hàng'])

# --- ADD ROW MARKER ---
# Use cumcount to distinguish multiple items of the same type in a single bill
Hoadontheothoigian['Mark'] = Hoadontheothoigian.groupby(['Số hoá đơn', 'Tên hàng','Số lượng']).cumcount() + 1

# Final Display
Hoadontheothoigian

# --- Order Log Transfer ---

# 1. Toppings parse function (keeping your preferred logic)
def parse_toppings(x):
    if pd.isna(x) or str(x).strip() in ["", "[]"]:
        return []
    try:
        # Handle iPOS single quote string format
        return ast.literal_eval(str(x))
    except:
        return []

# --- PROCESSING START ---

# Step 1: Parse the existing toppings column in the Log file
sale_change_log['toppings_list'] = sale_change_log['toppings'].apply(parse_toppings)

# Step 2: Prepend None to the list to preserve the main item row
# [None, topping1, topping2] -> Explodes into 3 rows: 1 main item, 2 toppings
sale_change_log['toppings_to_explode'] = sale_change_log['toppings_list'].apply(lambda x: [None] + x)

# Step 3: Explode (Increases row count from ~843 to ~1200)
log_final = sale_change_log.explode('toppings_to_explode').reset_index(drop=True)

# Step 4: Normalize (Flatten topping columns)
toppings_flat = pd.json_normalize(log_final['toppings_to_explode'])
toppings_flat.index = log_final.index

# Step 5: Smart Overwrite (Fillna)
# If the row is a Topping, take Topping data; if it's a main item (NaN), keep original
for col in toppings_flat.columns:
    if col in log_final.columns:
        log_final[col] = toppings_flat[col].fillna(log_final[col])
    else:
        # If topping has unique columns not present in main item, add them
        log_final[col] = toppings_flat[col]

# Step 6: Add '+' prefix to distinguish Toppings under the main item
is_topping = log_final['toppings_to_explode'].notna()
if 'item_name' in log_final.columns:
    log_final.loc[is_topping, 'item_name'] = "+ " + log_final.loc[is_topping, 'item_name'].astype(str)

# Step 7: Cleanup helper columns
log_final = log_final.drop(columns=['toppings_list', 'toppings_to_explode'])

# Check results
print(f"Total rows after processing: {len(log_final)}")
print(log_final[['tran_id', 'item_name', 'quantity', 'price', 'amount']].head(10))


def get_modify_message(x):
    try:
        # Convert string to dict
        data = ast.literal_eval(str(x))
        # Get specific field, return empty if not found
        return data.get('message_modify_table', '')
    except:
        return ''

# Create new column
log_final['message_modify_table'] = log_final['extra_data'].apply(get_modify_message)

def get_correct_tran_id(data_str):
    try:
        # Convert string to dictionary
        data = ast.literal_eval(data_str)

        # Get tran_id at the top level of the dictionary
        if isinstance(data, dict):
            return data.get('tran_id')
        # If it's a list, take the last element then get tran_id
        elif isinstance(data, list) and len(data) > 0:
            return data[-1].get('tran_id')
    except:
        return None

# Apply to DataFrame
log_final['tran_id'] = log_final['change_data'].apply(get_correct_tran_id).fillna(log_final['tran_id'])

# 1. Create temporary column to determine the sign
# Use .str.strip() to handle leading spaces before checking for '+'
is_topping = log_final['item_name'].str.strip().str.startswith('+', na=False) 
# If NOT a topping -> Get the sign of the quantity
# If IS a topping -> Set as NaN to be filled from the row above later
log_final['temp_sign'] = np.where(~is_topping, np.sign(log_final['quantity']), np.nan)

# 2. Forward fill the sign (ffill)
# Group by 'tran_id' to ensure signs don't leak between different invoices
log_final['temp_sign'] = log_final.groupby('tran_id')['temp_sign'].ffill()

# 3. Update Quantity
# Topping quantity = (Its absolute value) * (Main item's sign)
# fillna(1) as a safety measure if the first row is a topping (rare)
log_final['quantity'] = log_final['quantity'].abs() * log_final['temp_sign'].fillna(1)

# --- STEP 4: TIME PROCESSING (Use unit='ms' to prevent OutOfBounds errors) ---
if 'tran_date' in log_final.columns:
    # Convert milliseconds to datetime
    log_final['tran_date'] = pd.to_datetime(log_final['tran_date'], unit='ms')

    # Convert to VN timezone and reformat
    log_final['tran_date'] = (log_final['tran_date']
                                  .dt.tz_localize('UTC')
                                  .dt.tz_convert('Asia/Ho_Chi_Minh')
                                  .dt.strftime('%Y/%m/%d %H:%M:%S'))

# 1. Define mapping (Select columns and rename)
mapping = {
    'tran_id': 'Mã hoá đơn', 'tran_no': 'Số hoá đơn', 'tran_date': 'Thời gian',
    'table_name': 'Bàn', 'employee_name': 'Nhân viên', 'log_type': 'Loại log',
    'message_modify_table': 'Ghi chú', 'item_name': 'Món', 'quantity': 'Số lượng order'
}

# 2. Filter existing columns and rename immediately
cols_to_use = [c for c in mapping.keys() if c in log_final.columns]
Nhatkyorder = log_final[cols_to_use].rename(columns=mapping)

# 1. Identify columns to fill (all except Invoice No and Notes)
cols_to_exclude = ['Số hoá đơn', 'Ghi chú']
cols_to_fill = [c for c in Nhatkyorder.columns if c not in cols_to_exclude and c != 'Mã hoá đơn']

# 2. Group by Invoice ID and perform filling within each group
# Use ffill then bfill to ensure maximum data coverage
Nhatkyorder[cols_to_fill] = Nhatkyorder.groupby('Mã hoá đơn')[cols_to_fill].ffill().bfill()

# --- FILTER LOG TYPES ---
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



# Fill NaN for 'Loại log' first to enable filtering
Nhatkyorder['Loại log'] = Nhatkyorder['Loại log'].ffill()

# Filter for specific change logs
Nhatkyorder = Nhatkyorder[Nhatkyorder['Loại log'].isin(['SALE_CHANGE','SALE_SPLIT_ORDER','SALE_MERGE_ORDER'])]

# --- LOG STATISTICS ---
so_luong_gop = Nhatkyorder['Loại log'].str.contains('SALE_MERGE_ORDER', na=False, regex=False).sum()
so_luong_tach = Nhatkyorder['Loại log'].str.contains('SALE_SPLIT_ORDER', na=False, regex=False).sum()
so_luong_sua = Nhatkyorder['Loại log'].str.contains('SALE_CHANGE', na=False, regex=False).sum()

print(f"--- SYSTEM LOG STATISTICS ---")
print(f"1. Rows [Merge Order]: {so_luong_gop}")
print(f"2. Rows [Split Order]: {so_luong_tach}")
print(f"3. Rows [Edit Order]:  {so_luong_sua}")
print(f"-----------------------------")
print(f"Total logs to process: {so_luong_gop + so_luong_tach + so_luong_sua}")

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

## **Nhatkyorder nhóm 1: Lấy những hóa đơn có chứa "Gộp đơn" hoặc "Tách đơn"**"""

# Step 1: Filter Invoice IDs containing SPLIT or MERGE
target_hoadon_list = Nhatkyorder[
    Nhatkyorder['Loại log'].str.contains('SALE_SPLIT_ORDER|SALE_MERGE_ORDER', case=False, na=False)
][['Mã hoá đơn']].drop_duplicates()

# Step 2: Merge with original order log to get the FULL history of these invoices
Nhatkyorder_gop_tach = Nhatkyorder.merge(
    target_hoadon_list,
    on='Mã hoá đơn',
    how='inner'
)

# Step 3: Sort by Time and Invoice ID to follow processing flow
Nhatkyorder_gop_tach = Nhatkyorder_gop_tach.sort_values(['Mã hoá đơn', 'Thời gian']).reset_index(drop=True)

# --- Xử lý trong trường hợp có chứa "Gộp đơn" ---

# Step 1: Create target invoice column
Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'] = None

# Step 2: Mask for rows containing the merge phrase
mask = Nhatkyorder_gop_tach['Ghi chú'].str.contains('gộp vào', case=False, na=False)

# Step 3: Extraction logic
def extract_new_invoice(text):
    if pd.isna(text):
        return None
    try:
        # Extract part after "gộp vào"
        after_phrase = text.split("gộp vào")[-1].strip()
        # Extract part before the first "-"
        result = after_phrase.split("-")[0].strip()
        return result
    except:
        return None

# Step 4: Apply extraction
Nhatkyorder_gop_tach.loc[mask, 'Mã hóa đơn sau khi gộp bàn'] = \
    Nhatkyorder_gop_tach.loc[mask, 'Ghi chú'].apply(extract_new_invoice)

# Replace empty strings with None and fill within the same Invoice group
Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'] = Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'].replace('', None)
Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn'] = (
    Nhatkyorder_gop_tach.groupby('Số hoá đơn')['Mã hóa đơn sau khi gộp bàn']
      .transform(lambda x: x.ffill().bfill())
)

# Remove merge log rows used for metadata extraction
Nhatkyorder_gop_tach = Nhatkyorder_gop_tach[~Nhatkyorder_gop_tach['Ghi chú'].str.contains('[Gộp đơn]', na=False, regex=False)]

# Trace original invoice to final merged invoice
mapping_ma = dict(zip(Nhatkyorder_gop_tach['Mã hoá đơn'], Nhatkyorder_gop_tach['Mã hóa đơn sau khi gộp bàn']))
mapping_so_hd = Nhatkyorder_gop_tach.drop_duplicates('Mã hoá đơn').set_index('Mã hoá đơn')['Số hoá đơn'].to_dict()

def find_final_ma(code):
    """Recursively trace to the final invoice ID."""
    visited = set()
    current_code = code
    while pd.notna(current_code) and current_code in mapping_ma:
        next_code = mapping_ma[current_code]
        if pd.isna(next_code) or next_code == current_code or next_code in visited:
            break
        visited.add(current_code)
        current_code = next_code
    return current_code

Nhatkyorder_gop_tach['Mã hoá đơn cuối cùng'] = Nhatkyorder_gop_tach['Mã hoá đơn'].apply(find_final_ma)
Nhatkyorder_gop_tach['Số hoá đơn cuối cùng'] = Nhatkyorder_gop_tach['Mã hoá đơn cuối cùng'].map(mapping_so_hd)

# Update existing columns and cleanup
Nhatkyorder_gop_tach['Mã hoá đơn'] = Nhatkyorder_gop_tach['Mã hoá đơn cuối cùng']
Nhatkyorder_gop_tach['Số hoá đơn'] = Nhatkyorder_gop_tach['Số hoá đơn cuối cùng']
Nhatkyorder_gop_tach = Nhatkyorder_gop_tach.drop(columns=['Mã hóa đơn sau khi gộp bàn', 'Mã hoá đơn cuối cùng', 'Số hoá đơn cuối cùng'])

"""### **Xử lý trong trường hợp có chứa "Tách đơn"**"""

# Separate Split Order logs from other operations
Nhatkyorder_tachdon = Nhatkyorder_gop_tach[Nhatkyorder_gop_tach['Loại log'].str.contains('SALE_SPLIT_ORDER', case=False, na=False)]
Nhatkyorder_con_lai = Nhatkyorder_gop_tach[~Nhatkyorder_gop_tach['Loại log'].str.contains('SALE_SPLIT_ORDER', case=False, na=False)]

# --- CHECK RESULTS ---
print(f"Total initial log rows: {len(Nhatkyorder_gop_tach)}")
print(f"Number of SALE_SPLIT_ORDER rows: {len(Nhatkyorder_tachdon)}")
print(f"Number of remaining operation rows: {len(Nhatkyorder_con_lai)}")
      
Nhatkyorder_tachdon = Nhatkyorder_gop_tach
Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'] = None

# Identify rows where items are removed to create a new invoice
mask = (Nhatkyorder_tachdon['Ghi chú'].str.contains('bỏ món', case=False, na=False)) & \
       (Nhatkyorder_tachdon['Ghi chú'].str.contains('tạo thành hóa đơn', case=False, na=False))

def extract_full_invoice(text):
    if pd.isna(text):
        return None
    match = re.search(r'([A-Z0-9]{10,})\s*-', text)
    if match:
        return match.group(1).strip()
    return None

Nhatkyorder_tachdon.loc[mask, 'Mã hóa đơn sau khi tách bàn'] = Nhatkyorder_tachdon.loc[mask, 'Ghi chú'].apply(extract_full_invoice)

# Fill Split ID only within rows marked as SALE_SPLIT_ORDER
Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'] = Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].replace('', None)
mask_tach = Nhatkyorder_tachdon['Loại log'] == 'SALE_SPLIT_ORDER'

Nhatkyorder_tachdon.loc[mask_tach, 'Mã hóa đơn sau khi tách bàn'] = (
    Nhatkyorder_tachdon.groupby('Mã hoá đơn')['Mã hóa đơn sau khi tách bàn']
    .transform(lambda x: x.ffill().bfill())
)
Nhatkyorder_tachdon.loc[~mask_tach, 'Mã hóa đơn sau khi tách bàn'] = None

# --- TRACE PARENT-CHILD RELATIONSHIP FOR SPLITS ---
mapping_df = Nhatkyorder_tachdon[Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].notna()][['Mã hoá đơn', 'Mã hóa đơn sau khi tách bàn']].drop_duplicates()
child_to_parent_map = dict(zip(mapping_df['Mã hóa đơn sau khi tách bàn'], mapping_df['Mã hoá đơn']))
invoice_to_no_map = dict(zip(Nhatkyorder_tachdon['Mã hoá đơn'], Nhatkyorder_tachdon['Số hoá đơn']))

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

# Map Group IDs and Root Invoice Numbers
Nhatkyorder_tachdon['Group_ID_Goc'] = Nhatkyorder_tachdon['Mã hoá đơn'].apply(lambda x: find_ultimate_root(x, child_to_parent_map))
Nhatkyorder_tachdon['Số HĐ Gốc'] = Nhatkyorder_tachdon['Group_ID_Goc'].map(invoice_to_no_map)
Nhatkyorder_tachdon['Số hoá đơn tách bàn'] = Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].map(invoice_to_no_map)

# --- CLEANUP AND FINAL SORTING ---
Nhatkyorder_tachdon = Nhatkyorder_tachdon[~Nhatkyorder_tachdon['Ghi chú'].str.contains(r'hóa đơn được tạo mới', case=False, na=False)].copy()

# Secondary sort column using the last 4 characters of the Split Invoice No
Nhatkyorder_tachdon['Ma_Sort_Phu'] = Nhatkyorder_tachdon['Số hoá đơn tách bàn'].fillna('').astype(str).str[-4:]

Nhatkyorder_tachdon = Nhatkyorder_tachdon.sort_values(
    by=['Group_ID_Goc', 'Thời gian', 'Ma_Sort_Phu'],
    ascending=[True, True, True]
).reset_index(drop=True)

Nhatkyorder_tachdon = Nhatkyorder_tachdon.drop(columns=['Ma_Sort_Phu'])

# Filter rows where Split ID exists but corresponding Invoice No is missing
Nhatkyorder_tachdon = Nhatkyorder_tachdon[~(Nhatkyorder_tachdon['Mã hóa đơn sau khi tách bàn'].notna() & Nhatkyorder_tachdon['Số hoá đơn tách bàn'].isna())]

# --- FINAL INVENTORY TRACKING LOGIC ---
invoice_to_no_map = dict(zip(Nhatkyorder_tachdon['Mã hoá đơn'], Nhatkyorder_tachdon['Số hoá đơn']))
Nhatkyorder_tachdon['Số lượng order'] = pd.to_numeric(Nhatkyorder_tachdon['Số lượng order'], errors='coerce').fillna(0)
Nhatkyorder_tachdon['Món'] = Nhatkyorder_tachdon['Món'].astype(str).str.strip()
Nhatkyorder_tachdon = Nhatkyorder_tachdon.replace({pd.NA: None, np.nan: None, "": None})

final_rows = []

# Process by family group (Root Invoice)
for group_id, df_group in Nhatkyorder_tachdon.groupby('Group_ID_Goc'):
    current_inventory = []
    df_sorted = df_group.sort_values('Thời gian')
    processed_indices = set()

    for idx, row in df_sorted.iterrows():
        if idx in processed_indices: continue

        # A. IF ITEM IS ADDED (SALE_CHANGE / SALE_ORDER)
        if row['Loại log'] in ['SALE_CHANGE', 'Sửa đơn', 'SALE_ORDER']:
            if row['Số lượng order'] > 0:
                current_inventory.append({
                    'data': row.to_dict(),
                    'Món': row['Món'],
                    'Số lượng': row['Số lượng order']
                })
            processed_indices.add(idx)

        # B. IF ITEM IS SPLIT (SALE_SPLIT_ORDER)
        elif 'SPLIT' in str(row['Loại log']) and row['Mã hóa đơn sau khi tách bàn']:
            ma_con_moi = row['Mã hóa đơn sau khi tách bàn']
            so_hd_con_moi = row['Số hoá đơn tách bàn']
            thoi_gian_tach = row['Thời gian']

            # Items STAYING at the original table (negative quantity in logs)
            df_staying_block = df_sorted[(df_sorted['Thời gian'] == thoi_gian_tach) & 
                                         (df_sorted['Mã hóa đơn sau khi tách bàn'] == ma_con_moi)]
            processed_indices.update(df_staying_block.index.tolist())
            staying_dict = df_staying_block.groupby('Món')['Số lượng order'].sum().abs().to_dict()

            new_inventory_for_root = []
            for item in current_inventory:
                ten_mon = item['Món']
                sl_trong_kho = item['Số lượng']
                sl_muon_o_lai = staying_dict.get(ten_mon, 0)

                # Not in staying list -> SPLIT TO NEW INVOICE
                sl_tach_di = sl_trong_kho - sl_muon_o_lai
                if sl_tach_di > 0:
                    new_split_row = pd.Series(item['data'])
                    new_split_row['Mã hoá đơn'] = ma_con_moi
                    new_split_row['Số hoá đơn'] = so_hd_con_moi
                    new_split_row['Số lượng order'] = sl_tach_di
                    new_split_row['Mã hóa đơn sau khi tách bàn'] = None
                    new_split_row['Số hoá đơn tách bàn'] = None
                    final_rows.append(new_split_row)

                # In staying list -> KEEP IN ROOT INVOICE
                if sl_muon_o_lai > 0:
                    sl_thuc_te_o_lai = min(sl_trong_kho, sl_muon_o_lai)
                    item['Số lượng'] = sl_thuc_te_o_lai
                    new_inventory_for_root.append(item)
                    staying_dict[ten_mon] -= sl_thuc_te_o_lai

            current_inventory = new_inventory_for_root

    # Commit remaining items to root invoice
    for item in current_inventory:
        if item['Số lượng'] > 0:
            final_root_row = pd.Series(item['data'])
            final_root_row['Số lượng order'] = item['Số lượng']
            final_root_row['Mã hóa đơn sau khi tách bàn'] = None
            final_root_row['Số hoá đơn tách bàn'] = None
            final_rows.append(final_root_row)

# Resulting DataFrame
Nhatkyorder_final = pd.DataFrame(final_rows).reset_index(drop=True)

# --- DROP UNNECESSARY COLUMNS ---
cols_to_drop = ['Mã hóa đơn sau khi tách bàn', 'Group_ID_Goc', 'Số HĐ Gốc','Số hoá đơn tách bàn']
Nhatkyorder_final = Nhatkyorder_final.drop(columns=cols_to_drop)

# Reset index for a cleaner table view
Nhatkyorder_final = Nhatkyorder_final.reset_index(drop=True)

# Combine processed split/merge logs with remaining logs to create Group 1
Nhatkyorder_nhom_1 = pd.concat([Nhatkyorder_con_lai, Nhatkyorder_final], ignore_index=True)

"""## **Order Log Group 2: Invoices containing only "Edit Order" (SALE_CHANGE)**"""

# 1. Define accepted log pattern (Only "Edit Order")
pattern_accept = 'SALE_CHANGE'

# 2. Mark rows that satisfy the "Edit Order" condition
mask_hop_le = Nhatkyorder['Loại log'].str.contains(pattern_accept, case=False, na=False)

# 3. Find list of Invoices containing any action OTHER than "Edit Order"
# (e.g., Merge, Split, etc.) -> These will be excluded from this group
hoa_don_co_hanh_dong_khac = Nhatkyorder.loc[~mask_hop_le, 'Số hoá đơn'].unique()

# 4. Filter for invoices whose ENTIRE history only consists of "Edit Order"
Nhatkyorder_nhom_2 = (Nhatkyorder[~Nhatkyorder['Mã hoá đơn'].isin(hoa_don_co_hanh_dong_khac)]
    .sort_values(['Mã hoá đơn', 'Thời gian'])
    .reset_index(drop=True))

"""## **Merge Group 1 and Group 2 into full Nhatkyorder data**"""

Nhatkyorder = pd.concat([Nhatkyorder_nhom_1, Nhatkyorder_nhom_2], ignore_index=True)
# Sort by Invoice No and Time
Nhatkyorder = Nhatkyorder.sort_values(by=['Số hoá đơn', 'Thời gian'], ascending=True)

"""## **Transform Nhatkyorder for proper merging**"""

# 1. Smart Conversion: Automatically detect string or numeric timestamps
# errors='coerce' prevents crashes by returning NaT for invalid data
if Nhatkyorder['Thời gian'].dtype == 'object':
    # For string formats: omit unit='ms'
    Nhatkyorder['Thời gian'] = pd.to_datetime(Nhatkyorder['Thời gian'], errors='coerce')
else:
    # For numeric timestamps: keep unit='ms'
    Nhatkyorder['Thời gian'] = pd.to_datetime(Nhatkyorder['Thời gian'], unit='ms', errors='coerce')

# 2. Extract Date/Time attributes
Nhatkyorder['Ngày'] = Nhatkyorder['Thời gian'].dt.strftime('%d/%m/%Y')
Nhatkyorder['Năm'] = Nhatkyorder['Thời gian'].dt.year
Nhatkyorder['Tháng'] = Nhatkyorder['Thời gian'].dt.strftime('%m-%Y')

# 3. Calculate Week (ISO format)
iso = Nhatkyorder['Thời gian'].dt.isocalendar()
Nhatkyorder['Tuần'] = iso.week.astype(str).str.zfill(2) + '-' + iso.year.astype(str)

# Ensure quantity column is numeric before FIFO processing
Nhatkyorder['Số lượng order'] = pd.to_numeric(Nhatkyorder['Số lượng order'], errors='coerce').fillna(0)

# --- REVERSE FIFO DEDUCTION LOGIC ---
# Separating positive and negative quantity rows
pos_df = Nhatkyorder[Nhatkyorder['Số lượng order'] > 0].copy()
neg_df = Nhatkyorder[Nhatkyorder['Số lượng order'] < 0].copy()

for _, neg_row in neg_df.iterrows():
    amount_to_deduct = abs(neg_row['Số lượng order'])
    bill_id = neg_row['Mã hoá đơn']
    item_name = neg_row['Món']

    mask = (pos_df['Số hoá đơn'] == bill_id) & (pos_df['Món'] == item_name)
    potential_indices = pos_df[mask].index.tolist()

    # Iterate backwards from the most recent entry
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

# Filter out fully deducted rows
Nhatkyorder = pos_df[pos_df['Số lượng order'] > 0]

total_order = Nhatkyorder['Số lượng order'].sum()
print(f"Total order quantity: {total_order}")

# 1. Ensure Time is datetime format for accurate sorting
Nhatkyorder['Thời gian'] = pd.to_datetime(Nhatkyorder['Thời gian'])

# 2. Sort by Invoice, Item, and Time to keep old rows first
Nhatkyorder = Nhatkyorder.sort_values(by=['Số hoá đơn', 'Món', 'Thời gian'])

# 3. Create 'Mark' column to track occurrence order of items per invoice
# cumcount() assigns 0, 1, 2... for duplicate items within the same bill
Nhatkyorder['Mark'] = Nhatkyorder.groupby(['Mã hoá đơn', 'Món','Số lượng order']).cumcount() + 1

"""# **Calculate Staff KPI**

## **Merge filtered_df & Nhatkyorder**
"""

# 1. Cast 'Số lượng' on the right table to numeric to match the left table
Hoadontheothoigian['Số lượng'] = pd.to_numeric(Hoadontheothoigian['Số lượng'], errors='coerce').fillna(0).astype(int)

# 2. Ensure Mark is numeric on both sides
Nhatkyorder['Mark'] = pd.to_numeric(Nhatkyorder['Mark'], errors='coerce').fillna(0).astype(int)
Hoadontheothoigian['Mark'] = pd.to_numeric(Hoadontheothoigian['Mark'], errors='coerce').fillna(0).astype(int)

# 3. Execute Merge
Databanhang_1 = pd.merge(
    Nhatkyorder[['Mã hoá đơn','Thời gian','Món','Mark','Nhân viên','Loại log','Số lượng order','Ngày','Tuần','Tháng','Năm']],
    Hoadontheothoigian[['Cửa hàng','Tên hàng','Số lượng','Mark','Đơn giá','Mã hoá đơn','Số hoá đơn','Bàn','Số khách','Loại khách hàng','SĐT']],
    left_on = ['Mã hoá đơn','Món','Số lượng order','Mark'],
    right_on = ['Mã hoá đơn','Tên hàng','Số lượng','Mark'],
    how = 'right'
)

# Separate rows with NaN items (failed matches)
Databanhang_2 = Databanhang_1[Databanhang_1['Món'].isna()]
Databanhang_3 = Databanhang_1[~Databanhang_1['Món'].isna()]

# 1. Identify missing rows by merging Nhatkyorder with successfully matched data
check_missing = pd.merge(
    Nhatkyorder,
    Databanhang_3[['Số hoá đơn', 'Món', 'Số lượng order', 'Mark']],
    on=['Số hoá đơn', 'Món', 'Số lượng order', 'Mark'],
    how='left',
    indicator=True
)

# 2. Filter rows that only exist in the left table (Nhatkyorder)
Nhatkyorder_chua_thoa_dk = check_missing[check_missing['_merge'] == 'left_only']
Nhatkyorder_chua_thoa_dk = Nhatkyorder_chua_thoa_dk.drop(columns=['_merge'])

# Cleanup Group 2 before secondary merge
Databanhang_2 = Databanhang_2.drop(columns=['Thời gian','Món','Nhân viên','Loại log','Số lượng order','Ngày','Tuần','Tháng','Năm'])

# Step 1: Clean left table to ensure unique Invoice ID + Item key to prevent row duplication
nhatky_clean = Nhatkyorder_chua_thoa_dk[['Mã hoá đơn','Món','Thời gian','Mark','Nhân viên','Loại log','Số lượng order','Ngày','Tuần','Tháng','Năm']].drop_duplicates(subset=['Mã hoá đơn', 'Món'])

# Step 2: Perform secondary merge
Databanhang_4 = pd.merge(
    nhatky_clean,
    Databanhang_2[['Cửa hàng','Tên hàng','Số lượng','Mark','Đơn giá','Mã hoá đơn','Số hoá đơn','Bàn','Số khách','Loại khách hàng','SĐT']],
    left_on = ['Mã hoá đơn','Món'],
    right_on = ['Mã hoá đơn','Tên hàng'],
    how = 'right'
)

# Step 3: Remove empty store rows
Databanhang_4 = Databanhang_4[Databanhang_4['Cửa hàng'].notna()]

# Final Merge and cleanup
Databanhang_4 = Databanhang_4[~Databanhang_4['Món'].isna()]
Databanhang_4 = Databanhang_4.drop(columns=['Mark_x','Mark_y'])
Databanhang_3 = Databanhang_3.drop(columns=['Mark'])
Databanhang = pd.concat([Databanhang_4, Databanhang_3], ignore_index=True)

# Re-sorting and finalizing column order
new_order = ['Ngày', 'Tuần', 'Tháng', 'Năm','Cửa hàng','Mã hoá đơn','Số hoá đơn','Loại khách hàng', 'Món', 'Đơn giá','Nhân viên', 'Bàn','Loại log','Số lượng order','Số khách', 'Thời gian','SĐT']
Databanhang = Databanhang.reindex(columns=new_order)
Databanhang = Databanhang.sort_values(by=['Thời gian'], ascending=True)
Databanhang['Số lượng order'] = pd.to_numeric(Databanhang['Số lượng order'], errors='coerce').astype(int)

# --- TOPPING PRICE CORRECTION ---

# 1. Define price list for toppings
bang_gia_topping = {
    '+ Topping Hạt đác': 15000,
    '+ Topping Trân Châu Ô Long': 10000,
    '+ Topping Trân Châu Trắng': 10000,
    '+ Không lấy Topping': 0
}

# 2. Directly update Unit Price based on item name matching
for ten_topping, gia_moi in bang_gia_topping.items():
    # Use re.ESCAPE or manual backslash to handle the "+" safely in regex
    mask = Databanhang['Món'].str.contains(ten_topping.replace('+', r'\+'), na=False)
    Databanhang.loc[mask, 'Đơn giá'] = gia_moi

# 3. Recalculate Order Value
Databanhang['Giá trị order'] = Databanhang['Số lượng order'] * Databanhang['Đơn giá']
print(Databanhang.shape)
# --- SAVE DATA ---
BASE_DIR = r"D:\Làm việc - Bảo Châu\Project\Scraping_data_ban_Pos"
PATH_LUU_DATA_BAN_HANG = os.path.join(BASE_DIR, "Data_ban_hang")
os.makedirs(PATH_LUU_DATA_BAN_HANG, exist_ok=True)

# Generate dynamic file path based on dates
full_path_log = os.path.join(PATH_LUU_DATA_BAN_HANG, f"Data_Ban_Hang_{f_str}_{t_str}.xlsx")
Databanhang.to_excel(full_path_log, index=False)

"""## **Calculate Related KPIs**"""

# Group by dimensions to aggregate Sales Value
group_by_ban_hang = (
    Databanhang.groupby(
        ['Ngày','Tuần','Tháng','Năm', 'Cửa hàng', 'Số hoá đơn', 'Số khách', 'Loại khách hàng', 'Nhân viên','SĐT'],
        as_index=False
    )['Giá trị order']
    .sum()
)

# Sort for readability
group_by_ban_hang = group_by_ban_hang.sort_values(['Ngày', 'Số hoá đơn', 'Nhân viên']).reset_index(drop=True)
