# ===== app0822.py（修正版骨架） =====
import os
import re
import json  # 你用到 json，需要補這行
import importlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime, timedelta, time
# 這個檔案不要在頂層呼叫 st.xxx；import streamlit 是可以的，但建議只在需要的函式裡再 import
# import streamlit as st   # ← 若你只有在個別函式要用，建議在函式內再 import，避免頂層觸發 UI
from ortools.sat.python import cp_model
import matplotlib.dates as mdates


def clear_temp_folder(folder_name, base_path="暫存資料夾"):
    """
    清除指定暫存子資料夾中的所有檔案。
    """
    dir_path = os.path.join(base_path, folder_name)
    if not os.path.exists(dir_path):
        print(f"[略過] 找不到資料夾：{dir_path}")
        return

    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                print(f"[已刪除] {file_path}")
            except Exception as e:
                print(f"[錯誤] 無法刪除 {file_path}：{e}")
        else:
            print(f"[略過] 非檔案（可能是資料夾）：{file_path}")


# 檢查並建立所需資料夾（頂層執行純檔案系統動作是安全的）

def create_folders():
    temp_dir = "暫存資料夾"
    record_dir = "排程紀錄資料夾"
    subfolders = ["注塑", "彩印", "充填", "總排程"]
    for base in [temp_dir, record_dir]:
        if not os.path.exists(base):
            os.makedirs(base)
        for sub in subfolders:
            sub_path = os.path.join(base, sub)
            if not os.path.exists(sub_path):
                os.makedirs(sub_path)

create_folders()  # 保留：不涉及 Streamlit，僅做資料夾建置


# =============================================================================
# 存檔函數：將 DataFrame 分別存到暫存與排程紀錄資料夾
# =============================================================================
def save_schedule_df(station, df):
    today = datetime.now().strftime("%Y%m%d")
    temp_path = os.path.join("暫存資料夾", station, f"{station}_排程.xlsx")
    record_path = os.path.join("排程紀錄資料夾", station, f"{station}_{today}_排程.xlsx")
    df.to_excel(temp_path, index=False)
    df.to_excel(record_path, index=False)


# =============================================================================
# 儲存績效（result）到暫存資料夾（以 JSON 格式）
# =============================================================================
def save_performance(station, perf):
    temp_file = os.path.join("暫存資料夾", station, f"{station}_績效.json")
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(perf, f, ensure_ascii=False, indent=2)


# 讀取績效（result）
def load_performance(station):
    temp_file = os.path.join("暫存資料夾", station, f"{station}_績效.json")
    if os.path.exists(temp_file):
        with open(temp_file, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return None


# =============================================================================
# 讀取暫存排程結果（Excel檔）
# =============================================================================
def load_schedule_df(station):
    temp_path = os.path.join("暫存資料夾", station, f"{station}_排程.xlsx")
    if os.path.exists(temp_path):
        return pd.read_excel(temp_path)
    else:
        return None


# =============================================================================
# 儲存機台狀態（以 JSON 格式）
# =============================================================================
def save_machine_status(station, status_dict):
    status_file = os.path.join("暫存資料夾", station, f"{station}_機台狀態.json")
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(status_dict, f, ensure_ascii=False, indent=2)


def default_converter(o):
    if isinstance(o, (np.int64, np.int32)):
        return int(o)
    if isinstance(o, (np.float64, np.float32)):
        return float(o)
    # 如有需要，這裡可以處理其它型態
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")


def save_job_id_mapping(jobs, output_filename="job_id_mapping.json"):
    """
    將 jobs 列表中每個工作的 job_id 與其相關資訊儲存成 JSON 檔。

    :param jobs: 包含工作資訊的列表，每個元素為一個字典，其中應包含 "job_id" 鍵
    :param output_filename: 輸出檔案名稱，預設為 "job_id_mapping.json"
    """
    # 建立映射字典，這裡我們把 job_id 當作 key，整個工作資訊作為 value
    mapping = {job["job_id"]: job for job in jobs}

    output_path = os.path.join("暫存資料夾", "總排程", output_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, default=default_converter)
    print("Job ID Mapping 已儲存到", output_path)


# 讀取機台狀態
def load_machine_status(station):
    status_file = os.path.join("暫存資料夾", station, f"{station}_機台狀態.json")
    if os.path.exists(status_file):
        with open(status_file, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return {}


def compute_machine_status(schedule_records):
    """
    根據排程記錄計算各機台的狀態，
    除了紀錄每台機台最新的完成時間外，
    還同時記錄該筆工作的配方資訊與批號。

    各站點的配方資訊定義如下：
      - Injection: combination 與 batch
      - Printing: print_code 與 tone
      - Filling: order_code 與 batch

    回傳格式：
      {
          "machine_id": {"finish_time": finish_time, "recipe": {配方資訊}, "batch": batch_no},
          ...
      }
    """
    status = {}
    for rec in schedule_records:
        machine = rec["Machine ID"]
        finish_time = rec["Proc End"]
        station = rec["Station"]
        # 根據站點決定配方資訊，使用英文鍵
        if station == "Injection":
            recipe = {
                "combination": rec.get("組合編號", ""),
                "batch": rec.get("批次", "")
            }
        elif station == "Printing":
            recipe = {
                "print_code": rec.get("圖型碼", ""),
                "tone": rec.get("TONE", "")
            }
        elif station == "Filling":
            recipe = {
                "order_code": rec.get("工單號", ""),
                "batch": rec.get("批次", "")
            }
        else:
            recipe = None

        # 取得該筆工作的批號（這裡仍以中文鍵 "批號" 儲存，可視需要改為英文）
        batch_no = rec.get("批號", "")

        # 如果該機台已有紀錄，則以完成時間較晚者更新資訊
        if machine in status:
            if finish_time > status[machine]["finish_time"]:
                status[machine] = {
                    "finish_time": finish_time,
                    "recipe": recipe,
                    "batch": batch_no
                }
        else:
            status[machine] = {
                "finish_time": finish_time,
                "recipe": recipe,
                "batch": batch_no
            }
    return status


# =============================================================================
# 資料前處理函數
# =============================================================================
def find_shipping_date(df):
    df['出貨日'] = df['出貨日'].astype(str)

    def extract_date(text):
        match = re.search(r'交期(\d{1,2}/\d{1,2})', text)
        return match.group(1) if match else None

    df['日期'] = df['出貨日'].apply(extract_date)
    df['日期'] = pd.to_datetime(df['日期'], format='%m/%d', errors='coerce')
    df['日期'] = df['日期'].dt.strftime('%m/%d')
    return df


def find_combinations(df):
    df['組合編號'] = df['組合'].str.extract(r'([A-Z]+)')
    df['批次'] = df['組合'].str.extract(r'[A-Z]+(.*)')
    df = df.dropna(subset=['組合編號', '批次'])
    return df


def read_and_preprocess_ws(ws_file, base_date, join_date):
    ws = pd.read_excel(ws_file)
    # 使用使用者輸入的 join_date 結合 00:00 作為加入時間
    ws['加入時間'] = join_date
    ws['數量'] = pd.to_numeric(ws['數量'].str.replace('車', '', regex=False), errors='coerce')
    ws['數量'] = pd.to_numeric(ws['數量'], errors='coerce')
    ws = find_shipping_date(ws)
    ws = find_combinations(ws)
    ws['工單號'] = ws['批號'].str.extract(r'([A-Za-z])')
    col_idx = ws.columns.get_loc('批次') + 1
    ws.insert(col_idx, '工單號', ws.pop('工單號'))
    columns = ['批號', '加入時間', '數量', '圖型碼', 'TONE', '出貨日', 'Due Date', "Arrival Time", '組合編號', '批次',
               '工單號']

    current_year = datetime.now().year
    ws['日期'] = pd.to_datetime(ws['日期'], format='%m/%d', errors='coerce')
    ws['日期'] = ws['日期'].apply(lambda x: pd.to_datetime(f"{x.strftime('%m/%d')}/{current_year}", format='%m/%d/%Y')
    if pd.notnull(x) else None)
    large_number = 9999999
    ws['Due Date'] = ws['日期'].apply(
        lambda x: int((x - pd.Timestamp(base_date)).total_seconds() // 60) if pd.notnull(x) else large_number)
    if pd.isnull(join_date):
        arrival_time = 0
    else:
        diff = join_date - base_date
        arrival_time = int(diff.total_seconds() // 60)
    ws['Arrival Time'] = arrival_time
    tone_mapping = {'1T': 1, '2T': 2, '3T': 3, '4T': 4}
    ws['TONE'] = ws['TONE'].map(tone_mapping)
    ws['TONE'] = pd.to_numeric(ws['TONE'], errors='coerce')

    return ws[columns]


def load_job_id_mapping(input_filename="job_id_mapping.json"):
    """
    從 "暫存資料夾/總排程" 讀取 job_id mapping，回傳一個字典
    格式：{ job_id (字串): 工作資訊字典, ... }
    如果檔案不存在則回傳空字典。
    """
    path = os.path.join("暫存資料夾", "總排程", input_filename)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            mapping = json.load(f)
        return mapping
    else:
        return {}


def save_job_id_mapping_from_mapping(mapping, output_filename="job_id_mapping.json"):
    """
    將 mapping（格式：{ job_id (字串): 工作資訊字典, ... }）儲存成 JSON 檔。
    """
    output_path = os.path.join("暫存資料夾", "總排程", output_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, default=default_converter)
    print("Job ID Mapping 已儲存到", output_path)


def generate_jobs(ws, df_injection, df_printing, df_fill_large, df_fill_small, df_fill_special, special_filling_codes,
                  baseline_date):
    """
    根據訂單檔 (ws) 與其他相關檔案產生 jobs 列表，同時利用 "暫存資料夾/總排程/job_id_mapping.json"
    去比對每筆工作是否已存在（以批號作為比對依據），若存在則沿用原 job_id；若不存在則分配新的 job_id。
    """
    jobs = []
    # 讀取既有的 job id mapping
    mapping = load_job_id_mapping()
    # 建立一個以批號作為 key 的字典：{ 批號: job_id, ... }
    existing_batch_to_jobid = {}
    for job_id_str, job_info in mapping.items():
        # 這邊 job_id 存在 mapping 的 key中（可能是字串），轉換為 int
        existing_batch_to_jobid[job_info.get("批號", "")] = int(job_id_str)

    # 如果 mapping 不為空，取得目前最大 job_id；否則從 -1 開始（後續會 +1）
    if existing_batch_to_jobid:
        current_max = max(existing_batch_to_jobid.values())
    else:
        current_max = -1

    for idx, row in ws.iterrows():
        quantity = row.get('數量', 1)
        batch_number = row.get("批號", "")
        # 從 df_injection 取得注塑處理時間
        inj_candidates = df_injection[df_injection["num_batch"] == quantity]
        injection_time = inj_candidates["processing_time"].iloc[0] if not inj_candidates.empty else 100
        tone = row.get("TONE", None)
        # 從 df_printing 取得彩印處理時間，考慮 TONE
        prt_candidates = df_printing[(df_printing["num_batch"] == quantity) & (df_printing["TONE"] == tone)]
        printing_time = prt_candidates["processing_time"].iloc[0] if not prt_candidates.empty else 80
        order_code = row.get("工單號", "")
        if order_code in special_filling_codes:
            fill_candidates = df_fill_special[df_fill_special["num_batch"] == quantity]
            filling_special = fill_candidates["processing_time"].iloc[0] if not fill_candidates.empty else 90
            filling_big = filling_special
            filling_small = filling_special
        else:
            fill_candidates_big = df_fill_large[df_fill_large["num_batch"] == quantity]
            filling_big = fill_candidates_big["processing_time"].iloc[0] if not fill_candidates_big.empty else 90
            fill_candidates_small = df_fill_small[df_fill_small["num_batch"] == quantity]
            filling_small = fill_candidates_small["processing_time"].iloc[0] if not fill_candidates_small.empty else 90
            filling_special = None
        arrival_time_dt = row.get('加入時間', None)
        if pd.isnull(arrival_time_dt):
            arrival_time = 0
        else:
            diff = arrival_time_dt - baseline_date
            arrival_time = int(diff.total_seconds() // 60)

        due_date_val = row.get('Due Date', 9999999)
        if pd.isnull(due_date_val):
            due_date_val = 9999999

        # 判斷該筆工作的批號是否已存在於 mapping 中
        if batch_number in existing_batch_to_jobid:
            job_id = existing_batch_to_jobid[batch_number]
        else:
            # 若不存在，則分配新的 job_id：為 current_max + 1
            current_max += 1
            job_id = current_max
            # 更新 existing_batch_to_jobid 與 mapping
            existing_batch_to_jobid[batch_number] = job_id

        # 建立工作字典
        job = {
            "job_id": job_id,
            "批號": batch_number,
            "數量": quantity,
            "process_times": {
                "injection": injection_time,
                "printing": printing_time,
                "filling_big": filling_big,
                "filling_small": filling_small,
                "filling_special": filling_special
            },
            "recipes": {
                "injection": {"combination": row.get("組合編號", ""), "batch": row.get("批次", "")},
                "printing": {"print_code": row.get("圖型碼", "")},
                "filling": {"order_code": order_code, "batch": row.get("批次", "")}
            },
            "TONE": tone,
            "Due Date": due_date_val,
            "Arrival Time": arrival_time,
            "Machine ID": {
                "injection": "",
                "printing": "",
                "filling": "",
            },
            "start_times": {
                "injection": 0,
                "printing": 0,
                "filling": 0,
            },
            "finish_times": {
                "injection": 0,
                "printing": 0,
                "filling": 0,
            }
        }
        jobs.append(job)
        # 這邊我們每次都更新 mapping，方便後續檢查
        mapping[str(job_id)] = job

    # 儲存更新後的 mapping 到 JSON 檔
    save_job_id_mapping_from_mapping(mapping)
    return jobs


def get_pending_job_ids_and_previous_df_and_machine_state(station_chinese_name, baseline_date, join_date):
    # 讀取第一次彩印排程結果（存檔於 "彩印排程.xlsx"）
    # 嘗試讀取排程結果檔案

    df_old = load_schedule_df(station_chinese_name)
    if df_old is None or df_old.empty:
        print(f"[警告] 無法讀取 {station_chinese_name} 排程檔案，使用空資料。")
        df_old = pd.DataFrame(columns=[
            "Job ID", "批號", "數量", "Station", "Machine ID",
            "Proc Start", "Proc End", "組合編號", "圖型碼",
            "TONE", "工單號", "批次", "Due Date", "Arrival Time"
        ])

    mapping = load_job_id_mapping()
    all_batches = df_old["批號"].tolist()
    all_batches_in_mapping = [job.get("批號") for job in mapping.values()]
    # 抓取新工作
    diff_batches = list(set(all_batches_in_mapping) - set(all_batches))
    # 將日期轉換為分鐘
    join_time = int((join_date - baseline_date).total_seconds() // 60)
    # 過濾出已完成與正在進行中的工作：
    # 已完成：Proc End 小於等於 printing_join_time
    done_jobs_df = df_old[df_old["Proc End"] <= join_time].copy()
    # 正在進行中：Proc Start <= printing_join_time 且 Proc End > printing_join_time
    df_in_progress = df_old[(df_old["Proc Start"] <= join_time) & (df_old["Proc End"] > join_time)].copy()

    # 尚未開始的工作：Proc Start 大於 printing_join_time
    df_not_started = df_old[df_old["Proc Start"] > join_time].copy()
    unprocess_jobs = df_not_started["批號"].tolist()
    pending_job_ids = diff_batches + unprocess_jobs

    # 將已完成與正在進行中的工作合併，作為機台現有狀態的依據
    df_previous = pd.concat([done_jobs_df, df_in_progress], ignore_index=True)
    df_previous = df_previous.sort_values(by=["Machine ID", "Proc End"]).reset_index(drop=True)
    previous_machine_state = compute_machine_status(df_previous.to_dict(orient="records"))

    return pending_job_ids, df_previous, previous_machine_state


def get_jobs(pending_job_ids):
    mapping = load_job_id_mapping()
    jobs = []
    for job_id, job in mapping.items():
        batch = job.get("批號", "")
        if batch in pending_job_ids:
            jobs.append(job)
    return jobs


# =============================================================================
# 排程模型與求解函數（各站點獨立求解）
# =============================================================================
def compute_setup_time(station, job1, job2, setup_params):
    if station == "Injection":
        if job1["recipes"]["injection"]["combination"] != job2["recipes"]["injection"]["combination"]:
            return int(setup_params.get("injection_combination", 0))
        elif job1["recipes"]["injection"].get("batch", None) != job2["recipes"]["injection"].get("batch", None):
            return int(setup_params.get("injection_batch", 0))
        else:
            return 0
    elif station == "Printing":
        if job1["recipes"]["printing"]["print_code"] != job2["recipes"]["printing"]["print_code"]:
            return int(setup_params.get("printing_code", 0))
        else:
            return 0
    elif station == "Filling":
        if job1["recipes"]["filling"]["order_code"] != job2["recipes"]["filling"]["order_code"]:
            return int(setup_params.get("filling_order", 0))
        elif job1["recipes"]["filling"].get("batch", None) != job2["recipes"]["filling"].get("batch", None):
            return int(setup_params.get("filling_batch", 0))
        else:
            return 0
    return 0


def compurt_setup_time_update(station, prev_state, m_id, job2, setup_params):
    if station == 'Injection':
        if prev_state[m_id]['recipe']['combination'] != job2["recipes"]["injection"]["combination"]:
            return int(setup_params.get("injection_combination", 0))
        elif prev_state[m_id]['recipe']['batch'] != job2["recipes"]["injection"].get("batch", None):
            return int(setup_params.get("injection_batch", 0))
        else:
            return 0

    elif station == "Printing":
        if prev_state[m_id]["recipe"]["print_code"] != job2["recipes"]["printing"]["print_code"]:
            return int(setup_params.get("printing_code", 0))
        else:
            return 0
    elif station == "Filling":
        if prev_state[m_id]["recipe"]["order_code"] != job2["recipes"]["filling"]["order_code"]:
            return int(setup_params.get("filling_order", 0))
        elif prev_state[m_id]['recipe']['batch'] != job2["recipes"]["filling"].get("batch", None):
            return int(setup_params.get("filling_batch", 0))
        else:
            return 0
    return 0


def augment_schedule_records(schedule_records, jobs, setup_params, big_machines):
    """
    根據排程記錄和 jobs 列表，整合各工作之配方、機台與排程時間等資訊。
    若排程記錄中的 "Job ID" 無法直接對應到 jobs 列表，則嘗試以「批號」尋找對應工作。

    回傳一個整理後的 augmented_records 列表，每個元素為一個字典，包含各工作資訊。
    """
    from collections import defaultdict

    grouped = defaultdict(list)
    # 將排程記錄依據站點與機台分組
    for rec in schedule_records:
        key = (rec["Station"], rec["Machine ID"])
        grouped[key].append(rec)

    augmented_records = []
    for (station, machine_id), recs in grouped.items():
        # 若記錄中有 "Proc Start" 則以其為準，否則以 "Start" 當作起始欄位
        start_key = "Proc Start" if "Proc Start" in recs[0] else "Start"
        # 依據起始時間排序該機台上的記錄
        recs_sorted = sorted(recs, key=lambda r: r[start_key])
        for i, rec in enumerate(recs_sorted):
            # 嘗試根據 "Job ID" 尋找對應工作
            job_id = rec.get("Job ID")
            job = None
            for job_item in jobs:
                if job_item.get("job_id") == job_id:
                    job = job_item
                    break
            # 如果找不到，再嘗試根據 "批號" 比對
            if job is None:
                batch = rec.get("批號", "")
                for job_item in jobs:
                    if job_item.get("批號", "") == batch:
                        job = job_item
                        break
            # 若仍無法對應，則跳過該記錄
            if job is None:
                continue

            # 計算該筆工作在排程記錄中的處理時間
            processing_time = rec["Proc End"] - rec[start_key]

            if i == 0:
                idle_start = 0
                idle_end = rec[start_key]
                setup_start = rec[start_key]
                setup_end = rec[start_key]
                setup_time = 0
            else:
                prev_rec = recs_sorted[i - 1]
                # 取得前一筆記錄對應的工作資訊（依同樣邏輯搜尋）
                prev_job_id = prev_rec.get("Job ID")
                prev_job = None
                for job_item in jobs:
                    if job_item.get("job_id") == prev_job_id:
                        prev_job = job_item
                        break
                if prev_job is None:
                    # 若無法取得前一筆工作資訊則跳過
                    continue
                # 根據配方差異計算換線所需時間
                computed_setup = compute_setup_time(station, prev_job, job, setup_params)
                setup_time = computed_setup
                expected_start = prev_rec["Proc End"] + computed_setup
                idle_start = expected_start
                idle_end = rec[start_key]
                setup_start = prev_rec["Proc End"]
                setup_end = prev_rec["Proc End"] + computed_setup

            # 整理該筆記錄所有資訊
            augmented = {
                "Job ID": job.get("job_id"),
                "批號": job.get("批號", ""),
                "數量": job.get("數量", ""),
                "組合編號": rec.get("組合編號", ""),
                "圖型碼": rec.get("圖型碼", ""),
                "TONE": job.get("TONE", ""),
                "工單號": rec.get("工單號", ""),  # 這裡採用排程記錄中的工單號
                "批次": rec.get("批次", ""),
                "Station": station,
                "Machine ID": machine_id,
                "Machine Type": "normal" if station != "Filling" else (
                    "big" if machine_id in big_machines else "small"),
                "Setup Time": setup_time,
                "Processing Time": processing_time,
                "Idle Start": idle_start,
                "Idle End": idle_end,
                "Setup Start": setup_start,
                "Setup End": setup_end,
                "Proc Start": rec[start_key],
                "Proc End": rec["Proc End"],
                "Due Date": job.get("Due Date", 0),
                "Arrival Time": job.get("Arrival Time", 0)
            }
            augmented_records.append(augmented)
    return augmented_records


def plot_gantt_chart(df_records):
    plt.figure(figsize=(10, 6))
    ax = plt.gca()

    df_records["Row Key"] = df_records["Station"] + " - " + df_records["Machine ID"].astype(str)
    station_order = {"Injection": 0, "Printing": 1, "Filling": 2}

    def sort_key(key):
        station = key.split(" - ")[0]
        return station_order.get(station, 100)

    row_keys = sorted(df_records["Row Key"].unique().tolist(), key=sort_key)
    row_map = {key: i for i, key in enumerate(row_keys)}

    color_idle = "lightgray"
    color_setup = "lightskyblue"
    color_proc = "lightgreen"

    for i, row in df_records.iterrows():
        row_idx = row_map[row["Row Key"]]
        idle_duration = row["Idle End"] - row["Idle Start"]
        if idle_duration > 0:
            ax.barh(row_idx, idle_duration, left=row["Idle Start"], height=0.6,
                    color=color_idle, edgecolor="black", label="Idle" if i == 0 else None)
        setup_duration = row["Setup End"] - row["Setup Start"]
        if setup_duration > 0:
            ax.barh(row_idx, setup_duration, left=row["Setup Start"], height=0.6,
                    color=color_setup, edgecolor="black", label="Setup" if i == 0 else None)
        proc_duration = row["Proc End"] - row["Proc Start"]
        if proc_duration > 0:
            ax.barh(row_idx, proc_duration, left=row["Proc Start"], height=0.6,
                    color=color_proc, edgecolor="black", label="Processing" if i == 0 else None)
        ax.text(row["Proc Start"] + proc_duration / 2, row_idx,
                f"J{row['Job ID']}", ha='center', va='center', fontsize=8)

    ax.set_yticks(list(row_map.values()))
    ax.set_yticklabels(list(row_map.keys()))
    ax.set_xlabel("Time (minutes)")
    ax.set_ylabel("Station - Machine")
    ax.set_title("Gantt Chart (Idle/Setup/Processing)")
    plt.grid(axis="x", linestyle=":", alpha=0.5)

    handles, labels = ax.get_legend_handles_labels()
    unique = {}
    for h, l in zip(handles, labels):
        if l not in unique:
            unique[l] = h
    ax.legend(unique.values(), unique.keys())

    plt.tight_layout()
    return plt


import json
import os
import matplotlib.pyplot as plt
from collections import defaultdict
from datetime import datetime


def augment_records_from_json(mapping, station, setup_params):
    """
    根據 mapping（即 job_id_mapping.json 轉成的字典）和指定站點（"injection"/"printing"/"filling"）計算
    每筆工作的 idle 時間與 setup 時間。假設每筆工作已有 "start_times" 與 "finish_times"（均為 dict）。

    若該站點內，同一機台上依照該站的 start_times 排序，若有前一筆工作，
    則：
      - setup_time = compute_setup_time(station, prev_job, curr_job, setup_params)
      - idle_time = (前一筆工作 finish_time + setup_time) 到目前工作 start_time之間的間隔
    若沒有前一筆，idle 與 setup 均為 0。

    回傳一個列表，每個元素為一個字典，內容包含：
      - job_id, 批號, Station, Machine ID, Setup Time, Idle Time, Processing Time, Proc Start, Proc End, Due Date, Arrival Time
    """
    # 將 mapping 中的所有工作轉成列表
    jobs = list(mapping.values())
    # 將工作根據指定站點的機台分組
    grouped = defaultdict(list)
    for job in jobs:
        # station 轉成小寫以比對
        st = station.lower()
        if "Machine ID" in job and st in job["Machine ID"]:
            machine = job["Machine ID"][st]
            grouped[machine].append(job)

    augmented_records = []
    # 對每台機台的工作依據 start_times[station] 排序
    for machine, job_list in grouped.items():
        job_list_sorted = sorted(job_list, key=lambda j: j["start_times"][station.lower()])
        prev_finish = None
        prev_job = None
        for job in job_list_sorted:
            start_time = job["start_times"][station.lower()]
            finish_time = job["finish_times"][station.lower()]
            processing_time = finish_time - start_time
            if prev_job is None:
                setup_time = 0
                idle_time = 0
                setup_start = start_time
                setup_end = start_time
            else:
                setup_time = compute_setup_time(station.lower(), prev_job, job, setup_params)
                setup_start = prev_finish
                setup_end = prev_finish + setup_time
                idle_time = start_time - (prev_finish + setup_time)
                if idle_time < 0:
                    idle_time = 0
            record = {
                "job_id": job["job_id"],
                "批號": job.get("批號", ""),
                "Station": station.capitalize(),
                "Machine ID": machine,
                "組合編號": job.get("recipes", {}).get("injection", {}).get("combination", ""),
                "圖型碼": job.get("recipes", {}).get("printing", {}).get("print_code", ""),
                "工單號": job.get("recipes", {}).get("filling", {}).get("order_code", ""),
                "批次": job.get("recipes", {}).get("injection", {}).get("batch", ""),
                "TONE": job.get("TONE"),
                "Setup Time": setup_time,
                "Idle Time": idle_time,
                "Processing Time": processing_time,
                "Proc Start": start_time,
                "Proc End": finish_time,
                "Due Date": job.get("Due Date", 0),
                "Arrival Time": job.get("Arrival Time", 0)
            }
            augmented_records.append(record)
            prev_job = job
            prev_finish = finish_time
    return augmented_records


# 定義繪圖函式
def plot_gantt_chart(intervals, title="Gantt Chart"):
    colors = {"idle": "lightgray", "setup": "skyblue", "processing": "lightgreen"}
    station_order = ["injection", "printing", "filling"]
    machine_list_order = []
    for station in station_order:
        machines = set()
        for _, rec in intervals.iterrows():
            if rec["Station"].lower() == station:
                machines.add(rec["Machine ID"])
        for m in sorted(machines):
            machine_list_order.append((station, m))
    machine_to_y = {(st, m): i for i, (st, m) in enumerate(machine_list_order)}

    fig, ax = plt.subplots(figsize=(12, 8))
    for _, rec in intervals.iterrows():
        st_lower = rec["Station"].lower()
        m_id = rec["Machine ID"]
        y = machine_to_y.get((st_lower, m_id))
        if y is None:
            continue
        if rec["Idle Time"] > 0:
            ax.barh(y, rec["Idle Time"], left=rec["Proc Start"] - rec["Setup Time"] - rec["Idle Time"], height=0.8,
                    color=colors["idle"], edgecolor="black")
        if rec["Setup Time"] > 0:
            ax.barh(y, rec["Setup Time"], left=rec["Proc Start"] - rec["Setup Time"], height=0.8, color=colors["setup"],
                    edgecolor="black")
        processing_duration = rec["Processing Time"]
        ax.barh(y, processing_duration, left=rec["Proc Start"], height=0.8, color=colors["processing"],
                edgecolor="black")
        ax.text(rec["Proc Start"] + processing_duration / 2, y, f"J{rec['Job ID']}", ha="center", va="center",
                fontsize=8, color="black")

    y_ticks = list(range(len(machine_list_order)))
    y_labels = [f"{st.capitalize()}-{m}" for st, m in machine_list_order]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)
    ax.set_xlabel("Time (minutes)")
    ax.set_title(title)
    ax.grid(axis="x", linestyle="--", alpha=0.5)
    plt.tight_layout()
    return plt


def plot_gantt_chart_datetime(intervals, baseline_datetime, title="Gantt Chart with Timestamps"):
    colors = {"idle": "lightgray", "setup": "skyblue", "processing": "lightgreen"}
    station_order = ["injection", "printing", "filling"]
    machine_list_order = []

    # 先建立機台順序
    for station in station_order:
        machines = set()
        for _, rec in intervals.iterrows():
            if rec["Station"].lower() == station:
                machines.add(rec["Machine ID"])
        for m in sorted(machines):
            machine_list_order.append((station, m))
    machine_to_y = {(st, m): i for i, (st, m) in enumerate(machine_list_order)}

    fig, ax = plt.subplots(figsize=(12, 8))
    for _, rec in intervals.iterrows():
        st_lower = rec["Station"].lower()
        m_id = rec["Machine ID"]
        y = machine_to_y.get((st_lower, m_id))
        if y is None:
            continue

        # 計算對應時間
        start_dt = baseline_datetime + timedelta(minutes=rec["Proc Start"])
        end_dt = baseline_datetime + timedelta(minutes=rec["Proc End"])
        setup_start = start_dt - timedelta(minutes=rec["Setup Time"])
        idle_start = setup_start - timedelta(minutes=rec["Idle Time"])

        if rec["Idle Time"] > 0:
            ax.barh(y, timedelta(minutes=rec["Idle Time"]), left=idle_start, height=0.8, color=colors["idle"],
                    edgecolor="black")
        if rec["Setup Time"] > 0:
            ax.barh(y, timedelta(minutes=rec["Setup Time"]), left=setup_start, height=0.8, color=colors["setup"],
                    edgecolor="black")
        ax.barh(y, end_dt - start_dt, left=start_dt, height=0.8, color=colors["processing"], edgecolor="black")

        # 標註工作 ID
        ax.text(start_dt + (end_dt - start_dt) / 2, y, f"J{rec['Job ID']}", ha="center", va="center", fontsize=8,
                color="black")

    y_ticks = list(range(len(machine_list_order)))
    y_labels = [f"{st.capitalize()}-{m}" for st, m in machine_list_order]
    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)

    # X軸設為時間格式
    ax.xaxis_date()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d %H:%M'))
    ax.set_xlabel("Datetime")
    ax.set_title(title)
    ax.grid(axis="x", linestyle="--", alpha=0.5)
    plt.xticks(rotation=45)
    plt.tight_layout()
    return plt


def convert_minutes_to_datetime(df, baseline_date, columns=["Proc Start", "Proc End", "Due Date", "Arrival Time"]):
    """
    將 DataFrame 中指定的以分鐘為單位的欄位轉為 datetime 格式（基準點為 baseline_date）
    """
    df = df.copy()
    for col in columns:
        if col in df.columns:
            df[col + "（時間）"] = df[col].apply(
                lambda x: baseline_date + timedelta(minutes=int(x)) if pd.notnull(x) else None)
    return df


# 計算每一筆與前一筆同機台工作的 setup time
def compute_setup_time_for_df(station, row1, row2, setup_params):
    if station == "Injection":
        if row1["組合編號"] != row2["組合編號"]:
            return int(setup_params.get("injection_combination", 0))
        elif row1["批次"] != row2["批次"]:
            return int(setup_params.get("injection_batch", 0))
        else:
            return 0
    elif station == "Printing":
        if row1['圖型碼'] != row2["圖型碼"]:
            return int(setup_params.get("printing_code", 0))
        else:
            return 0
    elif station == "Filling":
        if row1["工單號"] != row2["工單號"]:
            return int(setup_params.get("filling_order", 0))
        elif row1["批次"] != row2["批次"]:
            return int(setup_params.get("filling_batch", 0))
        else:
            return 0
    return 0


def convert_json_to_station_dict(baseline_date, setup_params):
    mapping = load_job_id_mapping()
    # 定義全部站點順序，從下到上顯示依序為 注塑(injection) → 彩印(printing) → 充填(filling)
    stations = ["injection", "printing", "filling"]
    # 依序計算所有站點的區間資訊
    all_intervals = []
    for st in stations:
        intervals = augment_records_from_json(mapping, st, setup_params)
        all_intervals.extend(intervals)
    all_df = pd.DataFrame(all_intervals)
    # 轉換其中一台機台的資料（例如 injection_dict["P_0117"]）
    converted_df = convert_minutes_to_datetime(all_df, baseline_date)
    # 先依 Station 分出三個 DataFrame
    injection_df = converted_df[converted_df["Station"] == "Injection"].copy()
    printing_df = converted_df[converted_df["Station"] == "Printing"].copy()
    filling_df = converted_df[converted_df["Station"] == "Filling"].copy()

    # 分組成字典：每台機台對應一個 DataFrame
    injection_dict = {machine: df for machine, df in injection_df.groupby("Machine ID")}
    printing_dict = {machine: df for machine, df in printing_df.groupby("Machine ID")}
    filling_dict = {machine: df for machine, df in filling_df.groupby("Machine ID")}
    return converted_df, injection_dict, printing_dict, filling_dict


def merge_station_timings_with_info(all_df):
    # 建立站點對應中文名稱
    station_name_map = {
        "injection": "注塑",
        "printing": "彩印",
        "filling": "充填"
    }

    # 要保留的資訊欄位（非時間）
    info_cols = ['job_id', '批號', '組合編號', '圖型碼', '工單號', '批次', 'TONE']

    dfs = []
    for station_en, station_zh in station_name_map.items():
        # 過濾出該站資料
        df_sub = all_df[all_df["Station"].str.lower() == station_en].copy()
        # 選擇必要欄位
        use_cols = info_cols + ["Proc Start（時間）", "Proc End（時間）"]
        df_sub = df_sub[use_cols]
        df_sub = df_sub.rename(columns={
            "Proc Start（時間）": f"{station_zh}開始",
            "Proc End（時間）": f"{station_zh}結束"
        })
        dfs.append(df_sub)

    # 依照所有 info_cols 做 merge（以 job_id 為主鍵，但也考慮批號資訊一致）
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = pd.merge(
            merged_df, df,
            on=['job_id', '批號', '組合編號', '圖型碼', '工單號', '批次', 'TONE'],
            how='outer'
        )

    return merged_df.sort_values(by="job_id").reset_index(drop=True)


def solve_injection(jobs, machine_list, prev_state, setup_params, solve_time):
    # 讀取現有的 mapping
    mapping = load_job_id_mapping()
    # 建立 CP 模型
    model = cp_model.CpModel()
    # 取得工作數量
    num_jobs = len(jobs)
    # 設定一個足夠大的上界 (horizon)
    horizon = 10000000
    # 用字典儲存每個工作的操作變數 (開始、結束、處理時間、分配機台)
    ops = {}
    for j in range(num_jobs):
        # 取得工作 j 的注塑處理時間（轉成整數）
        proc_time = int(jobs[j]["process_times"]["injection"])
        # 建立工作 j 的開始時間變數，其範圍從 0 到 horizon
        start = model.NewIntVar(0, horizon, f'start_inj_{j}')
        # 建立工作 j 的結束時間變數
        end = model.NewIntVar(0, horizon, f'end_inj_{j}')
        # 建立工作 j 的機台分配變數，值為機台列表的索引
        machine = model.NewIntVar(0, len(machine_list) - 1, f'machine_inj_{j}')
        # 加上約束：工作 j 的結束時間 = 開始時間 + 處理時間
        model.Add(end == start + proc_time)
        # 考慮 Arrival Time（到達時間）：工作 j 的開始時間不能早於其到達時間
        model.Add(start >= jobs[j].get("Arrival Time", 0))
        # 新增 setup_time 變數
        setup_time_var = model.NewIntVar(0, horizon, f'setup_time_inj_{j}')
        # 若該機台有先前完成狀態，則限制分配到該機台的工作其開始時間至少要大於該狀態
        for m_idx, m_id in enumerate(machine_list):
            if m_id in prev_state:
                # 建立布林變數 b，表示工作 j 是否分配到機台 m_id
                b = model.NewBoolVar(f'job_{j}_on_machine_{m_idx}')
                model.Add(machine == m_idx).OnlyEnforceIf(b)
                model.Add(machine != m_idx).OnlyEnforceIf(b.Not())
                # 從 prev_state 取得該機台先前的完成時間與配方
                prev_finish = prev_state[m_id]["finish_time"]
                prev_recipe = prev_state[m_id]["recipe"]
                # 計算由前次配方到本工作所需的換線時間
                extra_setup = compurt_setup_time_update('Injection', prev_state, m_id, jobs[j], setup_params)
                # 限制：工作 j 的開始時間必須大於等於 前次完成時間 + 額外換線時間
                model.Add(start >= prev_finish + extra_setup).OnlyEnforceIf(b)
                # 綁定 setup_time_var
                model.Add(setup_time_var == extra_setup).OnlyEnforceIf(b)
        # 如果該機台沒有前次狀態，則 setup_time_var 設為 0
        for m_idx, m_id in enumerate(machine_list):
            if m_id not in prev_state:
                b_no_prev = model.NewBoolVar(f'job_{j}_on_machine_{m_idx}_no_prev')
                model.Add(machine == m_idx).OnlyEnforceIf(b_no_prev)
                model.Add(machine != m_idx).OnlyEnforceIf(b_no_prev.Not())
                model.Add(setup_time_var == 0).OnlyEnforceIf(b_no_prev)
        # 儲存工作 j 的所有變數到 ops 字典中 (新增 setup_time_var)
        ops[j] = (start, end, proc_time, machine, setup_time_var)

    # 設定同一台機台間工作換線的相關約束與成本
    total_setup_vars = []
    for j1 in range(num_jobs):
        for j2 in range(j1 + 1, num_jobs):
            # 建立布林變數，判斷工作 j1 與 j2 是否分配到同一台機台
            same_machine = model.NewBoolVar(f'same_inj_{j1}_{j2}')
            model.Add(ops[j1][3] == ops[j2][3]).OnlyEnforceIf(same_machine)
            model.Add(ops[j1][3] != ops[j2][3]).OnlyEnforceIf(same_machine.Not())
            # 建立布林變數，決定同一機台上哪個工作先做：j1 在 j2 前 or j2 在 j1 前（必定互斥且和為1）
            order_j1_before_j2 = model.NewBoolVar(f'order_inj_{j1}_before_{j2}')
            order_j2_before_j1 = model.NewBoolVar(f'order_inj_{j2}_before_{j1}')
            model.Add(order_j1_before_j2 + order_j2_before_j1 == 1).OnlyEnforceIf(same_machine)
            # 根據配方差異計算換線所需時間
            setup_time = compute_setup_time("Injection", jobs[j1], jobs[j2], setup_params)
            # 若 j1 在 j2 前，則 j2 的開始時間至少為 j1 結束時間加上換線時間
            model.Add(ops[j2][0] >= ops[j1][1] + setup_time).OnlyEnforceIf(order_j1_before_j2)
            # 反之，若 j2 在 j1 前，則 j1 的開始時間至少為 j2 結束時間加上換線時間
            model.Add(ops[j1][0] >= ops[j2][1] + setup_time).OnlyEnforceIf(order_j2_before_j1)
            # 設定換線成本變數，其值為 setup_time（依照順序決定）
            setup_cost = model.NewIntVar(0, setup_time, f'setup_inj_{j1}_{j2}')
            model.Add(setup_cost == order_j1_before_j2 * setup_time + order_j2_before_j1 * setup_time)
            total_setup_vars.append(setup_cost)

    # 計算所有換線成本的總和
    total_setup = model.NewIntVar(0, horizon, "total_setup_inj")
    if total_setup_vars:
        model.Add(total_setup == sum(total_setup_vars))
    else:
        model.Add(total_setup == 0)

    # 定義 Cmax 為所有工作結束時間中的最大值
    end_vars = [ops[j][1] for j in range(num_jobs)]
    Cmax = model.NewIntVar(0, horizon, "Cmax_inj")
    model.AddMaxEquality(Cmax, end_vars)

    # 設定目標：最小化 Cmax 與換線成本的和（此處換線成本的權重為 1）
    w_setup = 1
    model.Minimize(5*Cmax + w_setup * total_setup)

    # 設定求解器與求解時間上限
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = solve_time
    status = solver.Solve(model)

    # 如果求解成功，則整理結果
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        actual_solve_time = solver.WallTime()  # 實際求解時間
        result = {"Cmax": solver.Value(Cmax),
                  "total_setup": solver.Value(total_setup),
                  "status": solver.StatusName(status),
                  "solve_time": actual_solve_time}
        # 收集每個工作的完工時間
        finish_times = [solver.Value(ops[j][1]) for j in range(num_jobs)]

        # 更新每個工作的開始完工時間到job_id_mapping.json中
        start_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][0]) for j in range(num_jobs)}
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in start_times_dict:
                job["start_times"]["injection"] = start_times_dict[batch]

        finish_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][1]) for j in range(num_jobs)}
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in finish_times_dict:
                job["finish_times"]["injection"] = finish_times_dict[batch]

        machine_ids_dict = {jobs[j]["批號"]: machine_list[solver.Value(ops[j][3])] for j in range(num_jobs)}
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in machine_ids_dict:
                job["Machine ID"]["injection"] = machine_ids_dict[batch]

        # 新增 setup_time 字典記錄
        setup_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][4]) for j in range(num_jobs)}
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in setup_times_dict:
                job.setdefault("setup_times", {})
                job["setup_times"]["injection"] = setup_times_dict[batch]

        # 儲存更新後的 mapping
        save_job_id_mapping_from_mapping(mapping)
        # 建立排程記錄的列表，每筆記錄包含各欄位資訊
        schedule_records = []
        for j in range(num_jobs):
            record = {
                "Job ID": jobs[j].get("job_id"),
                "批號": jobs[j]["批號"],
                "數量": jobs[j].get("數量", ""),
                "Station": "Injection",
                "Machine ID": machine_list[solver.Value(ops[j][3])],
                "Proc Start": solver.Value(ops[j][0]),
                "Proc End": solver.Value(ops[j][1]),
                # 下面四個欄位來自於工作配方資料（注塑階段：組合編號與批次）
                "組合編號": jobs[j]["recipes"]["injection"]["combination"],
                "圖型碼": jobs[j]["recipes"]["printing"]["print_code"],  # 注塑階段不會使用彩印圖型碼，預設留空
                "TONE": jobs[j].get("TONE", ""),
                "工單號": jobs[j]["recipes"]["filling"]["order_code"],  # 注塑階段不設定工單號
                "批次": jobs[j]["recipes"]["injection"]["batch"],
                "Due Date": jobs[j].get("Due Date", 0),
                "Arrival Time": jobs[j].get("Arrival Time", 0),

            }
            schedule_records.append(record)
        return result, finish_times, finish_times_dict, schedule_records, setup_times_dict
    else:
        print("找不到可行解")
        return None, None, None, None, None


def solve_printing(jobs, machine_list, prev_state_printing, setup_params, solve_time, SPECIAL_PRINT_CODES):
    mapping = load_job_id_mapping()

    # 建立 CP 模型
    model = cp_model.CpModel()
    num_jobs = len(jobs)
    horizon = 100000000
    # 分類工作：哪些需要進行彩印排程，哪些可直接跳過（特殊圖型碼）
    need_schedule = []
    skip_schedule = {}
    for j in range(num_jobs):
        print_code = jobs[j]["recipes"]["printing"]["print_code"]
        batch = jobs[j].get('批號')
        target_job = None
        for job_id, job_data in mapping.items():
            if job_data.get('批號') == batch:
                target_job = job_data
                break
        # 如果圖型碼屬於特殊彩印碼，則不需要進行排程，直接設定完工時間
        if print_code in SPECIAL_PRINT_CODES:
            # skip_schedule[j] = injection_finish_times[j] + 1440
            skip_schedule[j] = target_job['finish_times']['injection'] + 1440
        else:
            need_schedule.append(j)

    # 建立需要排程工作的操作變數
    ops = {}
    for j in need_schedule:
        proc_time = int(jobs[j]["process_times"]["printing"])
        start = model.NewIntVar(0, horizon, f'start_prt_{j}')
        end = model.NewIntVar(0, horizon, f'end_prt_{j}')
        machine = model.NewIntVar(0, len(machine_list) - 1, f'machine_prt_{j}')
        setup_time_var = model.NewIntVar(0, horizon, f'setup_time_prt_{j}')
        model.Add(end == start + proc_time)
        # 考慮到達時間以及必須等待1440分鐘後（例如彩印流程延遲）
        model.Add(start >= jobs[j].get("Arrival Time", 0))
        batch = jobs[j].get('批號')
        target_job = None

        for job_id, job_data in mapping.items():
            if job_data.get('批號') == batch:
                target_job = job_data
                break

        # model.Add(start >= injection_finish_times[j] + 1440)
        model.Add(start >= target_job['finish_times']['injection'] + 1440)
        # 加入機台狀態的約束：若該機台有先前完成狀態，則要求開始時間不小於該狀態的完工時間加上額外換線時間
        for m_idx, m_id in enumerate(machine_list):
            if m_id in prev_state_printing:
                b = model.NewBoolVar(f'job_{j}_on_machine_{m_idx}')
                model.Add(machine == m_idx).OnlyEnforceIf(b)
                model.Add(machine != m_idx).OnlyEnforceIf(b.Not())
                prev_finish = prev_state_printing[m_id]["finish_time"]
                # 此處使用 compurt_setup_time_update 計算從該機台前次配方到本工作所需的換線時間，
                # 注意：此函數應能根據站點 "Printing" 與前次狀態的 recipe（例如 {"print_code": ..., "TONE": ...}）計算換線時間
                extra_setup = compurt_setup_time_update('Printing', prev_state_printing, m_id, jobs[j], setup_params)
                model.Add(start >= prev_finish + extra_setup).OnlyEnforceIf(b)
                model.Add(setup_time_var == extra_setup).OnlyEnforceIf(b)
        ops[j] = (start, end, proc_time, machine, setup_time_var)

    # 設定同一台機台間工作換線的相關約束與成本
    total_setup_vars = []
    for idx1 in range(len(need_schedule)):
        for idx2 in range(idx1 + 1, len(need_schedule)):
            j1 = need_schedule[idx1]
            j2 = need_schedule[idx2]
            same_machine = model.NewBoolVar(f'same_prt_{j1}_{j2}')
            model.Add(ops[j1][3] == ops[j2][3]).OnlyEnforceIf(same_machine)
            model.Add(ops[j1][3] != ops[j2][3]).OnlyEnforceIf(same_machine.Not())
            order_j1_before_j2 = model.NewBoolVar(f'order_prt_{j1}_before_{j2}')
            order_j2_before_j1 = model.NewBoolVar(f'order_prt_{j2}_before_{j1}')
            model.Add(order_j1_before_j2 + order_j2_before_j1 == 1).OnlyEnforceIf(same_machine)
            # 根據彩印配方差異計算換線所需時間
            setup_time = compute_setup_time("Printing", jobs[j1], jobs[j2], setup_params)
            model.Add(ops[j2][0] >= ops[j1][1] + setup_time).OnlyEnforceIf(order_j1_before_j2)
            model.Add(ops[j1][0] >= ops[j2][1] + setup_time).OnlyEnforceIf(order_j2_before_j1)
            setup_cost = model.NewIntVar(0, setup_time, f'setup_prt_{j1}_{j2}')
            model.Add(setup_cost == order_j1_before_j2 * setup_time + order_j2_before_j1 * setup_time)
            total_setup_vars.append(setup_cost)

    # 計算所有換線成本的總和
    total_setup = model.NewIntVar(0, horizon, "total_setup_prt")
    if total_setup_vars:
        model.Add(total_setup == sum(total_setup_vars))
    else:
        model.Add(total_setup == 0)

    # 定義 Cmax 為所有工作完工時間中的最大值
    end_vars = [ops[j][1] for j in need_schedule] if need_schedule else []
    if end_vars:
        Cmax = model.NewIntVar(0, horizon, "Cmax_prt")
        model.AddMaxEquality(Cmax, end_vars)
    else:
        Cmax = model.NewIntVar(0, horizon, "Cmax_prt")
        # model.Add(Cmax == max(injection_finish_times))
        model.Add(Cmax == max(target_job['finish_times']['injection']))

    # 設定目標：最小化 Cmax 與換線成本的總和
    w_setup = 1
    model.Minimize(5*Cmax + w_setup * total_setup)

    # 建立求解器並求解
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = solve_time
    status = solver.Solve(model)

    # 建立排程記錄與完工時間列表
    schedule_records = []
    finish_times = [None] * num_jobs
    setup_times_dict = {}
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        actual_solve_time = solver.WallTime()
        result = {"Cmax": solver.Value(Cmax),
                  "total_setup": solver.Value(total_setup),
                  "status": solver.StatusName(status),
                  "solve_time": actual_solve_time}

        # 更新每個工作的開始完工時間到job_id_mapping.json中
        start_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][0]) for j in need_schedule}
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in start_times_dict:
                job["start_times"]["printing"] = start_times_dict[batch]

        # 建立 批號 → 彩印完工時間 對照表
        finish_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][1]) for j in need_schedule}
        # 更新 mapping 中對應工作的 finish_times["printing"]
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in finish_times_dict:
                job.setdefault("finish_times", {})
                job["finish_times"]["printing"] = finish_times_dict[batch]

        # 建立 批號 → 彩印機台 對照表
        machine_ids_dict = {jobs[j]["批號"]: machine_list[solver.Value(ops[j][3])] for j in need_schedule}
        # 更新 mapping 中對應工作的 finish_times["printing"]
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in machine_ids_dict:
                job.setdefault("finish_times", {})
                job["Machine ID"]["printing"] = machine_ids_dict[batch]

        # 新增 setup_time 字典記錄
        setup_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][4]) for j in need_schedule}
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in setup_times_dict:
                job.setdefault("setup_times", {})
                job["setup_times"]["printing"] = setup_times_dict[batch]

        # 儲存更新後的 mapping
        save_job_id_mapping_from_mapping(mapping)

        for j in need_schedule:
            finish_times[j] = solver.Value(ops[j][1])
            record = {
                "Job ID": jobs[j].get("job_id"),
                "Station": "Printing",
                "批號": jobs[j].get("批號", ""),
                "數量": jobs[j].get("數量", ""),
                "Machine ID": machine_list[solver.Value(ops[j][3])],
                "Proc Start": solver.Value(ops[j][0]),
                "Proc End": solver.Value(ops[j][1]),
                "組合編號": jobs[j]["recipes"]["injection"]["combination"],  # 彩印階段不使用組合編號
                "圖型碼": jobs[j]["recipes"]["printing"]["print_code"],
                "TONE": jobs[j].get("TONE", ""),
                "工單號": jobs[j]["recipes"]["filling"]["order_code"],  # 彩印階段不設定工單號
                "批次": jobs[j]["recipes"]["injection"]["batch"],  # 彩印階段不使用批次（如有需要可自行調整）
                "Due Date": jobs[j].get("Due Date", 0),
                "Arrival Time": jobs[j].get("Arrival Time", 0),
                "Setup Time": solver.Value(ops[j][4]),
            }
            schedule_records.append(record)
    else:
        result = None

    # 對於不需要排程的工作（特殊彩印），直接指定完工時間
    for j in skip_schedule:
        batch = jobs[j].get('批號')
        injection_end = 0

        # 從 mapping 找到對應工作，讀取 injection 完工時間
        for job_id_str, job_data in mapping.items():
            if job_data.get('批號') == batch:
                job_data.setdefault("finish_times", {})
                injection_end = job_data["finish_times"].get("injection", 0)
                # 更新彩印 finish_times
                job_data["start_times"]["printing"] = injection_end + 1440
                job_data["finish_times"]["printing"] = injection_end + 1440
                job_data["Machine ID"]["printing"] = 'NA'
                break

        finish_times[j] = injection_end + 1440
        record = {
            "Job ID": jobs[j].get("job_id"),
            "Station": "Printing",
            "批號": jobs[j].get("批號", ""),
            "數量": jobs[j].get("數量", ""),
            "Machine ID": "NA",
            "Proc Start": injection_end + 1440,
            "Proc End": injection_end + 1440,
            "組合編號": jobs[j]["recipes"]["injection"]["combination"],
            "圖型碼": jobs[j]["recipes"]["printing"]["print_code"],
            "TONE": jobs[j].get("TONE", ""),
            "工單號": jobs[j]["recipes"]["filling"]["order_code"],
            "批次": jobs[j]["recipes"]["injection"]["batch"],
            "Due Date": jobs[j].get("Due Date", 0),
            "Arrival Time": jobs[j].get("Arrival Time", 0),
            "Setup Time": 0,
        }
        schedule_records.append(record)
    # 最後再次儲存 mapping（包含 skip 任務的更新）
    save_job_id_mapping_from_mapping(mapping)

    # 新增: 返回 setup_times_dict
    return result, finish_times, schedule_records


def solve_filling(jobs, machine_list, prev_state_filling, setup_params, solve_time, big_machines,
                  special_filling_codes):
    mapping = load_job_id_mapping()

    # 建立 CP 模型
    model = cp_model.CpModel()
    num_jobs = len(jobs)
    horizon = 100000000
    ops = {}
    for j in range(num_jobs):
        # 為工作 j 建立動態處理時間變數 (proc_time_var)
        proc_time_var = model.NewIntVar(0, horizon, f'proc_fill_{j}')
        start = model.NewIntVar(0, horizon, f'start_fill_{j}')
        end = model.NewIntVar(0, horizon, f'end_fill_{j}')
        machine = model.NewIntVar(0, len(machine_list) - 1, f'machine_fill_{j}')
        # 根據工作 j 的充填工單（order_code）是否屬於特殊充填碼，選擇不同處理時間
        if jobs[j]["recipes"]["filling"]["order_code"] in special_filling_codes:
            times = [int(jobs[j]["process_times"].get("filling_special", 90)) for _ in range(len(machine_list))]
        else:
            times = []
            for m_id in machine_list:
                if m_id in big_machines:
                    times.append(int(jobs[j]["process_times"].get("filling_big", 90)))
                else:
                    times.append(int(jobs[j]["process_times"].get("filling_small", 90)))
        # 使用 AddElement 根據機台選擇對應的處理時間
        model.AddElement(machine, times, proc_time_var)
        # 結束時間 = 開始時間 + 處理時間
        model.Add(end == start + proc_time_var)
        # 考慮到達時間：工作 j 的開始時間不能早於其到達時間
        model.Add(start >= jobs[j].get("Arrival Time", 0))
        batch = jobs[j].get('批號')
        target_job = None

        for job_id, job_data in mapping.items():
            if job_data.get('批號') == batch:
                target_job = job_data
                break
        # 同時，工作 j 的開始時間也必須大於或等於對應彩印完工時間
        model.Add(start >= target_job['finish_times']['printing'])
        for m_idx, m_id in enumerate(machine_list):
            if m_id in prev_state_filling:
                b = model.NewBoolVar(f'job_{j}_on_machine_{m_idx}')
                model.Add(machine == m_idx).OnlyEnforceIf(b)
                model.Add(machine != m_idx).OnlyEnforceIf(b.Not())
                prev_finish = prev_state_filling[m_id]["finish_time"]
                # 此處使用 compurt_setup_time_update 計算從該機台前次配方到本工作所需的換線時間，
                extra_setup = compurt_setup_time_update('Filling', prev_state_filling, m_id, jobs[j], setup_params)
                model.Add(start >= prev_finish + extra_setup).OnlyEnforceIf(b)
        ops[j] = (start, end, proc_time_var, machine)

    # 設定同一機台上工作的換線成本
    total_setup_vars = []
    for j1 in range(num_jobs):
        for j2 in range(j1 + 1, num_jobs):
            same_machine = model.NewBoolVar(f'same_fill_{j1}_{j2}')
            model.Add(ops[j1][3] == ops[j2][3]).OnlyEnforceIf(same_machine)
            model.Add(ops[j1][3] != ops[j2][3]).OnlyEnforceIf(same_machine.Not())
            order_j1_before_j2 = model.NewBoolVar(f'order_fill_{j1}_before_{j2}')
            order_j2_before_j1 = model.NewBoolVar(f'order_fill_{j2}_before_{j1}')
            model.Add(order_j1_before_j2 + order_j2_before_j1 == 1).OnlyEnforceIf(same_machine)
            # 計算換線時間：根據充填配方比較
            setup_time = compute_setup_time("Filling", jobs[j1], jobs[j2], setup_params)
            model.Add(ops[j2][0] >= ops[j1][1] + setup_time).OnlyEnforceIf(order_j1_before_j2)
            model.Add(ops[j1][0] >= ops[j2][1] + setup_time).OnlyEnforceIf(order_j2_before_j1)
            setup_cost = model.NewIntVar(0, setup_time, f'setup_fill_{j1}_{j2}')
            model.Add(setup_cost == order_j1_before_j2 * setup_time + order_j2_before_j1 * setup_time)
            total_setup_vars.append(setup_cost)

    # 計算所有換線成本的總和
    total_setup = model.NewIntVar(0, horizon, "total_setup_fill")
    if total_setup_vars:
        model.Add(total_setup == sum(total_setup_vars))
    else:
        model.Add(total_setup == 0)

    # 定義 Cmax 為所有工作完工時間的最大值
    end_vars = [ops[j][1] for j in range(num_jobs)]
    Cmax = model.NewIntVar(0, horizon, "Cmax_fill")
    model.AddMaxEquality(Cmax, end_vars)

    # 目標：最小化 Cmax 與換線成本的加權和（權重 w_setup 設為 1）
    w_setup = 1
    model.Minimize(5*Cmax + w_setup * total_setup)

    # 求解模型
    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = solve_time
    status = solver.Solve(model)

    # 如果求解成功，整理結果
    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        actual_solve_time = solver.WallTime()
        result = {"Cmax": solver.Value(Cmax),
                  "total_setup": solver.Value(total_setup),
                  "status": solver.StatusName(status),
                  "solve_time": actual_solve_time}
        finish_times = [solver.Value(ops[j][1]) for j in range(num_jobs)]

        # 建立 批號 → 充填完工時間 對照表
        start_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][0]) for j in range(num_jobs)}
        # 更新 mapping 中對應工作的 finish_times["printing"]
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in start_times_dict:
                job.setdefault("finish_times", {})
                job["start_times"]["filling"] = start_times_dict[batch]

        # 建立 批號 → 彩印完工時間 對照表
        finish_times_dict = {jobs[j]["批號"]: solver.Value(ops[j][1]) for j in range(num_jobs)}
        # 更新 mapping 中對應工作的 finish_times["printing"]
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in finish_times_dict:
                job.setdefault("finish_times", {})
                job["finish_times"]["filling"] = finish_times_dict[batch]

        # 建立 批號 → 彩印完工時間 對照表
        machine_ids_times_dict = {jobs[j]["批號"]: machine_list[solver.Value(ops[j][3])] for j in range(num_jobs)}
        # 更新 mapping 中對應工作的 finish_times["printing"]
        for job_id_str, job in mapping.items():
            batch = job.get("批號", "")
            if batch in machine_ids_times_dict:
                job.setdefault("finish_times", {})
                job["Machine ID"]["filling"] = machine_ids_times_dict[batch]

        # 儲存更新後的 mapping
        save_job_id_mapping_from_mapping(mapping)

        schedule_records = []
        for j in range(num_jobs):
            record = {
                "Job ID": jobs[j].get("job_id"),
                "Station": "Filling",
                "批號": jobs[j].get("批號", ""),
                "數量": jobs[j].get("數量", ""),
                "Machine ID": machine_list[solver.Value(ops[j][3])],
                "Proc Start": solver.Value(ops[j][0]),
                "Proc End": solver.Value(ops[j][1]),
                # Recipe 四個欄位：組合編號來自注塑、圖型碼來自彩印、工單號及批次來自充填
                "組合編號": jobs[j]["recipes"]["injection"]["combination"],
                "圖型碼": jobs[j]["recipes"]["printing"]["print_code"],
                "TONE": jobs[j].get("TONE", ""),
                "工單號": jobs[j]["recipes"]["filling"]["order_code"],
                "批次": jobs[j]["recipes"]["filling"]["batch"],
                "Due Date": jobs[j].get("Due Date", 0),
                "Arrival Time": jobs[j].get("Arrival Time", 0)
            }
            schedule_records.append(record)
        return result, finish_times, schedule_records
    else:
        return None, None, None


def process_oven_cut_inspection(ws):
    def process_oven_cut_inspection_no_data(ws):
        # 處理烘箱、裁切、檢驗時間
        ws['烘箱開始'] = ws['充填結束']
        ws['烘箱結束'] = ws.apply(
            lambda row: row['烘箱開始'] + timedelta(hours=34.5) if row['圖型碼'] == 'AAA' else row[
                                                                                                   '烘箱開始'] + timedelta(
                hours=25.5),
            axis=1
        )
        ws['裁切開始'] = ws['烘箱結束']
        ws['裁切結束'] = ws['裁切開始'] + timedelta(hours=36)
        ws['檢驗開始'] = ws['裁切結束']
        ws['檢驗結束'] = ws['檢驗開始'] + timedelta(hours=48)
        ws['入庫開始'] = ws['檢驗結束']
        ws['入庫結束'] = ws['入庫開始'] + timedelta(hours=24)

        return ws

    ws = process_oven_cut_inspection_no_data(ws)

    return ws


# ------------------------------
# Streamlit 主程式
# ------------------------------
# 初始化 session_state
if "jobs" not in st.session_state:
    st.session_state["jobs"] = None
if "injection_results" not in st.session_state:
    st.session_state["injection_results"] = None
if "printing_results" not in st.session_state:
    st.session_state["printing_results"] = None
if "filling_results" not in st.session_state:
    st.session_state["filling_results"] = None
if "oven_cut_results" not in st.session_state:
    st.session_state["oven_cut_results"] = None

st.title("排程系統")
st.write("請依序上傳訂單檔 (D.xlsx)、時間表檔 (time_T_fight.xlsx) 與機台資料檔 (rule_D.xlsx)，並設定重要參數。")

ws_file = st.file_uploader("上傳訂單檔 (D.xlsx)", type=["xlsx"], key="ws_file")
time_file = st.file_uploader("上傳時間表檔 (time_T_fight.xlsx)", type=["xlsx"], key="time_file")
ruleD_file = st.file_uploader("上傳機台資料檔 (rule_D.xlsx)", type=["xlsx"], key="ruleD_file")
st.write("基準時間：")
# baseline 日期時間選擇
date_only_b = st.date_input("選擇基準日期", value=datetime(2025, 11, 1).date(), key="baseline_date_only")
hour_b = st.number_input("小時 (基準)", min_value=0, max_value=23, value=0, key="baseline_hour")
minute_b = st.number_input("分鐘 (基準)", min_value=0, max_value=59, value=0, key="baseline_minute")
baseline_date = datetime.combine(date_only_b, datetime.min.time()).replace(hour=hour_b, minute=minute_b)

st.write("加入日期：")
date_only_j = st.date_input("選擇加入日期", value=datetime(2025, 11, 1).date(), key="join_date_only")
hour_j = st.number_input("小時 (加入)", min_value=0, max_value=23, value=0, key="join_hour")
minute_j = st.number_input("分鐘 (加入)", min_value=0, max_value=59, value=0, key="join_minute")
join_date = datetime.combine(date_only_j, datetime.min.time()).replace(hour=hour_j, minute=minute_j)

st.write("請設定換線參數：")
injection_combination = st.number_input("注塑-組合換線時間", value=180, key="injection_combination")
injection_batch = st.number_input("注塑-批次換線時間", value=120, key="injection_batch")
printing_code = st.number_input("彩印-圖型換線時間", value=320, key="printing_code")
filling_order = st.number_input("充填-工單換線時間", value=60, key="filling_order")
filling_batch = st.number_input("充填-批次換線時間", value=20, key="filling_batch")
setup_params = {
    "injection_combination": injection_combination,
    "injection_batch": injection_batch,
    "printing_code": printing_code,
    "filling_order": filling_order,
    "filling_batch": filling_batch
}
SPECIAL_PRINT_CODES = [code.strip() for code in
                       st.text_input("特殊彩印代碼 (逗號分隔)", value="AAA,BBB", key="special_print_codes").split(",")]
special_filling_codes = [code.strip() for code in
                         st.text_input("特殊充填工單代碼 (逗號分隔)", value="OK", key="special_filling_codes").split(
                             ",")]
big_machines = [m.strip() for m in
                st.text_input("充填大機台 (逗號分隔)", value="P_0317,P_0318,P_0319,P_0320", key="big_machines").split(
                    ",")]

if ruleD_file is not None:
    rule_injection = pd.read_excel(ruleD_file, sheet_name="注塑")
    injection_available = rule_injection[(rule_injection["是否損壞"] == False) & (rule_injection["開機否"] == True)][
        "Machine_ID"].tolist()
    rule_printing = pd.read_excel(ruleD_file, sheet_name="彩印")
    printing_available = rule_printing[(rule_printing["是否損壞"] == False) & (rule_printing["開機否"] == True)][
        "Machine_ID"].tolist()
    rule_filling = pd.read_excel(ruleD_file, sheet_name="充填")
    filling_available = rule_filling[(rule_filling["是否損壞"] == False) & (rule_filling["開機否"] == True)][
        "Machine_ID"].tolist()
else:
    st.write("未上傳 rule_D.xlsx，使用預設機台清單。")
    injection_available = ["P_0101", "P_0102"]
    printing_available = ["P_0201", "P_0202"]
    filling_available = ["P_0301", "P_0302"]

if time_file is not None:
    df_injection = pd.read_excel(time_file, sheet_name="注塑")
    df_printing = pd.read_excel(time_file, sheet_name="彩印")
    df_fill_large = pd.read_excel(time_file, sheet_name="充填_大機台")
    df_fill_small = pd.read_excel(time_file, sheet_name="充填_小機台")
    df_fill_special = pd.read_excel(time_file, sheet_name="充填_特殊工號")
else:
    st.error("請上傳時間表檔 (time_T_fight.xlsx)")

if ws_file is not None and time_file is not None and ruleD_file is not None:
    ws = read_and_preprocess_ws(ws_file, baseline_date, join_date)
    st.write("共產生", len(ws), "筆訂單資料。")
    st.dataframe(ws)
    # 若尚未紀錄檔案名稱或上傳檔案名稱改變，則重新產生 jobs

    if st.button("更新新資料至JSON", key="update_new_data"):
        jobs = generate_jobs(ws, df_injection, df_printing, df_fill_large, df_fill_small, df_fill_special,
                             special_filling_codes, baseline_date)
        st.session_state["jobs"] = jobs
        st.session_state["ws_file_name"] = ws_file.name

if st.button("刪除暫存檔案", key="delete_all_data"):
    clear_temp_folder('注塑', base_path="暫存資料夾")
    clear_temp_folder('彩印', base_path="暫存資料夾")
    clear_temp_folder('充填', base_path="暫存資料夾")
    clear_temp_folder('總排程', base_path="暫存資料夾")

# 使用 tabs 分頁顯示注塑與彩印排程結果，結果不會互相消失
tabs = st.tabs(["【注塑排程】", "【彩印排程】", "【充填排程】", "【烘箱、裁切、入庫】"])

# ---------- 注塑排程頁籤 ----------
with tabs[0]:
    st.header("【注塑排程】")
    solve_time_injection = st.number_input("注塑排程求解時間上限 (秒)", min_value=10, max_value=1000000, value=60,
                                           step=10, key="solve_time_injection")
    if st.button("開始注塑排程", key="solve_injection"):
        pending_job_ids, done_df, prev_state_injection = get_pending_job_ids_and_previous_df_and_machine_state("注塑",
                                                                                                               baseline_date,
                                                                                                               join_date)
        jobs_pending = get_jobs(pending_job_ids)
        injection_result, injection_finish_times, injection_finish_times_dict, injection_records, injection_setup_times_dict = solve_injection(
            jobs_pending, injection_available, prev_state_injection, setup_params, solve_time_injection
        )

        if injection_result:
            station = "Injection"
            injection_df = pd.DataFrame(injection_records).sort_values(by=["Machine ID", "Proc Start"])
            # 新增注塑開始與注塑結束欄位
            injection_df["注塑開始"] = injection_df["Proc Start"].apply(lambda x: baseline_date + timedelta(minutes=x))
            injection_df["注塑結束"] = injection_df["Proc End"].apply(lambda x: baseline_date + timedelta(minutes=x))
            # 新增交期欄位（在 Due Date 旁邊）
            if "Due Date" in injection_df.columns:
                due_date_idx = injection_df.columns.get_loc("Due Date")
                injection_df.insert(due_date_idx + 1, "交期", injection_df["Due Date"].apply(
                    lambda x: baseline_date + timedelta(minutes=x) if pd.notnull(x) else None))
            # 將欄位插入至 Proc End 後方
            cols = injection_df.columns.tolist()
            insert_index = cols.index("Proc End") + 1
            cols.insert(insert_index, cols.pop(cols.index("注塑開始")))
            cols.insert(insert_index + 1, cols.pop(cols.index("注塑結束")))
            injection_df = injection_df[cols]

            updated_rows = []
            for machine_id, group in injection_df.groupby("Machine ID"):
                group = group.sort_values("Proc Start").reset_index(drop=True)

                # 計算 Setup Time
                group["Setup Time"] = [0] + [
                    compute_setup_time_for_df("Injection", group.iloc[i - 1], group.iloc[i], setup_params)
                    for i in range(1, len(group))
                ]

                # 計算 Idle Time
                group["Idle Time"] = [0] + [
                    max(0, group.loc[i, "Proc Start"] - (group.loc[i - 1, "Proc End"] + group.loc[i, "Setup Time"]))
                    for i in range(1, len(group))
                ]

                updated_rows.append(group)

            df_with_setup = pd.concat(updated_rows, ignore_index=True)
            # 計算 Processing Time 欄位（如尚未存在）
            if 'Processing Time' not in df_with_setup.columns:
                df_with_setup['Processing Time'] = df_with_setup['Proc End'] - df_with_setup['Proc Start']
            # 將 Setup Time 插入到 Proc Start 前面，Idle Time 放在 Setup Time 後
            cols = df_with_setup.columns.tolist()
            cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Processing Time")))
            cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Setup Time")))
            cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Idle Time")))
            df_with_setup = df_with_setup[cols]

            st.session_state["injection_results"] = {
                "result": injection_result,
                "records": injection_records,
                "df": injection_df,
                "intervals": df_with_setup
            }
            save_schedule_df("注塑", df_with_setup)
            save_performance("注塑", injection_result)
        else:
            st.error("注塑排程未找到可行解！")

    # 顯示注塑結果（若有）
    if st.session_state["injection_results"] is not None:
        from io import BytesIO
        from datetime import datetime

        station = "Injection"
        st.write("【注塑績效】", st.session_state.injection_results["result"])
        st.write("【注塑排程 DataFrame】")
        st.dataframe(st.session_state.injection_results["intervals"])
        st.write("【注塑甘特圖】")
        st.pyplot(plot_gantt_chart(st.session_state.injection_results["intervals"],
                                   title="Injection Gantt Chart with Setup & Idle Time"))
        st.pyplot(plot_gantt_chart_datetime(st.session_state["injection_results"]["intervals"], baseline_date,
                                            title="Injection Gantt Chart with Setup & Idle Time By Date"))
        # === 加入下載功能 ===
        st.write("### 📥 注塑排程 Excel")

        injection_df = st.session_state.injection_results["intervals"]
        machine_groups = {
            machine_id: group_df.sort_values(by="Proc Start").reset_index(drop=True)
            for machine_id, group_df in injection_df.groupby("Machine ID")
        }

        # 加入時間戳記
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"注塑排程結果_{timestamp}.xlsx"

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            for machine_id, df in machine_groups.items():
                df.to_excel(writer, sheet_name=str(machine_id), index=False)
        output.seek(0)

        st.download_button(
            label="下載排程結果 Excel 檔案",
            data=output,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ---------- 彩印排程頁籤 ----------
with tabs[1]:
    st.header("【彩印排程】")
    solve_time_printing = st.number_input("彩印排程求解時間上限 (秒)", min_value=10, max_value=1000000, value=60,
                                          step=10, key="solve_time_printing")
    if st.button("開始彩印排程", key="solve_printing"):
        if st.session_state["injection_results"] is None:
            st.error("請先完成注塑排程！")
        else:
            pending_job_ids, done_df, prev_state_printing = get_pending_job_ids_and_previous_df_and_machine_state(
                "彩印", baseline_date, join_date)
            jobs_pending = get_jobs(pending_job_ids)
            printing_result, printing_finish_times, printing_records = solve_printing(
                jobs_pending, printing_available, prev_state_printing, setup_params, solve_time_printing,
                SPECIAL_PRINT_CODES
            )
            station = "Printing"
            if printing_result:
                printing_df = pd.DataFrame(printing_records).sort_values(by=["Machine ID", "Proc Start"])
                # 新增注塑開始與注塑結束欄位
                printing_df["彩印開始"] = printing_df["Proc Start"].apply(
                    lambda x: baseline_date + timedelta(minutes=x))
                printing_df["彩印結束"] = printing_df["Proc End"].apply(lambda x: baseline_date + timedelta(minutes=x))
                # 新增交期欄位（在 Due Date 旁邊）
                if "Due Date" in printing_df.columns:
                    due_date_idx = printing_df.columns.get_loc("Due Date")
                    printing_df.insert(due_date_idx + 1, "交期", printing_df["Due Date"].apply(
                        lambda x: baseline_date + timedelta(minutes=x) if pd.notnull(x) else None))
                # 將欄位插入至 Proc End 後方
                cols = printing_df.columns.tolist()
                insert_index = cols.index("Proc End") + 1
                cols.insert(insert_index, cols.pop(cols.index("彩印開始")))
                cols.insert(insert_index + 1, cols.pop(cols.index("彩印結束")))
                printing_df = printing_df[cols]
                # printing_df["Machine ID"] = printing_df["Machine ID"].astype(str)
                updated_rows = []
                for machine_id, group in printing_df.groupby("Machine ID"):
                    group = group.sort_values("Proc Start").reset_index(drop=True)

                    # 計算 Setup Time
                    group["Setup Time"] = [0] + [
                        compute_setup_time_for_df("Printing", group.iloc[i - 1], group.iloc[i], setup_params)
                        for i in range(1, len(group))
                    ]

                    # 計算 Idle Time
                    group["Idle Time"] = [0] + [
                        max(0, group.loc[i, "Proc Start"] - (group.loc[i - 1, "Proc End"] + group.loc[i, "Setup Time"]))
                        for i in range(1, len(group))
                    ]

                    updated_rows.append(group)

                df_with_setup = pd.concat(updated_rows, ignore_index=True)
                # 計算 Processing Time 欄位（如尚未存在）
                if 'Processing Time' not in df_with_setup.columns:
                    df_with_setup['Processing Time'] = df_with_setup['Proc End'] - df_with_setup['Proc Start']
                # 將 Setup Time 插入到 Proc Start 前面，Idle Time 放在 Setup Time 後
                cols = df_with_setup.columns.tolist()
                cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Processing Time")))
                cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Setup Time")))
                cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Idle Time")))
                df_with_setup = df_with_setup[cols]
                # 將 Machine ID 欄位統一轉為字串以避免排序錯誤
                df_with_setup["Machine ID"] = df_with_setup["Machine ID"].astype(str)

                st.session_state["printing_results"] = {
                    "result": printing_result,
                    "records": printing_records,
                    "df": printing_df,
                    "intervals": df_with_setup
                }
                save_schedule_df("彩印", df_with_setup)
                save_performance("彩印", printing_result)
            else:
                st.error("彩印排程未找到可行解！")

    # 顯示彩印結果（若有）
    if st.session_state["printing_results"] is not None:
        from io import BytesIO
        from datetime import datetime

        station = "Printing"
        st.write("【彩印績效】", st.session_state.printing_results["result"])
        st.write("【彩印排程 DataFrame】")
        st.dataframe(st.session_state.printing_results["intervals"])
        st.write("【彩印甘特圖】")
        st.pyplot(plot_gantt_chart(st.session_state.printing_results["intervals"],
                                   title="Printing Gantt Chart with Setup & Idle Time By Minute"))
        st.pyplot(plot_gantt_chart_datetime(st.session_state["printing_results"]["intervals"], baseline_date,
                                            title="Printing Gantt Chart with Setup & Idle Time By Date"))

        # === 加入下載功能 ===
        st.write("### 📥 彩印排程 Excel")

        printing_df = st.session_state.printing_results["intervals"]
        machine_groups = {
            machine_id: group_df.sort_values(by="Proc Start").reset_index(drop=True)
            for machine_id, group_df in printing_df.groupby("Machine ID")
        }

        # 加入時間戳記
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"彩印排程結果_{timestamp}.xlsx"

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            for machine_id, df in machine_groups.items():
                df.to_excel(writer, sheet_name=str(machine_id), index=False)
        output.seek(0)

        st.download_button(
            label="下載排程結果 Excel 檔案",
            data=output,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ---------- 充填排程頁籤 ----------
with tabs[2]:
    st.header("【充填排程】")
    solve_time_filling = st.number_input("充填排程求解時間上限 (秒)", min_value=10, max_value=1000000, value=60,
                                         step=10, key="solve_time_filling")
    if st.button("開始充填排程", key="solve_filling"):
        if st.session_state["printing_results"] is None:
            st.error("請先完成彩印排程！")
        pending_job_ids, done_df, prev_state_filling = get_pending_job_ids_and_previous_df_and_machine_state("充填",
                                                                                                             baseline_date,
                                                                                                             join_date)
        jobs_pending = get_jobs(pending_job_ids)
        filling_result, filling_finish_times, filling_records = solve_filling(
            jobs_pending, filling_available, prev_state_filling, setup_params, solve_time_filling, big_machines,
            special_filling_codes
        )
        if filling_result:
            station = "Filling"
            filling_df = pd.DataFrame(filling_records).sort_values(by=["Machine ID", "Proc Start"])
            # 新增注塑開始與注塑結束欄位
            filling_df["充填開始"] = filling_df["Proc Start"].apply(lambda x: baseline_date + timedelta(minutes=x))
            filling_df["充填結束"] = filling_df["Proc End"].apply(lambda x: baseline_date + timedelta(minutes=x))
            # 新增交期欄位（在 Due Date 旁邊）
            if "Due Date" in filling_df.columns:
                due_date_idx = filling_df.columns.get_loc("Due Date")
                filling_df.insert(due_date_idx + 1, "交期", filling_df["Due Date"].apply(
                    lambda x: baseline_date + timedelta(minutes=x) if pd.notnull(x) else None))
            # 將欄位插入至 Proc End 後方
            cols = filling_df.columns.tolist()
            insert_index = cols.index("Proc End") + 1
            cols.insert(insert_index, cols.pop(cols.index("充填開始")))
            cols.insert(insert_index + 1, cols.pop(cols.index("充填結束")))
            filling_df = filling_df[cols]
            updated_rows = []
            for machine_id, group in filling_df.groupby("Machine ID"):
                group = group.sort_values("Proc Start").reset_index(drop=True)

                # 計算 Setup Time
                group["Setup Time"] = [0] + [
                    compute_setup_time_for_df("Filling", group.iloc[i - 1], group.iloc[i], setup_params)
                    for i in range(1, len(group))
                ]

                # 計算 Idle Time
                group["Idle Time"] = [0] + [
                    max(0, group.loc[i, "Proc Start"] - (group.loc[i - 1, "Proc End"] + group.loc[i, "Setup Time"]))
                    for i in range(1, len(group))
                ]

                updated_rows.append(group)

            df_with_setup = pd.concat(updated_rows, ignore_index=True)
            # 計算 Processing Time 欄位（如尚未存在）
            if 'Processing Time' not in df_with_setup.columns:
                df_with_setup['Processing Time'] = df_with_setup['Proc End'] - df_with_setup['Proc Start']
            # 將 Setup Time 插入到 Proc Start 前面，Idle Time 放在 Setup Time 後
            cols = df_with_setup.columns.tolist()
            cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Processing Time")))
            cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Setup Time")))
            cols.insert(cols.index("Proc Start"), cols.pop(cols.index("Idle Time")))
            df_with_setup = df_with_setup[cols]
            # 將 Machine ID 欄位統一轉為字串以避免排序錯誤
            df_with_setup["Machine ID"] = df_with_setup["Machine ID"].astype(str)

            st.session_state["filling_results"] = {
                "result": filling_result,
                "records": filling_records,
                "df": filling_df,
                "intervals": df_with_setup
            }
            save_schedule_df("充填", df_with_setup)
            save_performance("充填", filling_result)
        else:
            st.error("充填排程未找到可行解！")
    # 顯示充填結果（若有）
    if st.session_state["filling_results"] is not None:
        station = "Filling"
        st.write("【充填績效】", st.session_state["filling_results"]["result"])
        st.write("【充填排程 DataFrame】")
        st.dataframe(st.session_state["filling_results"]["intervals"])
        st.write("【充填甘特圖】")
        st.pyplot(plot_gantt_chart(st.session_state["filling_results"]["intervals"],
                                   title="Filling Gantt Chart with Setup & Idle Time By Minute"))
        st.pyplot(plot_gantt_chart_datetime(st.session_state["filling_results"]["intervals"], baseline_date,
                                            title="Filling Gantt Chart with Setup & Idle Time By Date"))
        # === 加入下載功能 ===
        st.write("### 📥 充填排程 Excel")

        injection_df = st.session_state.injection_results["intervals"]
        machine_groups = {
            machine_id: group_df.sort_values(by="Proc Start").reset_index(drop=True)
            for machine_id, group_df in injection_df.groupby("Machine ID")
        }

        # 加入時間戳記
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"充填排程結果_{timestamp}.xlsx"

        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            for machine_id, df in machine_groups.items():
                df.to_excel(writer, sheet_name=str(machine_id), index=False)
        output.seek(0)

        st.download_button(
            label="下載排程結果 Excel 檔案",
            data=output,
            file_name=filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# ----------   烘箱、裁切、入庫排程頁籤 ----------
with tabs[3]:
    st.header("【烘箱、裁切、入庫排程】")
    if st.button("開始烘箱、裁切、入庫排程", key="solve_oven_cut"):
        if st.session_state["filling_results"] is None:
            st.error("請先完成彩印排程！")
        all_df, injection_dict, printing_dict, filling_dict = convert_json_to_station_dict(baseline_date, setup_params)
        merged_timings_df = merge_station_timings_with_info(all_df)
        final_df = process_oven_cut_inspection(merged_timings_df)
        st.session_state["oven_cut_results"] = {
            "df": final_df
        }
        st.write("【開始烘箱、裁切、入庫排程 DataFrame】")
        st.dataframe(st.session_state["oven_cut_results"]["df"])


def clear_temp_folder(folder_name, base_path="暫存資料夾"):
    dir_path = os.path.join(base_path, folder_name)
    if not os.path.exists(dir_path):
        print(f"[略過] 找不到資料夾：{dir_path}")
        return
    for filename in os.listdir(dir_path):
        file_path = os.path.join(dir_path, filename)
        if os.path.isfile(file_path):
            try:
                os.remove(file_path)
                print(f"[已刪除] {file_path}")
            except Exception as e:
                print(f"[錯誤] 無法刪除 {file_path}：{e}")
        else:
            print(f"[略過] 非檔案（可能是資料夾）：{file_path}")

def save_schedule_df(station, df):
    today = datetime.now().strftime("%Y%m%d")
    temp_path = os.path.join("暫存資料夾", station, f"{station}_排程.xlsx")
    record_path = os.path.join("排程紀錄資料夾", station, f"{station}_{today}_排程.xlsx")
    df.to_excel(temp_path, index=False)
    df.to_excel(record_path, index=False)

def save_performance(station, perf):
    temp_file = os.path.join("暫存資料夾", station, f"{station}_績效.json")
    with open(temp_file, "w", encoding="utf-8") as f:
        json.dump(perf, f, ensure_ascii=False, indent=2)

def load_performance(station):
    temp_file = os.path.join("暫存資料夾", station, f"{station}_績效.json")
    if os.path.exists(temp_file):
        with open(temp_file, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return None

def load_schedule_df(station):
    temp_path = os.path.join("暫存資料夾", station, f"{station}_排程.xlsx")
    if os.path.exists(temp_path):
        return pd.read_excel(temp_path)
    else:
        return None

def save_machine_status(station, status_dict):
    status_file = os.path.join("暫存資料夾", station, f"{station}_機台狀態.json")
    with open(status_file, "w", encoding="utf-8") as f:
        json.dump(status_dict, f, ensure_ascii=False, indent=2)

def default_converter(o):
    if isinstance(o, (np.int64, np.int32)):
        return int(o)
    if isinstance(o, (np.float64, np.float32)):
        return float(o)
    raise TypeError(f"Object of type {o.__class__.__name__} is not JSON serializable")

def save_job_id_mapping(jobs, output_filename="job_id_mapping.json"):
    mapping = {job["job_id"]: job for job in jobs}
    output_path = os.path.join("暫存資料夾", "總排程", output_filename)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(mapping, f, ensure_ascii=False, indent=2, default=default_converter)
    print("Job ID Mapping 已儲存到", output_path)

def load_machine_status(station):
    status_file = os.path.join("暫存資料夾", station, f"{station}_機台狀態.json")
    if os.path.exists(status_file):
        with open(status_file, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        return {}

def compute_machine_status(schedule_records):
    status = {}
    for rec in schedule_records:
        machine = rec["Machine ID"]
        finish_time = rec["Proc End"]
        station = rec["Station"]
        if station == "Injection":
            recipe = {"combination": rec.get("組合編號", ""), "batch": rec.get("批次", "")}
        elif station == "Printing":
            recipe = {"print_code": rec.get("圖型碼", ""), "tone": rec.get("TONE", "")}
        elif station == "Filling":
            recipe = {"order_code": rec.get("工單號", ""), "batch": rec.get("批次", "")}
        else:
            recipe = None
        batch_no = rec.get("批號", "")
        if machine in status:
            if finish_time > status[machine]["finish_time"]:
                status[machine] = {"finish_time": finish_time, "recipe": recipe, "batch": batch_no}
        else:
            status[machine] = {"finish_time": finish_time, "recipe": recipe, "batch": batch_no}
    return status

# =============================================================================
# ✅ 提供「入口函式」讓 app.py 可以呼叫
#    - app.py 會把上傳的 Excel 路徑傳進來：main(uploaded_path)
#    - 你可以在這裡接你的真正排程流程（例如 schedule_all(uploaded_path)）
# =============================================================================
def main(path: str | None = None):
    """
    雲端執行入口：
    - 有給 path（Excel 檔）就先讀一個簡單 DataFrame 回傳（可視化用）
    - 你可把真正的排程流程接在這裡，並回傳 DataFrame 或 list[DataFrame]
    """
    if path is None:
        return "app0822.main() OK（未提供檔案路徑）"

    # 範例：讀 Excel 的第一個工作表，回傳前 100 列（避免檔太大）
    try:
        df = pd.read_excel(path)
    except Exception as e:
        return f"讀取 Excel 失敗：{e}"

    # TODO: 在這裡接上你的實際流程，例如：
    # result_df = schedule_all(path)  # 假設你有這隻
    # save_schedule_df("注塑", result_df)
    # return result_df

    return df.head(100)  # 先回傳可視化測試用
