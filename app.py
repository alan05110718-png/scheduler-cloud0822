# app.py
import sys, streamlit as st
st.sidebar.info(f"Python: {sys.version}")

import os
import io
import time
import inspect
from datetime import datetime
from typing import List, Tuple, Dict

import streamlit as st
import pandas as pd

# === 這行很重要：匯入你的現成程式 ===
import app0822 as core  # 確保同資料夾有 app0822.py


# 監看檔案變動：抓出新檔或被覆蓋更新的檔案（用修改時間判斷）
def snapshot_tree(root: str) -> Dict[str, float]:
    snap = {}
    for base, _, files in os.walk(root):
        for fn in files:
            p = os.path.join(base, fn)
            try:
                snap[p] = os.path.getmtime(p)
            except Exception:
                pass
    return snap


def diff_tree(before: Dict[str, float], after: Dict[str, float]) -> List[str]:
    out = []
    for p, mt in after.items():
        if (p not in before) or (mt > before[p] + 1e-6):
            out.append(p)
    return sorted(out)


# 嘗試尋找可當入口的函式，依常見命名順位
CANDIDATE_FUNCS = [
    "main", "run", "app", "schedule_all", "run_scheduler",
    "pipeline", "execute", "start", "solve_injection"
]


def find_entrypoints(module) -> List[str]:
    found = []
    for name in CANDIDATE_FUNCS:
        if hasattr(module, name) and callable(getattr(module, name)):
            found.append(name)
    return found


def call_entrypoint(func, uploaded_path: str):
    """
    依函式簽章智慧呼叫：
    - 0 參數：直接呼叫
    - 1 參數：傳入 uploaded_path
    - 多於 1 參數：顯示提示，不硬調
    並把回傳物整理成可展示/下載的結果
    """
    sig = inspect.signature(func)
    params = sig.parameters
    ret = None
    if len(params) == 0:
        ret = func()
    elif len(params) == 1:
        ret = func(uploaded_path)
    else:
        st.warning(f"偵測到入口函式 `{func.__name__}` 需要 {len(params)} 個參數，我目前只會傳 0 或 1 個參數。請在 app0822.py 包一層只收 0/1 參數的入口函式。")
        return None

    # 嘗試把回傳結果以可視化方式呈現
    # 1) DataFrame 或 list[DataFrame]
    if isinstance(ret, pd.DataFrame):
        st.subheader("執行結果（DataFrame）")
        st.dataframe(ret)
        _offer_df_download(ret, "result.xlsx")
    elif isinstance(ret, list) and ret and all(isinstance(x, pd.DataFrame) for x in ret):
        st.subheader("執行結果（多個 DataFrame）")
        for i, df in enumerate(ret, 1):
            st.markdown(f"**表格 {i}**")
            st.dataframe(df)
        # 打包成一個 Excel 多工作表
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
            for i, df in enumerate(ret, 1):
                df.to_excel(writer, sheet_name=f"Sheet{i}", index=False)
        st.download_button("下載所有表格（Excel）", bio.getvalue(), file_name="results.xlsx")
    # 2) 其他型態僅顯示
    else:
        if ret is not None:
            st.subheader("執行回傳（原樣顯示）")
            st.write(ret)

    return ret


def _offer_df_download(df: pd.DataFrame, filename: str):
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="xlsxwriter") as writer:
        df.to_excel(writer, index=False)
    st.download_button("下載結果（Excel）", bio.getvalue(), file_name=filename)


# === Streamlit UI ===
st.set_page_config(page_title="排程雲端執行器", layout="wide")
st.title("📦 Excel → 一鍵執行 app0822.py → 下載結果")

with st.sidebar:
    st.header("設定")
    save_dir = st.text_input("上傳儲存資料夾", value="uploaded")
    watch_dirs = st.text_input("監看輸出資料夾（以逗號分隔）",
                               value="暫存資料夾,排程紀錄資料夾")
    st.caption("說明：執行前後比對這些資料夾，列出新產生/覆蓋的檔案供下載。")

os.makedirs(save_dir, exist_ok=True)
for d in [x.strip() for x in watch_dirs.split(",") if x.strip()]:
    os.makedirs(d, exist_ok=True)

uploaded = st.file_uploader("上傳 Excel 檔", type=["xlsx", "xls"])
entrypoints = find_entrypoints(core)

with st.expander("偵測到的入口函式", expanded=True):
    if entrypoints:
        st.write("依優先序：", entrypoints)
    else:
        st.error("在 app0822.py 裡找不到常見的入口函式（如 main/run/schedule_all）。\n請在 app0822.py 增加一個例如 `def main(path=None): ...` 的薄包裝。")

chosen = None
if entrypoints:
    chosen = st.selectbox("選擇要呼叫的入口函式", entrypoints)

run_btn = st.button("🚀 開始執行", type="primary", disabled=(chosen is None or uploaded is None))

if run_btn and uploaded is not None and chosen is not None:
    # 將使用者上傳存到本機
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    uploaded_path = os.path.join(save_dir, f"input_{ts}.xlsx")
    with open(uploaded_path, "wb") as f:
        f.write(uploaded.read())
    st.success(f"已儲存上傳檔：{uploaded_path}")

    # 執行前快照
    dirs_to_watch = [x.strip() for x in watch_dirs.split(",") if x.strip()]
    before = {}
    for d in dirs_to_watch:
        before.update(snapshot_tree(d))

    # 呼叫入口
    func = getattr(core, chosen)
    with st.spinner(f"執行 `{chosen}` 中，請稍候…"):
        try:
            ret = call_entrypoint(func, uploaded_path)
        except Exception as e:
            st.exception(e)
            ret = None

    # 執行後快照
    time.sleep(0.5)  # 讓檔案系統寫入穩定
    after = {}
    for d in dirs_to_watch:
        after.update(snapshot_tree(d))
    new_files = diff_tree(before, after)

    st.subheader("🗂 新產出/更新的檔案")
    if not new_files:
        st.info("未偵測到新檔或更新檔。若你的程式會把結果存到別的資料夾，請在左側『監看輸出資料夾』加上該路徑。")
    else:
        for p in new_files:
            st.write(p)
            try:
                with open(p, "rb") as f:
                    data = f.read()
                dl_name = os.path.basename(p)
                st.download_button(f"下載：{dl_name}", data, file_name=dl_name)
            except Exception as e:
                st.warning(f"無法提供下載（{p}）：{e}")
