import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import os
import shutil
import glob
import re
import requests
import time

# ---------------------------------------------------------
# 設定頁面配置
# ---------------------------------------------------------
st.set_page_config(page_title="00981a ETF 追蹤器", layout="wide")

# ---------------------------------------------------------
# 1. 資料庫與解析核心
# ---------------------------------------------------------

@st.cache_data(ttl=3600)
def sync_data_repo():
    """同步 GitHub 資料庫"""
    repo_url = "https://github.com/alan6040101/00981a-data.git"
    dir_name = "data_00981a"
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)
    os.system(f"git clone {repo_url} {dir_name} -q")
    
    files = glob.glob(f"{dir_name}/**/*.xlsx", recursive=True)
    local_list = []
    for f in files:
        if os.path.basename(f).startswith("~$"): continue
        digits = re.sub(r'\D', '', os.path.basename(f))
        d_str = None
        match8 = re.search(r'20\d{6}', digits)
        if match8: d_str = match8.group(0)
        else:
            match7 = re.search(r'(1\d{2})(\d{2})(\d{2})', digits)
            if match7:
                year = int(match7.group(1)) + 1911
                d_str = f"{year}{match7.group(2)}{match7.group(3)}"
        if d_str:
            local_list.append({'date': pd.to_datetime(d_str), 'path': f})
    return pd.DataFrame(local_list).sort_values('date').reset_index(drop=True)

def clean_number(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
    s = str(val).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
    try: return float(s)
    except: return 0.0

def format_money_label(val):
    if val is None: return "0"
    abs_val = abs(val)
    if abs_val >= 100000000: return f"{val/100000000:,.2f}億"
    elif abs_val >= 10000: return f"{val/10000:,.2f}萬"
    return f"{int(round(val)):,}"

def parse_excel_holding(path):
    try: df_raw = pd.read_excel(path, header=None, nrows=30)
    except: return pd.DataFrame()

    target_header_idx = -1
    for idx, row in df_raw.iterrows():
        row_str = "".join([str(x) for x in row.fillna("")])
        if any(k in row_str for k in ['代號', 'Code']) and any(k in row_str for k in ['名稱', 'Name']):
            target_header_idx = idx
            break

    if target_header_idx == -1: return pd.DataFrame()

    df = pd.read_excel(path, header=target_header_idx)
    df = df.dropna(how='all', axis=1)

    mapping = {'ID': ['代號', 'ID', 'Code'], 'Name': ['名稱', 'Name', 'Security'],
               'Shares': ['股數', 'Shares', 'Units'], 'Weight': ['權重', 'Weight', '%']}
    new_cols = []
    found = {}
    for col in df.columns:
        c_name = str(col).strip()
        mapped_name = c_name
        for target, keys in mapping.items():
            if target not in found and any(k in c_name for k in keys):
                mapped_name = target; found[target] = True; break
        new_cols.append(mapped_name)
    df.columns = new_cols

    if 'ID' in df.columns:
        df['ID'] = df['ID'].astype(str).str.replace(r'\.0|\.TW|\.TWO|\*', '', regex=True).str.strip()
        if 'Name' not in df.columns: df['Name'] = df['ID']
        
        if 'Shares' in df.columns: df['Shares_num'] = df['Shares'].apply(clean_number)
        else: df['Shares_num'] = 0.0
            
        if 'Weight' in df.columns:
            df['Weight_str'] = df['Weight'].astype(str)
            df['Weight_num'] = df['Weight'].apply(clean_number)
        else: 
            df['Weight_str'] = "0%"; df['Weight_num'] = 0.0
        return df[['ID', 'Name', 'Shares_num', 'Weight_str', 'Weight_num']]
    return pd.DataFrame()

# ---------------------------------------------------------
# 2. 效能優化與智慧救援引擎 (V8)
# ---------------------------------------------------------

@st.cache_data(ttl=3600)
def get_all_holdings_history(_df_files):
    all_records = []
    for _, row in _df_files.iterrows():
        df_step = parse_excel_holding(row['path'])
        if not df_step.empty:
            df_step['Date'] = row['date']
            all_records.append(df_step[['Date', 'ID', 'Shares_num']])
    if all_records:
        return pd.concat(all_records, ignore_index=True)
    return pd.DataFrame()

# 【官方備援 API】專門向台灣證券交易所索取精確的月度股價
@st.cache_data(ttl=3600)
def fetch_twse_monthly(sid, year_month_str):
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={year_month_str}&stockNo={sid}"
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        if data.get('stat') == 'OK':
            records = []
            for row in data['data']:
                try:
                    tw_year = int(row[0].split('/')[0])
                    g_year = tw_year + 1911
                    date_str = f"{g_year}-{row[0].split('/')[1]}-{row[0].split('/')[2]}"
                    records.append({
                        'Date': pd.to_datetime(date_str),
                        'Open': clean_number(row[3]),
                        'High': clean_number(row[4]),
                        'Low': clean_number(row[5]),
                        'Close': clean_number(row[6])
                    })
                except: continue
            if records:
                df = pd.DataFrame(records).set_index('Date')
                if hasattr(df.index, 'tz') and df.index.tz is not None:
                    df.index = df.index.tz_localize(None)
                df.index = pd.to_datetime(df.index).normalize()
                return df
    except: pass
    return pd.DataFrame()

# 【終極修正 V8】Yahoo Finance 混搭 TWSE 官方救援防漏接引擎
@st.cache_data(ttl=3600)
def fetch_stock_data_v8(sids, start_dt, end_dt):
    price_map = {}
    if not sids: return price_map
    
    def clean_and_format_price_df(df):
        if df is None or df.empty: return None
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
            
        df = df.rename(columns=lambda x: str(x).capitalize() if str(x).lower() in ['close', 'open', 'high', 'low', 'volume'] else x)
        
        if 'Close' in df.columns:
            df = df.dropna(subset=['Close'])
            if df.empty: return None
            df['Open'] = df['Open'].fillna(df['Close'])
            df['High'] = df['High'].fillna(df['Close'])
            df['Low'] = df['Low'].fillna(df['Close'])
            if hasattr(df.index, 'tz') and df.index.tz is not None:
                df.index = df.index.tz_localize(None)
            df.index = pd.to_datetime(df.index).normalize()
            df = df[~df.index.duplicated(keep='last')]
            return df
        return None

    for sid in sids:
        df_combined = pd.DataFrame()
        
        # 第一線：向 Yahoo Finance 索取
        try:
            tkr_tw = yf.Ticker(f"{sid}.TW")
            raw_tw = tkr_tw.history(start=start_dt, end=end_dt)
            if raw_tw is None or raw_tw.empty:
                raw_tw = yf.download(f"{sid}.TW", start=start_dt, end=end_dt, progress=False)
            cl_tw = clean_and_format_price_df(raw_tw)
            if cl_tw is not None: df_combined = cl_tw
        except: pass
            
        try:
            tkr_two = yf.Ticker(f"{sid}.TWO")
            raw_two = tkr_two.history(start=start_dt, end=end_dt)
            if raw_two is None or raw_two.empty:
                raw_two = yf.download(f"{sid}.TWO", start=start_dt, end=end_dt, progress=False)
            cl_two = clean_and_format_price_df(raw_two)
            if cl_two is not None:
                df_combined = cl_two if df_combined.empty else df_combined.combine_first(cl_two)
        except: pass
        
        # 第二線：TWSE 官方救援機制 (修補 YF 的資料黑洞)
        clean_sid = str(sid).replace('.TW', '').replace('.TWO', '').strip()
        try:
            if df_combined.empty:
                # 狀況A: YF 完全死掉，向 TWSE 抓取最近三個月
                months = pd.date_range(start_dt.replace(day=1), end_dt, freq='MS')
                for m in months[-3:]:
                    df_twse = fetch_twse_monthly(clean_sid, m.strftime('%Y%m01'))
                    if not df_twse.empty:
                        df_twse = df_twse[df_twse['Close'] > 0]
                        df_combined = df_twse if df_combined.empty else df_combined.combine_first(df_twse)
                    time.sleep(0.1)
            else:
                # 狀況B: 智慧偵測 YF 的資料斷層 (大於 10 天的破洞，例如 Jan 2 到 Feb 10)
                df_combined = df_combined.sort_index()
                gaps = df_combined.index.to_series().diff()
                big_gaps = gaps[gaps > pd.Timedelta(days=10)]
                
                for gap_end in big_gaps.index:
                    gap_start = gap_end - gaps[gap_end]
                    # 抓出這個斷層涵蓋的所有月份，向 TWSE 討回真實資料
                    gap_months = pd.date_range(gap_start.replace(day=1), gap_end, freq='MS')
                    for m in gap_months:
                        df_twse = fetch_twse_monthly(clean_sid, m.strftime('%Y%m01'))
                        if not df_twse.empty:
                            df_twse = df_twse[df_twse['Close'] > 0]
                            df_combined = df_combined.combine_first(df_twse)
                        time.sleep(0.1)
        except: pass

        if not df_combined.empty:
            df_combined = df_combined.sort_index()
            price_map[sid] = df_combined

    return price_map

def extract_cash_weight(path):
    try:
        df_raw = pd.read_excel(path, header=None, nrows=20)
        for i in range(len(df_raw)):
            row_str = " ".join(df_raw.iloc[i].astype(str).fillna(""))
            if any(k in row_str for k in ["現金", "Cash", "TWD"]):
                for j in range(3):
                    if i + j < len(df_raw):
                        for cell in df_raw.iloc[i+j]:
                            if pd.isna(cell): continue
                            s = str(cell).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
                            matches = re.findall(r'[-+]?\d+\.?\d*', s)
                            for m in matches:
                                try:
                                    v = float(m)
                                    if v != 0 and -100 <= v <= 100: return v
                                except: pass
    except: pass
    return 0.0

@st.cache_data(ttl=3600)
def get_etf_cash_history(_df_files):
    history = []
    for _, row in _df_files.iterrows():
        history.append({'Date': pd.to_datetime(row['date']).normalize(), 'Cash_Weight': extract_cash_weight(row['path'])})
    return pd.DataFrame(history).set_index('Date')

# ---------------------------------------------------------
# 3. 核心邏輯與繪圖
# ---------------------------------------------------------

def calculate_avg_cost_optimized(df_history, target_sid, price_df):
    if price_df.empty or df_history.empty: return [], [], [], []
    
    df_stock = df_history[df_history['ID'] == str(target_sid)][['Date', 'Shares_num']].copy()
    df_h = df_stock.rename(columns={'Shares_num': 'Shares'}).sort_values('Date').
