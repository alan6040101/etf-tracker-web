import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import os
import re
import glob
import shutil
import subprocess
import warnings

# ---------------- 設定頁面 ----------------
st.set_page_config(page_title="00981a 戰情室", layout="wide")
warnings.filterwarnings("ignore")

# ---------------- 0. 資料處理核心函式 ----------------

def extract_date_from_filename(filename):
    digits = re.sub(r'\D', '', filename)
    match8 = re.search(r'20\d{6}', digits)
    if match8: return match8.group(0)
    return None

def clean_number(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
    s = str(val).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
    try:
        return float(s)
    except:
        return 0.0

def format_money_label(val):
    if val is None: return "0"
    abs_val = abs(val)
    if abs_val >= 100000000:
        return f"{val/100000000:,.2f}億"
    elif abs_val >= 10000:
        return f"{val/10000:,.2f}萬"
    return f"{int(round(val)):,}"

@st.cache_data(ttl=3600)
def sync_00981a_data():
    """只同步 00981a 的資料"""
    repo_url = "https://github.com/alan6040101/00981a-data.git"
    dir_name = "data_00981a"
    
    if os.path.exists(dir_name):
        try:
            shutil.rmtree(dir_name)
        except: pass
    
    try:
        subprocess.run(["git", "clone", repo_url, dir_name, "-q"], check=True)
    except:
        return pd.DataFrame() # Return empty if failed

    files = glob.glob(f"{dir_name}/**/*.xlsx", recursive=True)
    local_list = []
    for f in files:
        if os.path.basename(f).startswith("~$"): continue
        d_str = extract_date_from_filename(f)
        if d_str:
            local_list.append({'date': d_str, 'path': f})
    
    df = pd.DataFrame(local_list)
    if not df.empty:
        df = df.sort_values('date', ascending=False).reset_index(drop=True)
    return df

def read_excel_holdings(path):
    """讀取單一 Excel 檔案並標準化欄位"""
    try:
        df_raw = pd.read_excel(path, header=None, nrows=30)
    except: return pd.DataFrame()

    target_header_idx = -1
    for idx, row in df_raw.iterrows():
        row_str = "".join([str(x) for x in row.fillna("")])
        if '代號' in row_str and '名稱' in row_str:
            target_header_idx = idx
            break
    
    if target_header_idx == -1: return pd.DataFrame()

    df = pd.read_excel(path, header=target_header_idx)
    df = df.dropna(how='all', axis=1)
    
    # 簡易欄位對應
    col_map = {}
    for c in df.columns:
        c_str = str(c).strip()
        if '代號' in c_str or 'Code' in c_str: col_map[c] = 'ID'
        elif '名稱' in c_str or 'Name' in c_str: col_map[c] = 'Name'
        elif '股數' in c_str or 'Shares' in c_str: col_map[c] = 'Shares'
        elif '權重' in c_str or 'Weight' in c_str: col_map[c] = 'Weight'
    
    df = df.rename(columns=col_map)
    required = ['ID', 'Name', 'Shares']
    if not all(k in df.columns for k in required): return pd.DataFrame()

    df['ID'] = df['ID'].astype(str).str.replace(r'\.TW|\.TWO', '', regex=True).str.strip()
    df['Shares'] = df['Shares'].apply(lambda x: clean_number(x))
    
    if 'Weight' in df.columns:
        df['Weight_Val'] = df['Weight'].apply(lambda x: clean_number(x))
    else:
        df['Weight_Val'] = 0.0

    return df[['ID', 'Name', 'Shares', 'Weight_Val']]

@st.cache_data(ttl=3600)
def get_all_stocks_history(all_sids):
    """一次下載所有涉及股票的歷史股價，用於計算成本線"""
    if not all_sids: return pd.DataFrame()
    tickers = [f"{sid}.TW" for sid in all_sids]
    # 下載過去 2 年數據以確保覆蓋
    data = yf.download(tickers, period="2y", group_by='ticker', auto_adjust=True, threads=True, progress=False)
    return data

def calculate_cost_line(history_files, target_sid, price_df):
    """計算特定股票的移動平均成本線"""
    # 1. 整理每日持股
    records = []
    # history_files 是依照日期 舊 -> 新 排序 (需要反轉輸入的列表，因為輸入通常是 新->舊)
    sorted_files = history_files.sort_values('date', ascending=True)
    
    for _, row in sorted_files.iterrows():
        df_h = read_excel_holdings(row['path'])
        if df_h.empty: shares = 0
        else:
            match = df_h[df_h['ID'] == target_sid]
            shares = match['Shares'].values[0] if not match.empty else 0
        records.append({'Date': pd.to_datetime(row['date']), 'Shares': shares})
    
    df_holdings = pd.DataFrame(records).set_index('Date')
    
    # 2. 結合股價計算成本
    if price_df.empty: return [], [], df_holdings
    
    # 確保索引是 DateTime
    price_df.index = pd.to_datetime(price_df.index)
    
    # 對齊數據
    combined = pd.concat([df_holdings, price_df['Close']], axis=1, join='outer')
    combined = combined.sort_index().ffill().dropna() # 簡單填補
    
    # 計算邏輯
    cost_line = []
    avg_cost = 0.0
    total_cost = 0.0
    
    # 只需要計算有持股變化的日子，但為了畫圖需要每日數據
    # 這裡簡化：只計算有持股紀錄的日子
    
    # 重新採樣對齊到日線
    aligned_shares = df_holdings.reindex(price_df.index, method='ffill').fillna(0)['Shares']
    aligned_diff = aligned_shares.diff().fillna(0)
    
    dates = price_df.index
    closes = price_df['Close']
    
    costs = []
    
    current_shares = 0
    
    for d, price, share, diff in zip(dates, closes, aligned_shares, aligned_diff):
        if share <= 0:
            avg_cost = 0
            total_cost = 0
            costs.append(None)
        else:
            if current_shares == 0:
                # 初始建立部位
                avg_cost = price
                total_cost = share * price
            else:
                if diff > 0: # 買進
                    total_cost += diff * price
                elif diff < 0: # 賣出
                    total_cost += diff * avg_cost # 賣出不影響平均成本
            
            if share > 0:
                avg_cost = total_cost / share
            else:
                avg_cost = 0
            
            costs.append(avg_cost)
            
        current_shares = share

    return costs, dates, aligned_shares, aligned_diff

# ---------------- 功能 1: 總覽 ----------------

def render_overview(file_list):
    st.header("📈 00981a 總覽")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("00981.TW 近一年走勢")
        etf_price = yf.download("00981.TW", period="1y", interval="1d", progress=False, auto_adjust=True)
        if not etf_price.empty:
            fig = go.Figure(data=[go.Candlestick(x=etf_price.index,
                            open=etf_price['Open'], high=etf_price['High'],
                            low=etf_price['Low'], close=etf_price['Close'])])
            fig.update_layout(height=400, xaxis_rangeslider_visible=False, template="plotly_white", margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("無法取得 00981.TW 股價資料")

    # 取得最新持股
    latest_file = file_list.iloc[0]
    st.write(f"📅 最新持股日期: **{latest_file['date']}**")
    
    df_holdings = read_excel_holdings(latest_file['path'])
    
    if df_holdings.empty:
        st.error("無法讀取最新持股資料")
        return

    # 取得現價與計算簡單成本 (這裡為了效能，我們只抓今天的價格做比較，成本先用 "估算" 或省略)
    # 若要精準成本，需要跑全歷史回測，會很久。這裡我們做「最新持股清單」
    
    with st.spinner("正在取得成分股即時報價..."):
        sids = df_holdings['ID'].tolist()
        tickers = [f"{sid}.TW" for sid in sids]
        current_data = yf.download(tickers, period="5d", group_by='ticker', auto_adjust=True, progress=False)
        
    # 整理表格
    table_data = []
    
    # 為了計算成本，我們需要跑歷史 loop (耗時)，或者我們先顯示基本資訊
    # 這裡我們做一個快速的「目前持股列表」
    
    for _, row in df_holdings.iterrows():
        sid = row['ID']
        ticker = f"{sid}.TW"
        
        # 取得現價
        current_price = 0
        try:
            # yfinance 格式處理
            if len(tickers) == 1: df_p = current_data
            else: df_p = current_data[ticker]
            
            if not df_p.empty:
                current_price = df_p['Close'].iloc[-1]
        except: pass
        
        table_data.append({
            '代號': sid,
            '名稱': row['Name'],
            '股數': row['Shares'],
            '權重(%)': row['Weight_Val'],
            '現價': current_price,
            '市值': row['Shares'] * current_price
        })
        
    df_table = pd.DataFrame(table_data)
    
    # 這裡如果要實作「低於成本」，需要成本資料。
    # 為了讓功能完整，我們嘗試計算「全部歷史成本」給「目前持有的股票」
    # 這可能需要一點時間
    
    with st.spinner("正在計算持股成本線 (需回溯歷史資料)..."):
        # 抓取所有歷史股價
        all_hist_price = get_all_stocks_history(df_table['代號'].unique().tolist())
        
        costs = []
        for sid in df_table['代號']:
            # 針對每一檔股票跑回測
            ticker = f"{sid}.TW"
            p_df = all_hist_price[ticker] if len(df_table) > 1 else all_hist_price
            
            # 簡化版：只取最後一天的成本
            c_line, _, _, _ = calculate_cost_line(file_list, sid, p_df)
            if c_line and c_line[-1] is not None:
                costs.append(c_line[-1])
            else:
                costs.append(0)
        
        df_table['估算成本'] = costs
    
    # 計算乖離率
    df_table['損益率(%)'] = df_table.apply(lambda x: ((x['現價'] - x['估算成本']) / x['估算成本'] * 100) if x['估算成本'] > 0 else 0, axis=1)
    
    # 篩選低於成本
    below_cost = df_table[df_table['損益率(%)'] < 0].sort_values('損益率(%)')

    with col2:
        st.subheader("⚠️ 股價低於成本警示")
        st.metric("低於成本檔數", f"{len(below_cost)} 檔")
        if not below_cost.empty:
            st.dataframe(below_cost[['代號', '名稱', '現價', '估算成本', '損益率(%)']], hide_index=True, use_container_width=True)
        else:
            st.success("目前無持股低於成本！")

    st.subheader("📋 最新完整持股清單")
    
    # 格式化顯示
    st.dataframe(
        df_table.style.format({
            "股數": "{:,.0f}", 
            "權重(%)": "{:.2f}", 
            "現價": "{:.2f}", 
            "市值": "{:,.0f}",
            "估算成本": "{:.2f}",
            "損益率(%)": "{:.2f}"
        }).background_gradient(subset=['損益率(%)'], cmap='RdYlGn', vmin=-10, vmax=10),
        use_container_width=True,
        hide_index=True
    )

# ---------------- 功能 2: 每日持倉變化 ----------------

def render_daily_change(file_list):
    st.header("📅 每日持倉變化分析")
    
    # 1. 日期選單
    dates = file_list['date'].tolist()
    
    col_sel, col_info = st.columns([1, 3])
    with col_sel:
        selected_date = st.selectbox("選擇日期", dates)
    
    if not selected_date: return

    # 找出當日與前一日檔案
    idx = dates.index(selected_date)
    curr_file = file_list.iloc[idx]
    
    prev_file = None
    if idx + 1 < len(file_list):
        prev_file = file_list.iloc[idx+1]
        
    with col_info:
        if prev_file is not None:
            st.info(f"比較區間: **{prev_file['date']}** (前一交易日) ⮕ **{selected_date}** (當日)")
        else:
            st.warning("這是最早的資料，無法進行比較。")
            return

    # 讀取兩份 Excel
    df_curr = read_excel_holdings(curr_file['path'])
    df_prev = read_excel_holdings(prev_file['path'])
    
    # 合併比較
    merged = pd.merge(df_prev[['ID', 'Name', 'Shares']], 
                      df_curr[['ID', 'Name', 'Shares', 'Weight_Val']], 
                      on='ID', how='outer', suffixes=('_old', '_new'))
    
    merged['Name'] = merged['Name_new'].combine_first(merged['Name_old']).fillna("未知")
    merged = merged.fillna(0)
    merged['股數變化'] = merged['Shares_new'] - merged['Shares_old']
    
    # 取得當日收盤價來計算金額
    sids_changed = merged[merged['股數變化'] != 0]['ID'].tolist()
    
    price_map = {}
    if sids_changed:
        with st.spinner("正在下載變動個股當日股價..."):
            target_dt = pd.to_datetime(selected_date)
            tickers = [f"{s}.TW" for s in sids_changed]
            # 下載前後幾天確保有資料
            data = yf.download(tickers, start=target_dt - timedelta(days=5), end=target_dt + timedelta(days=3), group_by='ticker', auto_adjust=True, progress=False)
            
            for sid in sids_changed:
                ticker = f"{sid}.TW"
                try:
                    p_df = data[ticker] if len(tickers) > 1 else data
                    # 找最接近 selected_date 的股價
                    p_df = p_df[p_df.index <= target_dt]
                    if not p_df.empty:
                        price_map[sid] = p_df['Close'].iloc[-1]
                    else:
                        price_map[sid] = 0
                except:
                    price_map[sid] = 0
    
    merged['當日股價'] = merged['ID'].map(price_map).fillna(0)
    merged['增減金額'] = merged['股數變化'] * merged['當日股價']
    
    # 篩選有變動的
    df_changes = merged[merged['股數變化'] != 0].copy()
    df_changes['增減金額字串'] = df_changes['增減金額'].apply(format_money_label)
    
    # 顯示變動表格
    st.subheader("📊 增減明細表")
    if not df_changes.empty:
        df_show = df_changes[['ID', 'Name', 'Shares_old', 'Shares_new', '股數變化', '增減金額', '增減金額字串', 'Weight_Val']]
        df_show.columns = ['代號', '名稱', '前股數', '今股數', '股數變化', '增減金額(數值)', '增減金額', '今日權重(%)']
        
        st.dataframe(
            df_show.style.format({
                '前股數': '{:,.0f}', '今股數': '{:,.0f}', '股數變化': '{:,.0f}', '今日權重(%)': '{:.2f}'
            }).background_gradient(subset=['股數變化'], cmap='RdYlGn'),
            use_container_width=True
        )
        
        # --- 個股詳細圖表 ---
        st.divider()
        st.subheader("📈 個股變動詳情 (K線 + 成本線)")
        
        # 下拉選單選擇要畫圖的股票
        selected_stock = st.selectbox(
            "請選擇一檔變動股票查看詳情:", 
            df_changes['ID'].tolist(),
            format_func=lambda x: f"{x} {df_changes[df_changes['ID']==x]['Name'].values[0]}"
        )
        
        if selected_stock:
            stock_name = df_changes[df_changes['ID']==selected_stock]['Name'].values[0]
            
            # 下載該股歷史資料 (為了畫圖)
            with st.spinner(f"正在繪製 {stock_name} 圖表..."):
                t_sid = f"{selected_stock}.TW"
                df_price = yf.download(t_sid, period="2y", auto_adjust=True, progress=False)
                
                # 計算成本線與持股水位
                cost_vals, dates, shares_vals, diff_vals = calculate_cost_line(file_list, selected_stock, df_price)
                
                # 計算金額柱狀圖
                amounts = diff_vals * df_price['Close']
                
                # 準備繪圖數據
                # 裁切一下範圍，不用看太久，看最近半年即可，或者看有資料的區間
                
                fig = make_subplots(
                    rows=3, cols=1, shared_xaxes=True, 
                    vertical_spacing=0.05,
                    row_heights=[0.5, 0.25, 0.25],
                    subplot_titles=(f"{stock_name} 股價 vs 00981a持有成本", "00981a 持股水位", "每日增減金額")
                )
                
                # 1. K線 + 成本線
                fig.add_trace(go.Candlestick(x=dates, open=df_price['Open'], high=df_price['High'],
                                            low=df_price['Low'], close=df_price['Close'], name="股價"), row=1, col=1)
                
                fig.add_trace(go.Scatter(x=dates, y=cost_vals, mode='lines', 
                                         line=dict(color='orange', width=2, dash='dot'), name="ETF成本線"), row=1, col=1)
                
                # 2. 持股水位
                fig.add_trace(go.Scatter(x=dates, y=shares_vals, mode='lines', fill='tozeroy',
                                         line=dict(color='blue'), name="持股數"), row=2, col=1)
                
                # 3. 增減金額
                colors = ['red' if v > 0 else 'green' for v in amounts]
                fig.add_trace(go.Bar(x=dates, y=amounts, marker_color=colors, name="增減金額"), row=3, col=1)
                
                fig.update_layout(height=800, xaxis_rangeslider_visible=False, showlegend=False, template="plotly_white")
                
                # 設定顯示範圍為最近 6 個月
                last_date = dates[-1]
                start_date = last_date - timedelta(days=180)
                fig.update_xaxes(range=[start_date, last_date], row=3, col=1)

                st.plotly_chart(fig, use_container_width=True)

    else:
        st.info("該日持股無任何變動。")

# ---------------- 主程式 ----------------

def main():
    st.sidebar.title("🚀 00981a 戰情室")
    
    with st.sidebar:
        st.write("資料同步中...")
        df_files = sync_00981a_data()
        
        if df_files.empty:
            st.error("無法取得資料，請檢查網路或 GitHub 來源。")
            return
        
        st.success(f"資料已更新 (共 {len(df_files)} 筆)")
        
        menu = st.radio("功能選單", ["總覽 (Overview)", "每日持倉變化 (Daily Changes)"])
        st.markdown("---")
        st.caption("Designed for ETF Tracking")

    if menu == "總覽 (Overview)":
        render_overview(df_files)
    elif menu == "每日持倉變化 (Daily Changes)":
        render_daily_change(df_files)

if __name__ == "__main__":
    main()
