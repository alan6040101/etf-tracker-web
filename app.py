import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta, date
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
    if val is None or pd.isna(val): return "0"
    abs_val = abs(val)
    if abs_val >= 100000000:
        return f"{val/100000000:,.2f}億"
    elif abs_val >= 10000:
        return f"{val/10000:,.2f}萬"
    return f"{int(round(val)):,}"

@st.cache_data(ttl=300)
def sync_00981a_data():
    """只同步 00981a 的資料"""
    repo_url = "https://github.com/alan6040101/00981a-data.git"
    dir_name = "data_00981a"
    
    # 嘗試刪除舊資料，如果失敗(權限問題)則忽略，直接用 git pull 更新
    if os.path.exists(dir_name):
        try:
            shutil.rmtree(dir_name)
        except: pass
    
    try:
        if not os.path.exists(dir_name):
            subprocess.run(["git", "clone", repo_url, dir_name, "-q"], check=True)
        else:
            # 如果資料夾存在，嘗試 pull
            subprocess.run(["git", "-C", dir_name, "pull", "-q"], check=False)
    except:
        pass # 忽略錯誤，嘗試讀取現有檔案

    files = glob.glob(f"{dir_name}/**/*.xlsx", recursive=True)
    local_list = []
    for f in files:
        if os.path.basename(f).startswith("~$"): continue
        d_str = extract_date_from_filename(f)
        if d_str:
            local_list.append({'date': d_str, 'path': f})
    
    df = pd.DataFrame(local_list)
    if not df.empty:
        # 確保日期排序正確
        df = df.sort_values('date', ascending=False).reset_index(drop=True)
    return df

def read_excel_holdings(path):
    """讀取單一 Excel 檔案並標準化欄位 - 增強版"""
    try:
        # 先讀前幾行找 Header
        df_raw = pd.read_excel(path, header=None, nrows=20)
    except Exception as e:
        return pd.DataFrame()

    target_header_idx = -1
    # 尋找包含關鍵字的列作為 Header
    for idx, row in df_raw.iterrows():
        row_str = "".join([str(x) for x in row.fillna("")])
        # 寬鬆匹配：只要有 '名稱' 和 ('股數' 或 'Shares' 或 '單位數')
        if ('名稱' in row_str or 'Name' in row_str) and \
           ('股數' in row_str or 'Shares' in row_str or '單位數' in row_str or 'Units' in row_str):
            target_header_idx = idx
            break
    
    if target_header_idx == -1:
        # 如果找不到，嘗試預設第一行
        target_header_idx = 0

    try:
        df = pd.read_excel(path, header=target_header_idx)
    except:
        return pd.DataFrame()

    df = df.dropna(how='all', axis=1)
    
    # 建立欄位對應字典
    col_map = {}
    for c in df.columns:
        c_str = str(c).strip()
        if any(x in c_str for x in ['代號', 'Code', 'ID']): col_map[c] = 'ID'
        elif any(x in c_str for x in ['名稱', 'Name', 'Security']): col_map[c] = 'Name'
        elif any(x in c_str for x in ['股數', 'Shares', 'Units', '單位數']): col_map[c] = 'Shares'
        elif any(x in c_str for x in ['權重', 'Weight', '%']): col_map[c] = 'Weight'
    
    df = df.rename(columns=col_map)
    
    # 檢查必要欄位
    if 'ID' not in df.columns or 'Shares' not in df.columns:
        return pd.DataFrame()

    # 資料清理
    df['ID'] = df['ID'].astype(str).str.replace(r'\.TW|\.TWO', '', regex=True).str.strip()
    # 處理股數：轉為數值，若失敗設為0
    df['Shares'] = df['Shares'].apply(lambda x: clean_number(x))
    
    if 'Weight' in df.columns:
        df['Weight_Val'] = df['Weight'].apply(lambda x: clean_number(x))
    else:
        df['Weight_Val'] = 0.0
        
    if 'Name' not in df.columns:
        df['Name'] = df['ID']

    return df[['ID', 'Name', 'Shares', 'Weight_Val']]

@st.cache_data(ttl=3600)
def get_all_stocks_history(all_sids):
    """一次下載歷史股價"""
    if not all_sids: return pd.DataFrame()
    tickers = [f"{sid}.TW" for sid in all_sids]
    # 下載較長區間以確保有足夠資料計算成本
    try:
        data = yf.download(tickers, period="1y", group_by='ticker', auto_adjust=True, threads=True, progress=False)
        return data
    except:
        return pd.DataFrame()

def calculate_cost_line(history_files, target_sid, price_df):
    """計算特定股票的移動平均成本線"""
    # 必須按照時間 舊 -> 新 排序
    sorted_files = history_files.sort_values('date', ascending=True)
    
    records = []
    for _, row in sorted_files.iterrows():
        df_h = read_excel_holdings(row['path'])
        shares = 0
        if not df_h.empty:
            match = df_h[df_h['ID'] == target_sid]
            if not match.empty:
                shares = match['Shares'].values[0]
        records.append({'Date': pd.to_datetime(row['date']), 'Shares': shares})
    
    df_holdings = pd.DataFrame(records).set_index('Date')
    
    if price_df.empty: 
        return [], [], [], []
    
    # 確保 Index 為 Datetime
    price_df.index = pd.to_datetime(price_df.index)
    
    # 合併持股與股價 (對齊日期)
    # 使用 reindex 對齊到股價的交易日
    aligned_shares = df_holdings.reindex(price_df.index, method='ffill').fillna(0)['Shares']
    aligned_diff = aligned_shares.diff().fillna(0)
    
    dates = price_df.index
    closes = price_df['Close']
    
    costs = []
    avg_cost = 0.0
    total_cost = 0.0
    current_shares = 0
    
    for price, share, diff in zip(closes, aligned_shares, aligned_diff):
        if share <= 10: # 視為清空
            avg_cost = 0
            total_cost = 0
            costs.append(None)
            current_shares = 0
        else:
            if current_shares <= 10: # 重新建倉
                avg_cost = price
                total_cost = share * price
            else:
                if diff > 0: # 買進，更新成本
                    total_cost += diff * price
                elif diff < 0: # 賣出，成本不變，總成本減少
                    total_cost += diff * avg_cost 
            
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
    
    if file_list.empty:
        st.error("無資料檔案")
        return

    col1, col2 = st.columns([2, 1])
    
    # 1. 00981a 股價圖
    with col1:
        st.subheader("00981.TW 近一年走勢")
        try:
            # 使用明確的代號 .TW
            etf_ticker = "00981.TW" 
            etf_price = yf.download(etf_ticker, period="1y", interval="1d", progress=False, auto_adjust=True)
            
            if not etf_price.empty:
                # 處理 MultiIndex Column 的情況 (yfinance 新版)
                if isinstance(etf_price.columns, pd.MultiIndex):
                    try:
                        etf_price = etf_price.xs(etf_ticker, axis=1, level=0)
                    except: pass # 如果失敗可能結構不同，直接用

                fig = go.Figure(data=[go.Candlestick(x=etf_price.index,
                                open=etf_price['Open'], high=etf_price['High'],
                                low=etf_price['Low'], close=etf_price['Close'])])
                fig.update_layout(height=400, xaxis_rangeslider_visible=False, template="plotly_white", margin=dict(l=20, r=20, t=20, b=20))
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.warning("無法從 Yahoo Finance 取得 00981.TW 數據，請稍後再試。")
        except Exception as e:
            st.error(f"股價讀取錯誤: {e}")

    # 2. 讀取最新持股
    latest_file = file_list.iloc[0]
    st.write(f"📅 最新持股日期: **{latest_file['date']}**")
    
    df_holdings = read_excel_holdings(latest_file['path'])
    
    if df_holdings.empty:
        st.error("⚠️ 無法讀取 Excel 持股資料，請檢查 GitHub 來源檔案格式。")
        return

    # 3. 準備表格資料與計算成本
    with st.spinner("正在取得成分股即時報價與計算成本..."):
        sids = df_holdings['ID'].tolist()
        
        # 取得歷史資料用於計算成本
        all_hist_price = get_all_stocks_history(sids)
        
        # 取得最新股價 (取歷史資料的最後一筆)
        current_prices = {}
        cost_map = {}
        
        for sid in sids:
            ticker = f"{sid}.TW"
            try:
                # 提取個股歷史
                if isinstance(all_hist_price.columns, pd.MultiIndex):
                    if ticker in all_hist_price.columns.levels[0]:
                        p_df = all_hist_price[ticker]
                    else:
                        p_df = pd.DataFrame()
                else:
                    p_df = all_hist_price # 只有一檔時
                
                if not p_df.empty:
                    # 1. 紀錄現價
                    current_prices[sid] = p_df['Close'].iloc[-1]
                    
                    # 2. 計算成本
                    c_line, _, _, _ = calculate_cost_line(file_list, sid, p_df)
                    # 取最後一個非 None 的成本
                    valid_costs = [c for c in c_line if c is not None]
                    cost_map[sid] = valid_costs[-1] if valid_costs else 0
                else:
                    current_prices[sid] = 0
                    cost_map[sid] = 0
            except:
                current_prices[sid] = 0
                cost_map[sid] = 0

    # 整合資料
    df_holdings['現價'] = df_holdings['ID'].map(current_prices)
    df_holdings['估算成本'] = df_holdings['ID'].map(cost_map)
    df_holdings['市值'] = df_holdings['Shares'] * df_holdings['現價']
    
    # 計算損益
    def calc_roi(row):
        if row['估算成本'] > 0:
            return (row['現價'] - row['估算成本']) / row['估算成本'] * 100
        return 0.0
    
    df_holdings['損益率(%)'] = df_holdings.apply(calc_roi, axis=1)

    # 4. 低於成本表格 (右側)
    with col2:
        below_cost = df_holdings[df_holdings['損益率(%)'] < 0].copy()
        below_cost = below_cost.sort_values('損益率(%)')
        
        st.subheader("⚠️ 股價低於成本警示")
        st.metric("低於成本檔數", f"{len(below_cost)} 檔")
        if not below_cost.empty:
            st.dataframe(
                below_cost[['代號', '名稱', '現價', '估算成本', '損益率(%)']].style.format({
                    '現價': '{:.2f}', '估算成本': '{:.2f}', '損益率(%)': '{:.2f}'
                }).map(lambda x: 'color: green', subset=['損益率(%)']), # 負數顯示綠色
                hide_index=True, 
                use_container_width=True
            )
        else:
            st.success("目前無持股低於成本！")

    # 5. 完整持股清單 (下方)
    st.subheader("📋 最新完整持股清單")
    
    # 排序：依權重
    df_holdings = df_holdings.sort_values('Weight_Val', ascending=False)
    
    st.dataframe(
        df_holdings[['ID', 'Name', 'Shares', 'Weight_Val', '現價', '估算成本', '損益率(%)', '市值']].style.format({
            "Shares": "{:,.0f}", 
            "Weight_Val": "{:.2f}", 
            "現價": "{:.2f}", 
            "市值": "{:,.0f}",
            "估算成本": "{:.2f}",
            "損益率(%)": "{:.2f}"
        }).background_gradient(subset=['損益率(%)'], cmap='RdYlGn', vmin=-10, vmax=10),
        use_container_width=True,
        hide_index=True,
        column_config={
            "ID": "代號", "Name": "名稱", "Shares": "股數", "Weight_Val": "權重(%)"
        }
    )

# ---------------- 功能 2: 每日持倉變化 ----------------

def render_daily_change(file_list):
    st.header("📅 每日持倉變化分析")
    
    # 日期選單 (Calendar)
    col_sel, col_info = st.columns([1, 3])
    
    # 取得可用日期列表 (YYYYMMDD 字串)
    available_dates_str = file_list['date'].tolist()
    # 轉為 date 物件供 date_input 使用
    available_dates_obj = [datetime.strptime(d, "%Y%m%d").date() for d in available_dates_str]
    
    if not available_dates_obj:
        st.error("無可用日期資料")
        return
        
    latest_date = available_dates_obj[0]
    
    with col_sel:
        # 預設選擇最新的一天
        input_date = st.date_input("選擇查詢日期", latest_date)
        # 轉回字串比對
        selected_date_str = input_date.strftime("%Y%m%d")
    
    if selected_date_str not in available_dates_str:
        st.warning(f"❌ {selected_date_str} 當日無資料。")
        st.info("可用日期: " + ", ".join(available_dates_str[:5]) + " ...")
        return

    # 邏輯處理
    idx = available_dates_str.index(selected_date_str)
    curr_file = file_list.iloc[idx]
    
    prev_file = None
    if idx + 1 < len(file_list):
        prev_file = file_list.iloc[idx+1]
        
    with col_info:
        if prev_file:
            st.info(f"比較區間: **{prev_file['date']}** (前一交易日) ⮕ **{curr_file['date']}** (當日)")
        else:
            st.warning("這是最早的資料，無法進行比較。")
            return

    # 讀取 Excel
    df_curr = read_excel_holdings(curr_file['path'])
    df_prev = read_excel_holdings(prev_file['path'])
    
    if df_curr.empty or df_prev.empty:
        st.error("讀取檔案失敗，可能是格式不符。")
        return
    
    # 合併比較
    merged = pd.merge(df_prev[['ID', 'Name', 'Shares']], 
                      df_curr[['ID', 'Name', 'Shares', 'Weight_Val']], 
                      on='ID', how='outer', suffixes=('_old', '_new'))
    
    merged['Name'] = merged['Name_new'].combine_first(merged['Name_old']).fillna("未知")
    merged = merged.fillna(0)
    merged['股數變化'] = merged['Shares_new'] - merged['Shares_old']
    
    # 下載股價計算金額
    sids_changed = merged[merged['股數變化'] != 0]['ID'].tolist()
    price_map = {}
    
    if sids_changed:
        with st.spinner("正在下載變動個股當日股價..."):
            target_dt = pd.to_datetime(selected_date_str)
            tickers = [f"{s}.TW" for s in sids_changed]
            try:
                data = yf.download(tickers, start=target_dt - timedelta(days=5), end=target_dt + timedelta(days=3), group_by='ticker', auto_adjust=True, progress=False)
                for sid in sids_changed:
                    ticker = f"{sid}.TW"
                    p_df = pd.DataFrame()
                    if isinstance(data.columns, pd.MultiIndex):
                        if ticker in data.columns.levels[0]: p_df = data[ticker]
                    else:
                        p_df = data
                    
                    p_df = p_df[p_df.index <= target_dt]
                    if not p_df.empty:
                        price_map[sid] = p_df['Close'].iloc[-1]
                    else:
                        price_map[sid] = 0
            except: pass
    
    merged['當日股價'] = merged['ID'].map(price_map).fillna(0)
    merged['增減金額'] = merged['股數變化'] * merged['當日股價']
    
    # 篩選變動
    df_changes = merged[merged['股數變化'] != 0].copy()
    
    # 樣式設定函數
    def color_change(val):
        if val > 0: return 'color: red'
        elif val < 0: return 'color: green'
        return ''

    st.subheader("📊 增減明細表")
    if not df_changes.empty:
        # 排序：權重由大到小
        df_changes = df_changes.sort_values('Weight_Val', ascending=False)
        
        df_show = df_changes[['ID', 'Name', 'Shares_old', 'Shares_new', '股數變化', '增減金額', 'Weight_Val']]
        
        st.dataframe(
            df_show.style.format({
                'Shares_old': '{:,.0f}', 'Shares_new': '{:,.0f}', 
                '股數變化': '{:+,.0f}', '增減金額': '{:,.0f}', 'Weight_Val': '{:.2f}'
            }).map(color_change, subset=['股數變化', '增減金額']),
            use_container_width=True,
            column_config={
                "Shares_old": "前股數", "Shares_new": "今股數", "Weight_Val": "今日權重(%)"
            }
        )
        
        # --- 個股詳細圖表 ---
        st.divider()
        st.subheader("📈 個股變動詳情 (K線 + 成本線)")
        
        # 下拉選單
        selected_stock = st.selectbox(
            "請選擇一檔變動股票查看詳情:", 
            df_changes['ID'].tolist(),
            format_func=lambda x: f"{x} {df_changes[df_changes['ID']==x]['Name'].values[0]}"
        )
        
        if selected_stock:
            stock_name = df_changes[df_changes['ID']==selected_stock]['Name'].values[0]
            
            with st.spinner(f"正在繪製 {stock_name} 圖表..."):
                t_sid = f"{selected_stock}.TW"
                df_price = yf.download(t_sid, period="1y", auto_adjust=True, progress=False)
                
                # 計算數據
                cost_vals, dates, shares_vals, diff_vals = calculate_cost_line(file_list, selected_stock, df_price)
                
                if not dates.empty:
                    # 修復 TypeError: 確保 amounts 是純數值序列，沒有 NaN
                    amounts = (diff_vals * df_price['Close']).fillna(0)
                    
                    fig = make_subplots(
                        rows=3, cols=1, shared_xaxes=True, 
                        vertical_spacing=0.05, row_heights=[0.5, 0.25, 0.25],
                        subplot_titles=(f"{stock_name} vs 成本", "00981a 持股水位", "每日增減金額")
                    )
                    
                    # 1. K線
                    fig.add_trace(go.Candlestick(x=dates, open=df_price['Open'], high=df_price['High'],
                                                low=df_price['Low'], close=df_price['Close'], name="股價"), row=1, col=1)
                    # 成本線
                    fig.add_trace(go.Scatter(x=dates, y=cost_vals, mode='lines', 
                                            line=dict(color='orange', width=2, dash='dot'), name="成本線"), row=1, col=1)
                    
                    # 2. 水位
                    fig.add_trace(go.Scatter(x=dates, y=shares_vals, mode='lines', fill='tozeroy',
                                            line=dict(color='blue'), name="持股數"), row=2, col=1)
                    
                    # 3. 金額 (修復顏色判斷錯誤)
                    # 轉為 list 避免 Series index 問題
                    amt_list = amounts.tolist()
                    colors = ['red' if v > 0 else 'green' for v in amt_list]
                    
                    fig.add_trace(go.Bar(x=dates, y=amounts, marker_color=colors, name="增減金額"), row=3, col=1)
                    
                    fig.update_layout(height=800, xaxis_rangeslider_visible=False, showlegend=False, template="plotly_white")
                    
                    # 顯示最近 6 個月
                    last_date = dates[-1]
                    start_date = last_date - timedelta(days=180)
                    fig.update_xaxes(range=[start_date, last_date], row=3, col=1)

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("無法取得該股價歷史資料，無法繪圖。")

    else:
        st.info("✅ 該日持股無任何變動。")

# ---------------- 主程式 ----------------

def main():
    st.sidebar.title("🚀 00981a 戰情室")
    
    with st.sidebar:
        st.write("資料同步中...")
        df_files = sync_00981a_data()
        
        if df_files.empty:
            st.error("⚠️ 無法取得資料，請檢查 GitHub 連結或網際網路。")
            return
        
        st.success(f"資料已更新 (共 {len(df_files)} 筆)")
        
        menu = st.radio("功能選單", ["總覽 (Overview)", "每日持倉變化 (Daily Changes)"])
        st.markdown("---")

    if menu == "總覽 (Overview)":
        render_overview(df_files)
    elif menu == "每日持倉變化 (Daily Changes)":
        render_daily_change(df_files)

if __name__ == "__main__":
    main()
