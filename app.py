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

# ---------------------------------------------------------
# 設定頁面配置
# ---------------------------------------------------------
st.set_page_config(page_title="00981a ETF 追蹤戰情室", layout="wide")

# ---------------------------------------------------------
# 1. 工具函式 (資料同步與解析)
# ---------------------------------------------------------

@st.cache_data(ttl=3600)  # 設定快取 1 小時，避免頻繁 git clone
def sync_data_repo():
    """同步 GitHub 資料庫"""
    repo_url = "https://github.com/alan6040101/00981a-data.git"
    dir_name = "data_00981a"
    
    if os.path.exists(dir_name):
        shutil.rmtree(dir_name)
    
    # 這裡使用 os.system 來執行 git clone，Streamlit cloud 支援 git 指令
    os.system(f"git clone {repo_url} {dir_name} -q")
    
    files = glob.glob(f"{dir_name}/**/*.xlsx", recursive=True)
    local_list = []
    
    for f in files:
        if os.path.basename(f).startswith("~$"): continue
        digits = re.sub(r'\D', '', os.path.basename(f))
        
        # 解析日期
        d_str = None
        match8 = re.search(r'20\d{6}', digits)
        if match8: 
            d_str = match8.group(0)
        else:
            match7 = re.search(r'(1\d{2})(\d{2})(\d{2})', digits)
            if match7:
                year = int(match7.group(1)) + 1911
                d_str = f"{year}{match7.group(2)}{match7.group(3)}"
        
        if d_str:
            local_list.append({'date': pd.to_datetime(d_str), 'path': f})
            
    df_files = pd.DataFrame(local_list).sort_values('date').reset_index(drop=True)
    return df_files

def clean_number(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
    s = str(val).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
    try:
        return float(s)
    except:
        return 0.0

def parse_excel_holding(path):
    """讀取單一 Excel 並標準化欄位"""
    try:
        df_raw = pd.read_excel(path, header=None, nrows=30)
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

    mapping = {
        'ID': ['代號', 'ID', 'Code'],
        'Name': ['名稱', 'Name', 'Security'],
        'Shares': ['股數', 'Shares', 'Units'],
        'Weight': ['權重', 'Weight', '%']
    }

    new_cols = []
    found = {}
    for col in df.columns:
        c_name = str(col).strip()
        mapped_name = c_name
        for target, keys in mapping.items():
            if target not in found and any(k in c_name for k in keys):
                mapped_name = target
                found[target] = True
                break
        new_cols.append(mapped_name)
    df.columns = new_cols

    if 'ID' in df.columns:
        df['ID'] = df['ID'].astype(str).str.replace(r'\.0|\.TW|\.TWO|\*', '', regex=True).str.strip()
        if 'Name' not in df.columns: df['Name'] = df['ID']
        
        if 'Shares' in df.columns:
            df['Shares'] = df['Shares'].apply(lambda x: clean_number(x))
        else: df['Shares'] = 0.0
            
        if 'Weight' in df.columns:
            df['Weight'] = df['Weight'].apply(lambda x: clean_number(x))
        else: df['Weight'] = 0.0
        
        return df[['ID', 'Name', 'Shares', 'Weight']]
    
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_stock_price_history(ticker, start_date):
    """取得股價歷史 (含快取)"""
    try:
        df = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        return df
    except:
        return pd.DataFrame()

def calculate_avg_cost(df_files, target_sid, price_df):
    """計算特定股票的移動平均成本線"""
    # 1. 抓取該股票在每一天的持股數
    history_data = []
    # 為了效能，我們不每次都開檔，而是假設 df_files 已經排序
    # 注意：這裡若檔案很多，逐個開檔會慢，但在 Streamlit 中我們可以接受幾秒的等待
    for _, row in df_files.iterrows():
        df_step = parse_excel_holding(row['path'])
        shares = 0
        if not df_step.empty:
            match = df_step[df_step['ID'] == str(target_sid)]
            if not match.empty:
                shares = match['Shares'].values[0]
        history_data.append({'Date': row['date'], 'Shares': shares})
    
    df_h = pd.DataFrame(history_data).sort_values('Date').set_index('Date')
    
    # 2. 對齊股價日期
    if price_df.empty: return [], [], []
    
    # 合併持股與股價 (以股價交易日為主)
    df_calc = price_df[['Close']].join(df_h, how='left')
    df_calc['Shares'] = df_calc['Shares'].ffill().fillna(0)
    df_calc['Diff'] = df_calc['Shares'].diff().fillna(0)
    
    # 3. 計算成本
    cost_line = []
    total_cost = 0.0
    avg_cost = 0.0
    
    shares_series = df_calc['Shares'].values
    diff_series = df_calc['Diff'].values
    close_series = df_calc['Close'].values
    
    for s, d, p in zip(shares_series, diff_series, close_series):
        if s <= 0:
            avg_cost = 0.0
            total_cost = 0.0
            cost_line.append(None)
        else:
            # 初始建倉 或 加碼
            if total_cost == 0 and avg_cost == 0:
                avg_cost = p
                total_cost = s * p
            else:
                if d > 0: # 買進，更新成本
                    total_cost += d * p
                elif d < 0: # 賣出，成本單價不變，總成本減少
                    total_cost += d * avg_cost
            
            if s > 0:
                avg_cost = total_cost / s
            else:
                avg_cost = 0.0
            
            cost_line.append(avg_cost)
            
    return df_calc.index, cost_line, shares_series, diff_series

# ---------------------------------------------------------
# 2. 頁面邏輯
# ---------------------------------------------------------

st.title("📊 00981a 追蹤戰情室")

# 下載資料
with st.spinner('正在同步 GitHub 資料...'):
    df_files = sync_data_repo()

if df_files.empty:
    st.error("找不到資料檔，請確認 GitHub 連結是否正確。")
    st.stop()

# 側邊欄選單
menu = st.sidebar.radio("功能選單", ["總覽 (Dashboard)", "每日持倉變化"])

latest_date_record = df_files.iloc[-1]['date']
latest_path = df_files.iloc[-1]['path']
st.sidebar.info(f"資料庫最新日期: {latest_date_record.strftime('%Y-%m-%d')}")

# =========================================================
# 頁面 A: 總覽 (Dashboard)
# =========================================================
if menu == "總覽 (Dashboard)":
    st.header("📈 00981a 總覽")
    
    # 1. 00981 ETF 本身走勢
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("00981.TW 近一年走勢")
        end_d = datetime.now()
        start_d = end_d - timedelta(days=365)
        df_etf = get_stock_price_history("00981.TW", start_d)
        
        if not df_etf.empty:
            fig = go.Figure(data=[go.Candlestick(
                x=df_etf.index,
                open=df_etf['Open'], high=df_etf['High'],
                low=df_etf['Low'], close=df_etf['Close']
            )])
            fig.update_layout(height=400, margin=dict(l=20, r=20, t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("無法取得 00981.TW 股價資料")

    # 讀取最新持股
    df_latest = parse_excel_holding(latest_path)
    
    # 準備計算 "低於成本" 的股票 (這需要一點時間，因為要跑迴圈算成本)
    st.subheader("⚠️ 潛在雷區 (現價 < 981建倉成本)")
    
    if st.button("計算持股盈虧分析 (需時較長)"):
        report_data = []
        progress_bar = st.progress(0)
        
        # 取得所有成分股代號
        sids = df_latest['ID'].tolist()
        
        # 批次下載這一年股價以加速
        tickers = [f"{sid}.TW" for sid in sids]
        # 為了處理 .TWO，這邊做簡單判斷 (或是直接全部都試)
        # 簡化起見，先假設都是 .TW，實務上可混合下載
        
        with st.spinner("正在下載成分股股價與計算成本..."):
            # 下載全部股價 (快取)
            bulk_data = yf.download(tickers, start=start_d, group_by='ticker', progress=False, auto_adjust=True)
            
            for i, row in enumerate(df_latest.itertuples()):
                sid = row.ID
                name = row.Name
                current_shares = row.Shares
                
                # 取得該股股價 df
                try:
                    if len(tickers) > 1:
                        df_p = bulk_data[f"{sid}.TW"]
                    else:
                        df_p = bulk_data # 只有一檔時結構不同
                    
                    df_p = df_p.dropna()
                except:
                    df_p = pd.DataFrame()

                if not df_p.empty:
                    # 計算成本
                    _, cost_line, _, _ = calculate_avg_cost(df_files, sid, df_p)
                    
                    current_price = df_p['Close'].iloc[-1]
                    current_cost = cost_line[-1] if cost_line and cost_line[-1] is not None else 0
                    
                    if current_cost > 0:
                        diff_pct = (current_price - current_cost) / current_cost * 100
                        if diff_pct < 0: # 虧損中
                            report_data.append({
                                "代號": sid,
                                "名稱": name,
                                "現價": round(current_price, 2),
                                "981成本": round(current_cost, 2),
                                "帳面損益 (%)": round(diff_pct, 2),
                                "目前持股": f"{int(current_shares):,}"
                            })
                
                progress_bar.progress((i + 1) / len(df_latest))
        
        df_underwater = pd.DataFrame(report_data)
        if not df_underwater.empty:
            df_underwater = df_underwater.sort_values("帳面損益 (%)")
            st.dataframe(
                df_underwater.style.format({
                    "現價": "{:.2f}", 
                    "981成本": "{:.2f}",
                    "帳面損益 (%)": "{:.2f}%"
                }).applymap(lambda v: 'color: green' if v < 0 else 'color: red', subset=['帳面損益 (%)']),
                use_container_width=True
            )
        else:
            st.success("目前沒有持股低於成本價！")
    else:
        st.info("點擊按鈕開始計算每檔成分股的成本與現價比較。")

    # 顯示最新持股清單
    st.subheader(f"📋 最新持股清單 ({latest_date_record.strftime('%Y-%m-%d')})")
    st.dataframe(df_latest, use_container_width=True)

# =========================================================
# 頁面 B: 每日持倉變化
# =========================================================
elif menu == "每日持倉變化":
    st.header("📅 每日持倉變化分析")
    
    # 1. 日期選擇器
    col_date, col_dummy = st.columns([1, 3])
    with col_date:
        # 預設選最新日期
        default_date = latest_date_record.date()
        pick_date = st.date_input("選擇查詢日期", default_date)
        pick_date_ts = pd.to_datetime(pick_date)

    # 2. 尋找 當日 與 前一日
    # 在 df_files 中找
    curr_record = df_files[df_files['date'] == pick_date_ts]
    
    if curr_record.empty:
        st.warning(f"資料庫中沒有 {pick_date} 的資料。")
        st.stop()
        
    curr_idx = curr_record.index[0]
    prev_idx = curr_idx - 1
    
    path_curr = curr_record.iloc[0]['path']
    
    if prev_idx < 0:
        st.warning("這是資料庫的第一天，無法比較變化。")
        df_curr = parse_excel_holding(path_curr)
        st.dataframe(df_curr)
        st.stop()
        
    path_prev = df_files.iloc[prev_idx]['path']
    date_prev = df_files.iloc[prev_idx]['date']
    
    st.write(f"正在比較: **{pick_date}** vs **{date_prev.strftime('%Y-%m-%d')}**")
    
    # 3. 讀取並合併
    df_t = parse_excel_holding(path_curr)
    df_y = parse_excel_holding(path_prev)
    
    m = pd.merge(df_y[['ID', 'Name', 'Shares']], 
                 df_t[['ID', 'Name', 'Shares', 'Weight']], 
                 on='ID', how='outer', suffixes=('_old', '_new'))
    
    m['Name'] = m['Name_new'].combine_first(m['Name_old']).fillna("未知")
    m = m.fillna(0)
    m['股數變化'] = m['Shares_new'] - m['Shares_old']
    
    # 篩選變動股
    df_change = m[m['股數變化'] != 0].copy()
    
    # 4. 估算增減金額 (需要當日股價)
    st.subheader("💰 增減金額估算")
    if df_change.empty:
        st.info("今日持股無變化。")
    else:
        # 下載變動股的當日收盤價
        change_sids = df_change['ID'].tolist()
        tickers_change = [f"{sid}.TW" for sid in change_sids]
        
        price_map = {}
        with st.spinner("下載變動股股價中..."):
            # 下載這兩天的資料即可，取最新
            dl_start = pick_date_ts - timedelta(days=5) 
            dl_end = pick_date_ts + timedelta(days=1)
            try:
                df_prices = yf.download(tickers_change, start=dl_start, end=dl_end, group_by='ticker', progress=False, auto_adjust=True)
                
                for sid in change_sids:
                    ticker = f"{sid}.TW"
                    try:
                        if len(change_sids) > 1:
                            p_data = df_prices[ticker]
                        else:
                            p_data = df_prices
                        
                        # 找 pick_date 當天或最接近的一天
                        p_at_date = p_data[p_data.index <= pick_date_ts].iloc[-1]['Close']
                        price_map[sid] = p_at_date
                    except:
                        price_map[sid] = 0
            except:
                pass
        
        df_change['參考股價'] = df_change['ID'].map(price_map)
        df_change['估算金額'] = df_change['股數變化'] * df_change['參考股價']
        
        # 格式化顯示
        df_show = df_change[['ID', 'Name', 'Shares_old', 'Shares_new', '股數變化', '參考股價', '估算金額', 'Weight']].sort_values('估算金額', ascending=False)
        
        st.dataframe(
            df_show.style.format({
                'Shares_old': "{:,.0f}", 'Shares_new': "{:,.0f}", '股數變化': "{:,.0f}",
                '參考股價': "{:.2f}", '估算金額': "{:,.0f}", 'Weight': "{:.2f}"
            }).bar(subset=['股數變化', '估算金額'], align='mid', color=['#d65f5f', '#5fba7d']),
            use_container_width=True
        )

    # 5. 個股互動 K 線圖 (含成本)
    st.divider()
    st.subheader("📈 變動個股技術分析 (含成本線)")
    
    if not df_change.empty:
        # 下拉選單選擇股票
        selected_stock_label = st.selectbox(
            "選擇要查看的股票:", 
            options=df_change['ID'] + " " + df_change['Name']
        )
        
        if selected_stock_label:
            target_sid = selected_stock_label.split(" ")[0]
            
            # 抓取該股完整歷史股價 (1年) 以繪圖
            chart_start = datetime.now() - timedelta(days=365)
            df_chart_price = get_stock_price_history(f"{target_sid}.TW", chart_start)
            
            if not df_chart_price.empty:
                # 計算成本線
                dates, cost_line, shares_series, diff_series = calculate_avg_cost(df_files, target_sid, df_chart_price)
                
                # 計算金額柱狀圖
                amounts = diff_series * df_chart_price['Close'].values
                
                # 繪圖
                fig = make_subplots(rows=3, cols=1, shared_xaxes=True, 
                                    row_heights=[0.6, 0.2, 0.2], vertical_spacing=0.05,
                                    subplot_titles=("股價 & 成本", "持股水位", "增減金額"))
                
                # Row 1: K線 + 成本
                fig.add_trace(go.Candlestick(x=dates, open=df_chart_price['Open'], high=df_chart_price['High'],
                                             low=df_chart_price['Low'], close=df_chart_price['Close'], name='股價'), row=1, col=1)
                
                fig.add_trace(go.Scatter(x=dates, y=cost_line, mode='lines', 
                                         line=dict(color='orange', width=2, dash='dot'), name='981成本'), row=1, col=1)
                
                # Row 2: 持股數
                fig.add_trace(go.Scatter(x=dates, y=shares_series, mode='lines+markers',
                                         fill='tozeroy', line=dict(color='blue'), name='持股數'), row=2, col=1)
                
                # Row 3: 增減金額
                colors = ['red' if x > 0 else 'green' for x in amounts]
                fig.add_trace(go.Bar(x=dates, y=amounts, marker_color=colors, name='淨買賣額'), row=3, col=1)
                
                fig.update_layout(height=800, xaxis_rangeslider_visible=False, title_text=f"{selected_stock_label} 交易分析")
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.error("無法下載股價資料")
