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
# 設定頁面配置 (TradingView 專業風格)
# ---------------------------------------------------------
st.set_page_config(page_title="00981a ETF 追蹤器", layout="wide", initial_sidebar_state="expanded")

# 注入 TradingView 風格 CSS
st.markdown("""
<style>
    /* 全域背景與字體 */
    .stApp { background-color: #0E1117; color: #D1D4DC; }
    [data-testid="stSidebar"] { background-color: #131722; border-right: 1px solid #2B2B43; }
    
    /* 隱藏預設的頂部 padding */
    .block-container { padding-top: 2rem; }
    
    /* KPI 卡片風格 */
    div[data-testid="metric-container"] {
        background-color: #1E222D;
        border: 1px solid #2B2B43;
        border-radius: 8px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    }
    div[data-testid="metric-container"] > div { color: #8A93A6; font-size: 0.9rem; }
    div[data-testid="metric-container"] > div:nth-child(2) { color: #FFFFFF; font-size: 1.8rem; font-weight: bold; }
    
    /* Alert Center 紅色標題 */
    .alert-header {
        background-color: #D32F2F;
        color: white;
        padding: 10px 15px;
        font-weight: bold;
        border-radius: 8px 8px 0 0;
        font-size: 1.1rem;
        display: flex;
        align-items: center;
        margin-top: 20px;
    }
    .alert-content {
        background-color: #1E222D;
        border: 1px solid #D32F2F;
        border-top: none;
        border-radius: 0 0 8px 8px;
        padding: 15px;
    }
    
    /* DataFrame 表格風格覆寫 */
    .stDataFrame { background-color: #1E222D; border-radius: 8px; }
</style>
""", unsafe_allow_html=True)

# 定義 TradingView 的 Plotly 統一版型
tv_layout = dict(
    template="plotly_dark", paper_bgcolor="#1E222D", plot_bgcolor="#1E222D",
    font=dict(color="#8A93A6", size=12),
    xaxis=dict(showgrid=True, gridcolor="#2B2B43", linecolor="#2B2B43", zeroline=False),
    yaxis=dict(showgrid=True, gridcolor="#2B2B43", linecolor="#2B2B43", zeroline=False),
    margin=dict(l=10, r=10, t=40, b=10)
)

# ---------------------------------------------------------
# 1. 資料庫與解析核心 (保持不變)
# ---------------------------------------------------------

@st.cache_data(ttl=3600)
def sync_data_repo():
    repo_url = "https://github.com/alan6040101/00981a-data.git"
    dir_name = "data_00981a"
    if os.path.exists(dir_name): shutil.rmtree(dir_name)
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
        if d_str: local_list.append({'date': pd.to_datetime(d_str), 'path': f})
    return pd.DataFrame(local_list).sort_values('date').reset_index(drop=True)

def clean_number(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
    s = str(val).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
    try: return float(s)
    except: return 0.0

def parse_excel_holding(path):
    try: df_raw = pd.read_excel(path, header=None, nrows=30)
    except: return pd.DataFrame()
    target_header_idx = -1
    for idx, row in df_raw.iterrows():
        row_str = "".join([str(x) for x in row.fillna("")])
        if any(k in row_str for k in ['代號', 'Code']) and any(k in row_str for k in ['名稱', 'Name']):
            target_header_idx = idx; break
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
# 2. 效能優化引擎 (修復 API 漏包斷層)
# ---------------------------------------------------------

@st.cache_data(ttl=3600)
def get_all_holdings_history(_df_files):
    all_records = []
    for _, row in _df_files.iterrows():
        df_step = parse_excel_holding(row['path'])
        if not df_step.empty:
            df_step['Date'] = row['date']
            all_records.append(df_step[['Date', 'ID', 'Shares_num']])
    if all_records: return pd.concat(all_records, ignore_index=True)
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_bulk_prices(sids, start_dt, end_dt):
    """取得股價，並強制商業日曆對齊，修復 YF 斷層"""
    price_map = {}
    if not sids: return price_map
    
    tickers_tw = [f"{sid}.TW" for sid in sids]
    tickers_two = [f"{sid}.TWO" for sid in sids]
    
    def clean_and_format_price_df(s_data):
        if s_data is None or s_data.empty: return None
        s_data = s_data.rename(columns=lambda x: str(x).capitalize() if str(x).lower() in ['close', 'open', 'high', 'low', 'volume'] else x)
        if 'Close' in s_data.columns:
            s_data = s_data.dropna(subset=['Close'])
            if hasattr(s_data.index, 'tz') and s_data.index.tz is not None:
                s_data.index = s_data.index.tz_localize(None)
            s_data.index = pd.to_datetime(s_data.index).normalize()
            s_data = s_data[~s_data.index.duplicated(keep='last')]
            
            # 【根因修復核心】強制對齊商業日曆，填補 YF 漏包的空白 K 棒
            if not s_data.empty:
                full_idx = pd.bdate_range(start=s_data.index.min(), end=s_data.index.max())
                s_data = s_data.reindex(full_idx)
                s_data['Close'] = s_data['Close'].ffill()
                s_data['Open'] = s_data['Open'].fillna(s_data['Close'])
                s_data['High'] = s_data['High'].fillna(s_data['Close'])
                s_data['Low'] = s_data['Low'].fillna(s_data['Close'])
                s_data['Volume'] = s_data['Volume'].fillna(0)
                return s_data
        return None

    def extract_ticker(df_all, ticker):
        if df_all is None or df_all.empty: return pd.DataFrame()
        if isinstance(df_all.columns, pd.MultiIndex):
            if ticker in df_all.columns.get_level_values(1): return df_all.xs(ticker, level=1, axis=1).copy()
            return pd.DataFrame()
        return df_all.copy()

    df_tw = yf.download(tickers_tw, start=start_dt, end=end_dt, progress=False, auto_adjust=True)
    df_two = yf.download(tickers_two, start=start_dt, end=end_dt, progress=False, auto_adjust=True)
    
    for sid in sids:
        s_tw = extract_ticker(df_tw, f"{sid}.TW")
        s_two = extract_ticker(df_two, f"{sid}.TWO")
        
        cleaned_tw = clean_and_format_price_df(s_tw)
        cleaned_two = clean_and_format_price_df(s_two)
        
        if cleaned_tw is not None and cleaned_two is not None: price_map[sid] = cleaned_tw.combine_first(cleaned_two)
        elif cleaned_tw is not None: price_map[sid] = cleaned_tw
        elif cleaned_two is not None: price_map[sid] = cleaned_two

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
# 3. 技術分析與繪圖核心 (新增 RSI, MACD)
# ---------------------------------------------------------

def calc_rsi(series, period=14):
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calc_macd(series, fast=12, slow=26, signal=9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return macd, signal_line, histogram

def calculate_avg_cost_optimized(df_history, target_sid, price_df):
    if price_df.empty or df_history.empty: return [], [], [], []
    df_stock = df_history[df_history['ID'] == str(target_sid)][['Date', 'Shares_num']].copy()
    df_h = df_stock.rename(columns={'Shares_num': 'Shares'}).sort_values('Date').set_index('Date')
    
    df_calc = price_df[['Close']].join(df_h, how='left')
    df_calc['Shares'] = df_calc['Shares'].ffill().fillna(0)
    df_calc['Diff'] = df_calc['Shares'].diff().fillna(0)
    
    cost_line = []
    total_cost, avg_cost = 0.0, 0.0
    shares_series = df_calc['Shares'].values
    diff_series = df_calc['Diff'].values
    close_series = df_calc['Close'].values
    
    for s, d, p in zip(shares_series, diff_series, close_series):
        if s <= 0:
            avg_cost, total_cost = 0.0, 0.0
            cost_line.append(None)
        else:
            if total_cost == 0 and avg_cost == 0: avg_cost, total_cost = p, s * p
            else:
                if d > 0: total_cost += d * p
                elif d < 0: total_cost += d * avg_cost
            avg_cost = total_cost / s if s > 0 else 0.0
            cost_line.append(avg_cost)
            
    return df_calc.index, cost_line, shares_series, diff_series

def draw_ta_chart(sid, name, df_history):
    """繪製專業級技術分析圖 (K線/成本 + RSI + 成交量 + MACD)"""
    chart_start = datetime.now() - timedelta(days=365)
    price_map = get_bulk_prices([sid], chart_start, datetime.now() + timedelta(days=1))
    df_chart = price_map.get(sid, pd.DataFrame())
    
    if df_chart.empty:
        st.error(f"無法取得 {sid} 的股價資料")
        return

    dates, cost_line, shares_series, diff_series = calculate_avg_cost_optimized(df_history, sid, df_chart)
    str_dates = dates.strftime('%Y-%m-%d')
    
    # 計算技術指標
    df_chart['RSI'] = calc_rsi(df_chart['Close'])
    macd, macd_signal, macd_hist = calc_macd(df_chart['Close'])

    fig = make_subplots(
        rows=4, cols=1, shared_xaxes=True, 
        row_heights=[0.5, 0.15, 0.15, 0.2], vertical_spacing=0.03,
        subplot_titles=(f"{sid} {name}", "RSI (14)", "Volume", "MACD (12, 26, 9)")
    )
    
    # 1. 主圖：K線 + 成本線
    fig.add_trace(go.Candlestick(
        x=str_dates, open=df_chart['Open'], high=df_chart['High'], low=df_chart['Low'], close=df_chart['Close'],
        name='Price', increasing_line_color='#EF5350', decreasing_line_color='#26A69A'
    ), row=1, col=1)
    fig.add_trace(go.Scatter(x=str_dates, y=cost_line, mode='lines', line=dict(color='#FCCA46', width=2), name='Moving AVA Costs'), row=1, col=1)
    
    # 2. RSI
    fig.add_trace(go.Scatter(x=str_dates, y=df_chart['RSI'], mode='lines', line=dict(color='#B39DDB', width=1.5), name='RSI'), row=2, col=1)
    fig.add_hline(y=70, line_dash="dash", line_color="#424242", row=2, col=1)
    fig.add_hline(y=30, line_dash="dash", line_color="#424242", row=2, col=1)
    
    # 3. Volume
    vol_colors = ['#EF5350' if df_chart['Close'].iloc[i] >= df_chart['Open'].iloc[i] else '#26A69A' for i in range(len(df_chart))]
    fig.add_trace(go.Bar(x=str_dates, y=df_chart['Volume'], marker_color=vol_colors, name='Volume'), row=3, col=1)
    
    # 4. MACD
    fig.add_trace(go.Bar(x=str_dates, y=macd_hist, marker_color=['#EF5350' if val > 0 else '#26A69A' for val in macd_hist], name='Histogram'), row=4, col=1)
    fig.add_trace(go.Scatter(x=str_dates, y=macd, mode='lines', line=dict(color='#2962FF', width=1.5), name='MACD'), row=4, col=1)
    fig.add_trace(go.Scatter(x=str_dates, y=macd_signal, mode='lines', line=dict(color='#FF9800', width=1.5), name='Signal'), row=4, col=1)
    
    fig.update_layout(**tv_layout, height=850, xaxis_rangeslider_visible=False, showlegend=False)
    fig.update_xaxes(type='category', nticks=10) 
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------
# 4. 網頁主介面
# ---------------------------------------------------------

with st.spinner('正在同步資料庫...'):
    df_files = sync_data_repo()

if df_files.empty: st.error("找不到資料"); st.stop()
df_history_cache = get_all_holdings_history(df_files)
latest_date_record = df_files.iloc[-1]['date']
latest_path = df_files.iloc[-1]['path']

st.sidebar.title("📊 00981a Tracker")
st.sidebar.info(f"Latest: {latest_date_record.strftime('%Y-%m-%d')}")
menu = st.sidebar.radio("", ["總覽 (Dashboard)", "每日持倉變化"])

# =========================================================
# 頁面 A: 總覽 (Dashboard)
# =========================================================
if menu == "總覽 (Dashboard)":
    st.header("總覽")
    df_latest = parse_excel_holding(latest_path)
    
    # --- KPI 卡片區 ---
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    start_d = datetime.now() - timedelta(days=365)
    etf_price_map = get_bulk_prices(["00981A", "00981"], start_d, datetime.now() + timedelta(days=1))
    
    df_etf_A = etf_price_map.get("00981A", pd.DataFrame())
    df_etf_B = etf_price_map.get("00981", pd.DataFrame())
    df_etf = df_etf_A.combine_first(df_etf_B) if not df_etf_A.empty and not df_etf_B.empty else (df_etf_A if not df_etf_A.empty else df_etf_B)
    
    curr_nav = df_etf['Close'].iloc[-1] if not df_etf.empty else 0
    ytd_ret = ((curr_nav / df_etf['Close'].iloc[0]) - 1) * 100 if not df_etf.empty and df_etf['Close'].iloc[0] != 0 else 0
    
    with kpi1: st.metric("Total Assets (預估總資產)", "$1,490,130.38", delta=None) # Mock value for UI
    with kpi2: st.metric("NAV (淨值)", f"${curr_nav:.2f}", delta="0.85%")
    with kpi3: st.metric("Expense Ratio (費用率)", "0.85%")
    with kpi4: st.metric("YTD Return (YTD 報酬率)", f"{ytd_ret:.2f}%")

    st.markdown("<br>", unsafe_allow_html=True)
    col_left, col_right = st.columns([7, 3])
    
    with col_left:
        # --- ETF 主圖表 ---
        if not df_etf.empty:
            df_cw = get_etf_cash_history(df_files)
            df_etf_comb = df_etf.join(df_cw, how='left')
            df_etf_comb['Cash_Weight'] = df_etf_comb['Cash_Weight'].ffill().fillna(0)
            str_dates = df_etf_comb.index.strftime('%Y-%m-%d')

            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.8, 0.2], vertical_spacing=0.02, subplot_titles=("00981a NAV Trend", "Volume"))
            fig.add_trace(go.Candlestick(
                x=str_dates, open=df_etf_comb['Open'], high=df_etf_comb['High'], low=df_etf_comb['Low'], close=df_etf_comb['Close'],
                name='K線', increasing_line_color='#EF5350', decreasing_line_color='#26A69A'
            ), row=1, col=1)
            
            # 底部成交量
            fig.add_trace(go.Bar(x=str_dates, y=df_etf_comb['Volume'], marker_color='#2962FF', name='Volume'), row=2, col=1)
            fig.update_layout(**tv_layout, height=450, xaxis_rangeslider_visible=False, showlegend=False)
            fig.update_xaxes(type='category', nticks=10)
            st.plotly_chart(fig, use_container_width=True, key="dashboard_main_chart")
        else:
            st.warning("無法取得 ETF 資料。")

        # --- Alert Center (紅框設計) ---
        st.markdown('<div class="alert-header">🚨 ALERT CENTER (即時警示)</div>', unsafe_allow_html=True)
        with st.container():
            st.markdown('<div class="alert-content">', unsafe_allow_html=True)
            if st.button("掃描跌破成本股票", type="primary"):
                sids = df_latest['ID'].tolist()
                report_data = []
                with st.spinner("掃描中..."):
                    price_map = get_bulk_prices(sids, start_d, datetime.now() + timedelta(days=1))
                    for row in df_latest.itertuples():
                        sid, name = row.ID, row.Name
                        df_p = price_map.get(sid, pd.DataFrame())
                        if not df_p.empty:
                            _, cost_line, _, _ = calculate_avg_cost_optimized(df_history_cache, sid, df_p)
                            curr_price = df_p['Close'].iloc[-1]
                            curr_cost = cost_line[-1] if cost_line and cost_line[-1] is not None else 0
                            if curr_cost > 0:
                                diff_pct = (curr_price - curr_cost) / curr_cost * 100
                                if diff_pct < 0:
                                    report_data.append({"股票代號": f"{name} ({sid})", "Current Price": round(curr_price, 2), "NAV Cost": round(curr_cost, 2), "Deviation %": f"{diff_pct:.2f}%"})
                
                if report_data:
                    df_uw = pd.DataFrame(report_data)
                    st.dataframe(df_uw, use_container_width=True, hide_index=True)
                else:
                    st.success("目前沒有持股低於成本價！")
            st.markdown('</div>', unsafe_allow_html=True)

    with col_right:
        # --- Top Holdings (橫向進度條風格) ---
        st.markdown("<h4 style='color: #D1D4DC; text-align: center;'>Top Holdings (最新持股)</h4>", unsafe_allow_html=True)
        if not df_latest.empty:
            df_sorted = df_latest.sort_values(by='Weight_num', ascending=True).tail(10) # 取前10大
            fig_bar = go.Figure(go.Bar(
                x=df_sorted['Weight_num'], y=df_sorted['Name'], orientation='h',
                marker=dict(color=['#26A69A', '#EF5350', '#2962FF', '#FCCA46', '#AB47BC', '#FF7043', '#8D6E63', '#26C6DA', '#D4E157', '#EC407A']),
                text=df_sorted['Weight_str'], textposition='outside', textfont=dict(color='#D1D4DC')
            ))
            fig_bar.update_layout(
                **tv_layout, height=550, margin=dict(l=0, r=0, t=10, b=0),
                xaxis=dict(showgrid=False, showticklabels=False),
                yaxis=dict(showgrid=False, color='#D1D4DC', tickfont=dict(size=13))
            )
            st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})

# =========================================================
# 頁面 B: 每日持倉變化
# =========================================================
elif menu == "每日持倉變化":
    col_title, col_picker, col_kpi = st.columns([4, 2, 3])
    with col_title: st.header("每日持倉變化")
    with col_picker: pick_date = st.date_input("選擇日期", latest_date_record.date())
    with col_kpi: st.metric("Total Portfolio Value", "$1,678,978.66") # Mock UI placement
    
    st.markdown("<hr style='margin-top: 5px; margin-bottom: 20px;'>", unsafe_allow_html=True)
    
    pick_date_ts = pd.to_datetime(pick_date)
    curr_record = df_files[df_files['date'] == pick_date_ts]
    
    if curr_record.empty:
        st.warning(f"無 {pick_date} 資料。")
    else:
        curr_idx = curr_record.index[0]
        prev_idx = curr_idx - 1
        
        if prev_idx < 0:
            st.warning("第一筆資料，無前一日可比較。")
        else:
            path_curr = curr_record.iloc[0]['path']
            path_prev = df_files.iloc[prev_idx]['path']
            df_t = parse_excel_holding(path_curr)
            df_y = parse_excel_holding(path_prev)
            
            m = pd.merge(df_y[['ID', 'Name', 'Shares_num']], df_t[['ID', 'Name', 'Shares_num', 'Weight_str', 'Weight_num']], on='ID', how='outer', suffixes=('_old', '_new'))
            m['Name'] = m['Name_new'].combine_first(m['Name_old']).fillna("未知")
            m = m.fillna(0)
            m['股數變化'] = m['Shares_num_new'] - m['Shares_num_old']
            
            df_change = m[(m['Shares_num_old'] != 0) | (m['Shares_num_new'] != 0)].copy()
            sids_change = df_change[df_change['股數變化'] != 0]['ID'].tolist()
            
            price_map = {}
            if sids_change:
                bulk_p_map = get_bulk_prices(sids_change, pick_date_ts - timedelta(days=7), pick_date_ts + timedelta(days=1))
                for sid in sids_change:
                    s_data = bulk_p_map.get(sid, pd.DataFrame())
                    if not s_data.empty:
                        valid_p = s_data[s_data.index <= pick_date_ts]
                        price_map[sid] = valid_p['Close'].iloc[-1] if not valid_p.empty else 0
                    else: price_map[sid] = 0
            
            df_change['Price'] = df_change['ID'].map(price_map).fillna(0)
            df_change['差額'] = df_change['股數變化'] * df_change['Price']
            
            # 計算 24h % CHG (模擬)
            df_change['% CHG'] = np.random.uniform(-5, 5, size=len(df_change)) 
            
            display_rows = []
            for _, row in df_change.iterrows():
                diff_amt = row['差額']
                diff_txt = f"${diff_amt:,.2f}" if diff_amt != 0 else "$0.00"
                chg_pct = row['% CHG']
                
                display_rows.append({
                    'Ticker': row['ID'], 'Name': row['Name'], 
                    '% CHG': chg_pct, 
                    'NET Δ SHRS (差額)': diff_txt, 
                    'Raw_Diff': row['股數變化'], 'Is_Zero': int(row['Shares_num_new']) == 0, 'Weight_num': row['Weight_num']
                })
            
            df_display = pd.DataFrame(display_rows).sort_values(by=['Is_Zero', 'Weight_num'], ascending=[True, False])
            
            col_table, col_chart = st.columns([4, 6])
            
            with col_table:
                # 為了符合 Mockup，只顯示精簡欄位並設定自訂格式
                st.dataframe(
                    df_display[['Ticker', 'Name', '% CHG', 'NET Δ SHRS (差額)']].style.format({
                        '% CHG': '{:.2f}%'
                    }).applymap(
                        lambda v: 'color: #EF5350' if float(v.strip('%')) > 0 else 'color: #26A69A' if float(v.strip('%')) < 0 else '', 
                        subset=['% CHG']
                    ).applymap(
                        lambda v: 'color: #EF5350' if v.startswith('$') and not v.startswith('$-') and v != '$0.00' else 'color: #26A69A' if v.startswith('$-') else '', 
                        subset=['NET Δ SHRS (差額)']
                    ),
                    use_container_width=True, hide_index=True, height=850
                )
                
            with col_chart:
                changed_stocks = df_display[df_display['Raw_Diff'] != 0]
                if not changed_stocks.empty:
                    target_label = st.selectbox("Select Ticker for Technical Analysis:", changed_stocks['Ticker'] + " " + changed_stocks['Name'])
                    if target_label:
                        tsid, tname = target_label.split(" ")[0], target_label.split(" ")[1]
                        draw_ta_chart(tsid, tname, df_history_cache)
                else:
                    st.info("當日無任何變動股。")
