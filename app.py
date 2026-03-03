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
# 2. 效能優化與智慧救援引擎 (終極 V9: 包含上市+上櫃備援)
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

# 【上市備援】TWSE
def fetch_twse_monthly(sid, year_month_str):
    url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={year_month_str}&stockNo={sid}"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
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
                        'Open': clean_number(row[3]), 'High': clean_number(row[4]),
                        'Low': clean_number(row[5]), 'Close': clean_number(row[6])
                    })
                except: continue
            if records:
                df = pd.DataFrame(records).set_index('Date')
                df.index = pd.to_datetime(df.index).normalize()
                return df
    except: pass
    return pd.DataFrame()

# 【上櫃備援】TPEx (解決 00981A 真正問題的核心)
def fetch_tpex_monthly(sid, year, month):
    roc_year = year - 1911
    month_str = f"{month:02d}"
    url = f"https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php?l=zh-tw&d={roc_year}/{month_str}&stkno={sid}"
    try:
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = requests.get(url, headers=headers, timeout=5)
        data = r.json()
        if 'aaData' in data and data['aaData']:
            records = []
            for row in data['aaData']:
                try:
                    d_parts = row[0].split('/')
                    g_year = int(d_parts[0]) + 1911
                    date_str = f"{g_year}-{d_parts[1]}-{d_parts[2]}"
                    records.append({
                        'Date': pd.to_datetime(date_str),
                        'Open': clean_number(row[3]), 'High': clean_number(row[4]),
                        'Low': clean_number(row[5]), 'Close': clean_number(row[6])
                    })
                except: continue
            if records:
                df = pd.DataFrame(records).set_index('Date')
                df.index = pd.to_datetime(df.index).normalize()
                return df
    except: pass
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_stock_data_v9(sids, start_dt, end_dt):
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
        
        # 1. 嘗試 Yahoo Finance
        try:
            tkr_tw = yf.Ticker(f"{sid}.TW")
            raw_tw = tkr_tw.history(start=start_dt, end=end_dt)
            if raw_tw is None or raw_tw.empty: raw_tw = yf.download(f"{sid}.TW", start=start_dt, end=end_dt, progress=False)
            cl_tw = clean_and_format_price_df(raw_tw)
            if cl_tw is not None: df_combined = cl_tw
        except: pass
            
        try:
            tkr_two = yf.Ticker(f"{sid}.TWO")
            raw_two = tkr_two.history(start=start_dt, end=end_dt)
            if raw_two is None or raw_two.empty: raw_two = yf.download(f"{sid}.TWO", start=start_dt, end=end_dt, progress=False)
            cl_two = clean_and_format_price_df(raw_two)
            if cl_two is not None:
                df_combined = cl_two if df_combined.empty else df_combined.combine_first(cl_two)
        except: pass
        
        # 2. 官方雙引擎備援 (TWSE + TPEx) 解決資料斷層
        clean_sid = str(sid).replace('.TW', '').replace('.TWO', '').strip()
        try:
            if df_combined.empty:
                months = pd.date_range(start_dt.replace(day=1), end_dt, freq='MS')
                for m in months[-3:]:
                    df_twse = fetch_twse_monthly(clean_sid, m.strftime('%Y%m01'))
                    df_tpex = fetch_tpex_monthly(clean_sid, m.year, m.month)
                    df_official = df_twse if not df_twse.empty else df_tpex
                    if not df_official.empty:
                        df_official = df_official[df_official['Close'] > 0]
                        df_combined = df_official if df_combined.empty else df_combined.combine_first(df_official)
                    time.sleep(0.3)
            else:
                df_combined = df_combined.sort_index()
                gaps = df_combined.index.to_series().diff()
                big_gaps = gaps[gaps > pd.Timedelta(days=10)]
                for gap_end in big_gaps.index:
                    gap_start = gap_end - gaps[gap_end]
                    gap_months = pd.date_range(gap_start.replace(day=1), gap_end, freq='MS')
                    for m in gap_months:
                        df_twse = fetch_twse_monthly(clean_sid, m.strftime('%Y%m01'))
                        df_tpex = fetch_tpex_monthly(clean_sid, m.year, m.month)
                        df_official = df_twse if not df_twse.empty else df_tpex
                        if not df_official.empty:
                            df_official = df_official[df_official['Close'] > 0]
                            df_combined = df_combined.combine_first(df_official)
                        time.sleep(0.3)
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
            if total_cost == 0 and avg_cost == 0:
                avg_cost, total_cost = p, s * p
            else:
                if d > 0: total_cost += d * p
                elif d < 0: total_cost += d * avg_cost
            
            avg_cost = total_cost / s if s > 0 else 0.0
            cost_line.append(avg_cost)
            
    return df_calc.index, cost_line, shares_series, diff_series

def draw_analysis_chart(sid, name, df_history, unique_key_prefix):
    chart_start = datetime.now() - timedelta(days=365)
    price_map = fetch_stock_data_v9([sid], chart_start, datetime.now() + timedelta(days=1))
    df_chart_price = price_map.get(sid, pd.DataFrame())
    
    if df_chart_price.empty:
        st.error(f"無法取得 {sid} {name} 的股價資料")
        return

    dates, cost_line, shares_series, diff_series = calculate_avg_cost_optimized(df_history, sid, df_chart_price)
    amounts = diff_series * df_chart_price['Close'].values
    
    x_vals = np.arange(len(dates))
    str_dates = dates.strftime('%Y-%m-%d')

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, 
        row_heights=[0.6, 0.2, 0.2], vertical_spacing=0.05,
        subplot_titles=(f"<b>{sid} {name} 股價與成本</b>", "<b>持股水位</b>", "<b>每日增減金額</b>")
    )
    
    # 【游標修復】加入 customdata 綁定真實日期，並設定 hovertemplate
    fig.add_trace(go.Candlestick(
        x=x_vals, open=df_chart_price['Open'].values, high=df_chart_price['High'].values,
        low=df_chart_price['Low'].values, close=df_chart_price['Close'].values, name='股價',
        increasing_line_color='red', decreasing_line_color='green',
        customdata=str_dates,
        hovertemplate='日期: %{customdata}<br>開: %{open:.2f}<br>高: %{high:.2f}<br>低: %{low:.2f}<br>收: %{close:.2f}<extra></extra>'
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(
        x=x_vals, y=cost_line, mode='lines', 
        line=dict(color='orange', width=2, dash='dot'), name='981成本',
        customdata=str_dates, hovertemplate='日期: %{customdata}<br>成本: %{y:.2f}<extra></extra>'
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(
        x=x_vals, y=shares_series, mode='lines+markers',
        fill='tozeroy', line=dict(color='blue'), name='持股數',
        customdata=str_dates, hovertemplate='日期: %{customdata}<br>持股數: %{y}<extra></extra>'
    ), row=2, col=1)
    
    colors = ['red' if x > 0 else 'green' for x in amounts]
    fig.add_trace(go.Bar(
        x=x_vals, y=amounts, marker_color=colors, name='淨買賣額',
        customdata=str_dates, hovertemplate='日期: %{customdata}<br>淨買賣: %{y}<extra></extra>'
    ), row=3, col=1)
    
    fig.update_layout(height=800, xaxis_rangeslider_visible=False, template="plotly_white")
    
    tick_step = max(1, len(x_vals) // 10)
    fig.update_xaxes(tickmode='array', tickvals=x_vals[::tick_step], ticktext=str_dates[::tick_step])
    st.plotly_chart(fig, use_container_width=True, key=f"{unique_key_prefix}_chart_{sid}")

@st.dialog("個股詳細分析", width="large")
def show_stock_dialog(sid, name, df_history):
    draw_analysis_chart(sid, name, df_history, "dialog")

# ---------------------------------------------------------
# 4. 網頁主介面
# ---------------------------------------------------------

st.title("📊 00981a ETF 追蹤器")

with st.spinner('正在同步資料庫...'):
    df_files = sync_data_repo()

if df_files.empty:
    st.error("找不到資料，請確認 GitHub 連結。")
    st.stop()

df_history_cache = get_all_holdings_history(df_files)
latest_date_record = df_files.iloc[-1]['date']
latest_path = df_files.iloc[-1]['path']
st.sidebar.info(f"最新資料日期: {latest_date_record.strftime('%Y-%m-%d')}")

menu = st.sidebar.radio("功能選單", ["總覽 (Dashboard)", "每日持倉變化"])

# =========================================================
# 頁面 A: 總覽 (Dashboard)
# =========================================================
if menu == "總覽 (Dashboard)":
    st.header("📈 00981a 總覽")
    df_latest = parse_excel_holding(latest_path)
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("00981a 近一年走勢與現金權重")
        start_d = datetime.now() - timedelta(days=365)
        
        # 呼叫 V9 雙引擎
        etf_price_map = fetch_stock_data_v9(["00981A", "00981"], start_d, datetime.now() + timedelta(days=1))
        df_etf_A = etf_price_map.get("00981A", pd.DataFrame())
        df_etf_B = etf_price_map.get("00981", pd.DataFrame())
        
        if not df_etf_A.empty and not df_etf_B.empty: df_etf = df_etf_A.combine_first(df_etf_B)
        elif not df_etf_A.empty: df_etf = df_etf_A
        else: df_etf = df_etf_B
        
        if not df_etf.empty:
            df_cw = get_etf_cash_history(df_files)
            df_etf_comb = df_etf.join(df_cw, how='left')
            df_etf_comb['Cash_Weight'] = df_etf_comb['Cash_Weight'].ffill().fillna(0)
            df_etf_comb = df_etf_comb.dropna(subset=['Close'])
            
            x_vals = np.arange(len(df_etf_comb))
            str_dates = df_etf_comb.index.strftime('%Y-%m-%d')

            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05,
                subplot_titles=("<b>00981a K線</b>", "<b>現金權重走勢 (%)</b>")
            )
            
            # 【游標修復】Dashboard 主圖
            fig.add_trace(go.Candlestick(
                x=x_vals, open=df_etf_comb['Open'].values, high=df_etf_comb['High'].values,
                low=df_etf_comb['Low'].values, close=df_etf_comb['Close'].values, name='K線',
                increasing_line_color='red', decreasing_line_color='green',
                customdata=str_dates,
                hovertemplate='日期: %{customdata}<br>開: %{open:.2f}<br>高: %{high:.2f}<br>低: %{low:.2f}<br>收: %{close:.2f}<extra></extra>'
            ), row=1, col=1)
            
            fig.add_trace(go.Scatter(
                x=x_vals, y=df_etf_comb['Cash_Weight'].values, mode='lines', 
                line=dict(color='#17becf', width=2), fill='tozeroy', name='現金權重',
                customdata=str_dates, hovertemplate='日期: %{customdata}<br>現金權重: %{y:.2f}%<extra></extra>'
            ), row=2, col=1)

            fig.update_layout(height=500, xaxis_rangeslider_visible=False, margin=dict(l=20, r=20, t=20, b=20), template="plotly_white")
            tick_step = max(1, len(x_vals) // 10)
            fig.update_xaxes(tickmode='array', tickvals=x_vals[::tick_step], ticktext=str_dates[::tick_step])
            
            st.plotly_chart(fig, use_container_width=True, key="dashboard_main_chart")
        else:
            st.warning("無法取得 00981A 資料。")

    with col2:
        st.subheader("📋 最新持股 (依權重)")
        if not df_latest.empty:
            df_sorted = df_latest.sort_values(by='Weight_num', ascending=False)
            df_show = df_sorted[['ID', 'Name', 'Shares_num', 'Weight_str']].rename(columns={
                'ID': '股票代號', 'Name': '股票名稱', 'Shares_num': '持股數', 'Weight_str': '持股權重'
            })
            st.dataframe(df_show.style.format({'持股數': '{:,.0f}'}), use_container_width=True, height=400, hide_index=True)

    st.divider()
    st.subheader("⚠️ 股價跌破 ETF 成本線")
    
    if st.button("一鍵極速分析", type="primary", key="btn_calc_cost"):
        st.session_state['run_dashboard_analysis'] = True

    if st.session_state.get('run_dashboard_analysis'):
        report_data = []
        sids = df_latest['ID'].tolist()
        
        with st.spinner("正在計算成本分析..."):
            price_map = fetch_stock_data_v9(sids, start_d, datetime.now() + timedelta(days=1))
            
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
                            report_data.append({"代號": sid, "名稱": name, "現價": round(curr_price, 2), "981成本": round(curr_cost, 2), "帳面損益": diff_pct})
        
        df_underwater = pd.DataFrame(report_data)
        if not df_underwater.empty:
            df_underwater = df_underwater.sort_values("帳面損益")
            st.markdown("💡 **直接點擊【股票名稱按鈕】即可彈出 K 線圖與成本分析**")
            cols = st.columns([1, 2, 1, 1, 1.5])
            cols[0].markdown("**股票代號**")
            cols[1].markdown("**股票名稱 (點擊看圖)**")
            cols[2].markdown("**現價**")
            cols[3].markdown("**981成本**")
            cols[4].markdown("**帳面損益 (%)**")
            
            for _, row in df_underwater.iterrows():
                cols = st.columns([1, 2, 1, 1, 1.5])
                cols[0].write(row['代號'])
                if cols[1].button(f"{row['名稱']}", key=f"btn_uw_{row['代號']}", use_container_width=True):
                    show_stock_dialog(row['代號'], row['名稱'], df_history_cache)
                cols[2].write(f"{row['現價']:.2f}")
                cols[3].write(f"{row['981成本']:.2f}")
                color = "green" if row['帳面損益'] < 0 else "red"
                cols[4].markdown(f"<span style='color:{color}'>{row['帳面損益']:.2f}%</span>", unsafe_allow_html=True)
        else:
            st.success("目前沒有持股低於成本價！")

# =========================================================
# 頁面 B: 每日持倉變化
# =========================================================
elif menu == "每日持倉變化":
    st.header("📅 每日持倉變化")
    col_date, _ = st.columns([1, 3])
    with col_date:
        pick_date = st.date_input("選擇日期", latest_date_record.date(), key="daily_date_picker")
        pick_date_ts = pd.to_datetime(pick_date)

    curr_record = df_files[df_files['date'] == pick_date_ts]
    
    if curr_record.empty: st.warning(f"無 {pick_date} 資料。")
    else:
        curr_idx = curr_record.index[0]
        prev_idx = curr_idx - 1
        if prev_idx < 0: st.warning("這是第一筆資料，無前一日可比較。")
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
                dl_start = pick_date_ts - timedelta(days=7) 
                bulk_p_map = fetch_stock_data_v9(sids_change, dl_start, pick_date_ts + timedelta(days=1))
                for sid in sids_change:
                    s_data = bulk_p_map.get(sid, pd.DataFrame())
                    if not s_data.empty:
                        valid_p = s_data[s_data.index <= pick_date_ts]
                        price_map[sid] = valid_p['Close'].iloc[-1] if not valid_p.empty else 0
                    else: price_map[sid] = 0
            
            df_change['Price'] = df_change['ID'].map(price_map).fillna(0)
            df_change['差額'] = df_change['股數變化'] * df_change['Price']
            
            display_rows = []
            for _, row in df_change.iterrows():
                diff_amt = row['差額']
                diff_txt = format_money_label(diff_amt) if diff_amt != 0 else "0"
                display_rows.append({
                    '股票代號': row['ID'], '股票名稱': row['Name'], '持股權重': row['Weight_str'],
                    '前股數': int(row['Shares_num_old']), '今股數': int(row['Shares_num_new']),
                    '股數變化': int(row['股數變化']), '差額': diff_txt, 'Weight_num': row['Weight_num'], 'Is_Zero': int(row['Shares_num_new']) == 0
                })
            
            df_display = pd.DataFrame(display_rows)
            if not df_display.empty:
                df_display = df_display.sort_values(by=['Is_Zero', 'Weight_num'], ascending=[True, False])
                st.subheader("📋 持股變化表")
                st.dataframe(
                    df_display.drop(columns=['Weight_num', 'Is_Zero']).style.applymap(
                        lambda v: 'color: red' if v > 0 else 'color: green' if v < 0 else '', subset=['股數變化']
                    ).format({'前股數': '{:,}', '今股數': '{:,}', '股數變化': '{:,}'}),
                    use_container_width=True, hide_index=True
                )
                st.divider()
                st.subheader("📈 變動個股技術分析")
                
                changed_stocks = df_display[df_display['股數變化'] != 0]
                if not changed_stocks.empty:
                    target_label = st.selectbox("選擇有變動的股票:", changed_stocks['股票代號'] + " " + changed_stocks['股票名稱'])
                    if target_label:
                        tsid = target_label.split(" ")[0]
                        tname = target_label.split(" ")[1]
                        draw_analysis_chart(tsid, tname, df_history_cache, "daily")
                else: st.info("當日無任何變動股可供繪圖。")
            else: st.info("該日持股無變化。")
