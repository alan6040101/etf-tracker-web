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
# 1. 工具函式
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
    """讀取並解析 Excel，保留原始字串格式以便顯示"""
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
        
        # 保留數值欄位用於計算
        if 'Shares' in df.columns:
            df['Shares_num'] = df['Shares'].apply(lambda x: clean_number(x))
        else: df['Shares_num'] = 0.0
            
        # 保留權重原始字串與數值
        if 'Weight' in df.columns:
            df['Weight_str'] = df['Weight'].astype(str)
            df['Weight_num'] = df['Weight'].apply(lambda x: clean_number(x))
        else: 
            df['Weight_str'] = "0%"
            df['Weight_num'] = 0.0
        
        return df[['ID', 'Name', 'Shares_num', 'Weight_str', 'Weight_num']]
    return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_stock_price_history(ticker, start_date):
    """取得股價歷史 (含容錯機制)"""
    # 嘗試使用者指定的代號
    try:
        df = yf.download(ticker, start=start_date, progress=False, auto_adjust=True)
        if not df.empty:
            if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
            return df
    except: pass
    
    # 如果失敗，嘗試去除 'A' 或標準 .TW 格式 (針對 00981A.TW -> 00981.TW)
    if 'A' in ticker:
        alt_ticker = ticker.replace('A', '')
        try:
            df = yf.download(alt_ticker, start=start_date, progress=False, auto_adjust=True)
            if not df.empty:
                if isinstance(df.columns, pd.MultiIndex): df.columns = df.columns.get_level_values(0)
                return df
        except: pass
        
    return pd.DataFrame()

def calculate_avg_cost(df_files, target_sid, price_df):
    """計算特定股票的移動平均成本線"""
    history_data = []
    # 這裡為了效能，簡化讀取，實際運作建議將解析結果快取
    # 在 Streamlit 中，若檔案不多可直接跑
    for _, row in df_files.iterrows():
        df_step = parse_excel_holding(row['path'])
        shares = 0
        if not df_step.empty:
            match = df_step[df_step['ID'] == str(target_sid)]
            if not match.empty:
                shares = match['Shares_num'].values[0]
        history_data.append({'Date': row['date'], 'Shares': shares})
    
    df_h = pd.DataFrame(history_data).sort_values('Date').set_index('Date')
    
    if price_df.empty: return [], [], [], []
    
    df_calc = price_df[['Close']].join(df_h, how='left')
    df_calc['Shares'] = df_calc['Shares'].ffill().fillna(0)
    df_calc['Diff'] = df_calc['Shares'].diff().fillna(0)
    
    cost_line = []
    total_cost = 0.0
    avg_cost = 0.0
    
    shares_series = df_calc['Shares'].values
    diff_series = df_calc['Diff'].values
    close_series = df_calc['Close'].values
    
    for s, d, p in zip(shares_series, diff_series, close_series):
        if s <= 0:
            avg_cost = 0.0; total_cost = 0.0; cost_line.append(None)
        else:
            if total_cost == 0 and avg_cost == 0:
                avg_cost = p; total_cost = s * p
            else:
                if d > 0: total_cost += d * p
                elif d < 0: total_cost += d * avg_cost
            
            if s > 0: avg_cost = total_cost / s
            else: avg_cost = 0.0
            cost_line.append(avg_cost)
            
    return df_calc.index, cost_line, shares_series, diff_series

def draw_analysis_chart(sid, name, df_files):
    """繪製個股詳細分析圖 (K線 + 成本 + 水位 + 金額)"""
    chart_start = datetime.now() - timedelta(days=365)
    df_chart_price = get_stock_price_history(f"{sid}.TW", chart_start)
    
    if df_chart_price.empty:
        st.error(f"無法取得 {sid} {name} 的股價資料")
        return

    dates, cost_line, shares_series, diff_series = calculate_avg_cost(df_files, sid, df_chart_price)
    amounts = diff_series * df_chart_price['Close'].values

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, 
        row_heights=[0.6, 0.2, 0.2], vertical_spacing=0.05,
        subplot_titles=(f"<b>{sid} {name} 股價與成本</b>", "<b>持股水位</b>", "<b>每日增減金額</b>")
    )
    
    # 1. K線 + 成本
    fig.add_trace(go.Candlestick(
        x=dates, open=df_chart_price['Open'], high=df_chart_price['High'],
        low=df_chart_price['Low'], close=df_chart_price['Close'], name='股價'
    ), row=1, col=1)
    
    fig.add_trace(go.Scatter(
        x=dates, y=cost_line, mode='lines', 
        line=dict(color='orange', width=2, dash='dot'), name='981成本'
    ), row=1, col=1)
    
    # 2. 持股
    fig.add_trace(go.Scatter(
        x=dates, y=shares_series, mode='lines+markers',
        fill='tozeroy', line=dict(color='blue'), name='持股數'
    ), row=2, col=1)
    
    # 3. 金額
    colors = ['red' if x > 0 else 'green' for x in amounts]
    fig.add_trace(go.Bar(
        x=dates, y=amounts, marker_color=colors, name='淨買賣額'
    ), row=3, col=1)
    
    fig.update_layout(height=800, xaxis_rangeslider_visible=False, template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------
# 2. 主程式邏輯
# ---------------------------------------------------------

st.title("📊 00981a ETF 追蹤戰情室")

with st.spinner('正在同步資料庫...'):
    df_files = sync_data_repo()

if df_files.empty:
    st.error("找不到資料，請確認 GitHub 連結。")
    st.stop()

latest_date_record = df_files.iloc[-1]['date']
latest_path = df_files.iloc[-1]['path']
st.sidebar.info(f"最新資料日期: {latest_date_record.strftime('%Y-%m-%d')}")

menu = st.sidebar.radio("功能選單", ["總覽 (Dashboard)", "每日持倉變化"])

# =========================================================
# 頁面 A: 總覽 (Dashboard)
# =========================================================
if menu == "總覽 (Dashboard)":
    st.header("📈 00981a 總覽")
    
    # 1. 00981A.TW 走勢 (修正：使用使用者指定代號)
    col1, col2 = st.columns([2, 1])
    with col1:
        st.subheader("00981a 近一年走勢")
        end_d = datetime.now()
        start_d = end_d - timedelta(days=365)
        
        # 這裡指定抓取 00981A.TW
        target_etf_ticker = "00981A.TW"
        df_etf = get_stock_price_history(target_etf_ticker, start_d)
        
        if not df_etf.empty:
            fig = go.Figure(data=[go.Candlestick(
                x=df_etf.index,
                open=df_etf['Open'], high=df_etf['High'],
                low=df_etf['Low'], close=df_etf['Close']
            )])
            fig.update_layout(height=400, margin=dict(l=20, r=20, t=20, b=20), title=target_etf_ticker)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning(f"無法取得 {target_etf_ticker} 資料，請確認代號是否正確。")

    # 2. 潛在雷區表格 (修正：支援點擊互動)
    df_latest = parse_excel_holding(latest_path)
    st.subheader("⚠️ 潛在雷區 (點擊表格列以查看詳情)")
    
    # 預先計算按鈕
    if st.button("計算持股盈虧 (需時較長)"):
        st.session_state['run_analysis'] = True

    if st.session_state.get('run_analysis'):
        report_data = []
        sids = df_latest['ID'].tolist()
        tickers = [f"{sid}.TW" for sid in sids]
        
        with st.spinner("正在計算成本分析..."):
            bulk_data = yf.download(tickers, start=start_d, group_by='ticker', progress=False, auto_adjust=True)
            
            for row in df_latest.itertuples():
                sid = row.ID; name = row.Name
                try:
                    df_p = bulk_data[f"{sid}.TW"] if len(tickers) > 1 else bulk_data
                    df_p = df_p.dropna()
                except: df_p = pd.DataFrame()

                if not df_p.empty:
                    _, cost_line, _, _ = calculate_avg_cost(df_files, sid, df_p)
                    curr_price = df_p['Close'].iloc[-1]
                    curr_cost = cost_line[-1] if cost_line and cost_line[-1] is not None else 0
                    
                    if curr_cost > 0:
                        diff_pct = (curr_price - curr_cost) / curr_cost * 100
                        if diff_pct < 0: # 只顯示虧損
                            report_data.append({
                                "代號": sid, "名稱": name,
                                "現價": round(curr_price, 2),
                                "981成本": round(curr_cost, 2),
                                "帳面損益 (%)": round(diff_pct, 2)
                            })
        
        df_underwater = pd.DataFrame(report_data)
        
        if not df_underwater.empty:
            df_underwater = df_underwater.sort_values("帳面損益 (%)")
            
            # 使用 selection_mode 實現點選功能
            event = st.dataframe(
                df_underwater.style.format({
                    "現價": "{:.2f}", "981成本": "{:.2f}", "帳面損益 (%)": "{:.2f}%"
                }).applymap(lambda v: 'color: green' if v < 0 else 'color: red', subset=['帳面損益 (%)']),
                use_container_width=True,
                on_select="rerun",
                selection_mode="single-row"
            )
            
            # 處理點擊事件
            if len(event.selection.rows) > 0:
                selected_idx = event.selection.rows[0]
                selected_row = df_underwater.iloc[selected_idx]
                st.divider()
                draw_analysis_chart(selected_row['代號'], selected_row['名稱'], df_files)
        else:
            st.success("目前沒有持股低於成本價！")

# =========================================================
# 頁面 B: 每日持倉變化
# =========================================================
elif menu == "每日持倉變化":
    st.header("📅 每日持倉變化")
    
    col_date, _ = st.columns([1, 3])
    with col_date:
        pick_date = st.date_input("選擇日期", latest_date_record.date())
        pick_date_ts = pd.to_datetime(pick_date)

    curr_record = df_files[df_files['date'] == pick_date_ts]
    
    if curr_record.empty:
        st.warning(f"無 {pick_date} 資料。")
    else:
        curr_idx = curr_record.index[0]
        prev_idx = curr_idx - 1
        
        if prev_idx < 0:
            st.warning("這是第一筆資料，無前一日可比較。")
        else:
            # 1. 建立 Excel 邏輯報表
            path_curr = curr_record.iloc[0]['path']
            path_prev = df_files.iloc[prev_idx]['path']
            
            df_t = parse_excel_holding(path_curr)
            df_y = parse_excel_holding(path_prev)
            
            # 合併
            m = pd.merge(df_y[['ID', 'Name', 'Shares_num']], 
                         df_t[['ID', 'Name', 'Shares_num', 'Weight_str', 'Weight_num']], 
                         on='ID', how='outer', suffixes=('_old', '_new'))
            
            m['Name'] = m['Name_new'].combine_first(m['Name_old']).fillna("未知")
            m = m.fillna(0)
            
            # 核心邏輯：股數變化
            m['股數變化'] = m['Shares_num_new'] - m['Shares_num_old']
            
            # 篩選變動
            df_change = m[(m['Shares_num_old'] != 0) | (m['Shares_num_new'] != 0)].copy()
            df_change = df_change[df_change['股數變化'] != 0].copy()
            
            # 計算差額 (Diff Amount) - 需取得當日股價
            sids_change = df_change['ID'].tolist()
            price_map = {}
            
            if sids_change:
                tickers_c = [f"{sid}.TW" for sid in sids_change]
                dl_start = pick_date_ts - timedelta(days=5)
                dl_end = pick_date_ts + timedelta(days=1)
                try:
                    p_data = yf.download(tickers_c, start=dl_start, end=dl_end, group_by='ticker', progress=False, auto_adjust=True)
                    for sid in sids_change:
                        try:
                            series = p_data[f"{sid}.TW"] if len(sids_change) > 1 else p_data
                            # 找最接近 pick_date 的價格
                            valid_p = series[series.index <= pick_date_ts]
                            price_map[sid] = valid_p['Close'].iloc[-1] if not valid_p.empty else 0
                        except: price_map[sid] = 0
                except: pass
            
            df_change['Price'] = df_change['ID'].map(price_map).fillna(0)
            df_change['差額'] = df_change['股數變化'] * df_change['Price']
            
            # 格式化欄位 (還原原始需求)
            display_rows = []
            for _, row in df_change.iterrows():
                diff_amt = row['差額']
                diff_txt = format_money_label(diff_amt) if diff_amt != 0 else "0"
                
                display_rows.append({
                    '股票代號': row['ID'],
                    '股票名稱': row['Name'],
                    '持股權重': row['Weight_str'],
                    '前股數': int(row['Shares_num_old']),
                    '今股數': int(row['Shares_num_new']),
                    '股數變化': int(row['股數變化']),
                    '差額': diff_txt,
                    'Raw_Diff': row['股數變化'] # 用於排序或變色判斷
                })
            
            df_display = pd.DataFrame(display_rows)
            
            if not df_display.empty:
                # 排序: 買進在前，賣出在後 (可選)
                df_display = df_display.sort_values('Raw_Diff', ascending=False)
                
                # 樣式設定
                st.subheader("📋 持股變化表")
                
                # 定義變色函式
                def color_diff(val):
                    if isinstance(val, (int, float)):
                        return 'color: red' if val > 0 else 'color: green' if val < 0 else ''
                    return ''
                
                def color_diff_text(val):
                    # 簡單判斷字串內容或依賴 Raw_Diff 判斷 (這裡簡化處理)
                    return '' 

                # 顯示表格 (模擬 Excel 條件格式)
                st.dataframe(
                    df_display.drop(columns=['Raw_Diff']).style.applymap(
                        lambda v: 'color: red' if v > 0 else 'color: green', 
                        subset=['股數變化']
                    ).format({
                        '前股數': '{:,}', '今股數': '{:,}', '股數變化': '{:,}'
                    }),
                    use_container_width=True
                )
                
                # 2. 下方 K 線圖 (下拉選單)
                st.divider()
                st.subheader("📈 變動個股技術分析")
                
                target_label = st.selectbox("選擇股票:", df_display['股票代號'] + " " + df_display['股票名稱'])
                
                if target_label:
                    tsid = target_label.split(" ")[0]
                    tname = target_label.split(" ")[1]
                    draw_analysis_chart(tsid, tname, df_files)
            else:
                st.info("該日持股無變化。")
