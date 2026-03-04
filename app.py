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
# 設定頁面配置 (TradingView 風格準備)
# ---------------------------------------------------------
st.set_page_config(page_title="00981a ETF 追蹤器", layout="wide", initial_sidebar_state="expanded")

# 注入 TradingView 風格的 CSS
st.markdown("""
<style>
    /* 全域背景與字體顏色 */
    .stApp {
        background-color: #131722;
        color: #D1D4DC;
    }
    /* 側邊欄背景 */
    [data-testid="stSidebar"] {
        background-color: #1E222D;
    }
    /* 標題與文字顏色 */
    h1, h2, h3, h4, h5, h6, p, span, div, label {
        color: #D1D4DC !important;
    }
    /* 強調按鈕風格 (TradingView Blue) */
    .stButton>button {
        background-color: #2962FF;
        color: white !important;
        border: none;
        border-radius: 4px;
        transition: 0.3s;
    }
    .stButton>button:hover {
        background-color: #1E4BD8;
        border: none;
    }
    /* 下拉選單與輸入框風格 */
    .stSelectbox div[data-baseweb="select"], .stDateInput div[data-baseweb="input"] {
        background-color: #2A2E39;
        color: #D1D4DC;
        border-color: #2A2E39;
    }
    /* 分隔線 */
    hr {
        border-color: #2B2B43;
    }
</style>
""", unsafe_allow_html=True)

# 定義 TradingView 的 Plotly 統一版型
tv_layout = dict(
    template="plotly_dark",
    paper_bgcolor="#131722",
    plot_bgcolor="#131722",
    font=dict(color="#D1D4DC", size=13),
    xaxis=dict(showgrid=True, gridcolor="#2B2B43", linecolor="#2B2B43", zeroline=False),
    yaxis=dict(showgrid=True, gridcolor="#2B2B43", linecolor="#2B2B43", zeroline=False),
    margin=dict(l=20, r=20, t=35, b=20)
)

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
# 2. 效能優化引擎 (快取歷史持股與無縫股價拼接)
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

@st.cache_data(ttl=3600)
def get_bulk_prices(sids, start_dt, end_dt):
    """【終極修正版】雙向平行下載並無縫拼接 .TW 與 .TWO，解決斷層問題"""
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
            if not s_data.empty: return s_data
        return None

    def extract_ticker(df_all, ticker):
        if df_all is None or df_all.empty: return pd.DataFrame()
        if isinstance(df_all.columns, pd.MultiIndex):
            if ticker in df_all.columns.get_level_values(1):
                return df_all.xs(ticker, level=1, axis=1).copy()
            return pd.DataFrame()
        return df_all.copy()

    # 平行下載兩種市場的可能性
    df_tw = yf.download(tickers_tw, start=start_dt, end=end_dt, progress=False, auto_adjust=True)
    df_two = yf.download(tickers_two, start=start_dt, end=end_dt, progress=False, auto_adjust=True)
    
    for sid in sids:
        s_tw = extract_ticker(df_tw, f"{sid}.TW")
        s_two = extract_ticker(df_two, f"{sid}.TWO")
        
        cleaned_tw = clean_and_format_price_df(s_tw)
        cleaned_two = clean_and_format_price_df(s_two)
        
        # 完美拼接：如果轉板了，兩段歷史會被組合在一起，絕不漏接
        if cleaned_tw is not None and cleaned_two is not None:
            price_map[sid] = cleaned_tw.combine_first(cleaned_two)
        elif cleaned_tw is not None:
            price_map[sid] = cleaned_tw
        elif cleaned_two is not None:
            price_map[sid] = cleaned_two

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
    
    price_map = get_bulk_prices([sid], chart_start, datetime.now() + timedelta(days=1))
    df_chart_price = price_map.get(sid, pd.DataFrame())
    
    if df_chart_price.empty:
        st.error(f"無法取得 {sid} {name} 的股價資料")
        return

    dates, cost_line, shares_series, diff_series = calculate_avg_cost_optimized(df_history, sid, df_chart_price)
    amounts = diff_series * df_chart_price['Close'].values
    str_dates = dates.strftime('%Y-%m-%d')

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True, 
        row_heights=[0.6, 0.2, 0.2], vertical_spacing=0.05,
        subplot_titles=(f"<b>{sid} {name} 股價與成本</b>", "<b>持股水位</b>", "<b>每日增減金額</b>")
    )
    
    # K線：紅漲綠跌 (TradingView 高對比度色彩)
    fig.add_trace(go.Candlestick(
        x=str_dates, open=df_chart_price['Open'], high=df_chart_price['High'],
        low=df_chart_price['Low'], close=df_chart_price['Close'], name='股價',
        increasing_line_color='#EF5350', increasing_fillcolor='#EF5350',
        decreasing_line_color='#26A69A', decreasing_fillcolor='#26A69A'
    ), row=1, col=1)
    
    # 成本線：採用 TradingView 黃金分割色
    fig.add_trace(go.Scatter(
        x=str_dates, y=cost_line, mode='lines', 
        line=dict(color='#FCCA46', width=2, dash='dot'), name='981成本'
    ), row=1, col=1)
    
    # 持股數：TradingView 藍色漸層
    fig.add_trace(go.Scatter(
        x=str_dates, y=shares_series, mode='lines+markers',
        fill='tozeroy', line=dict(color='#2962FF', width=2), fillcolor='rgba(41, 98, 255, 0.1)', name='持股數'
    ), row=2, col=1)
    
    # 增減金額：紅買綠賣
    colors = ['#EF5350' if x > 0 else '#26A69A' for x in amounts]
    fig.add_trace(go.Bar(
        x=str_dates, y=amounts, marker_color=colors, name='淨買賣額'
    ), row=3, col=1)
    
    fig.update_layout(**tv_layout, height=800, xaxis_rangeslider_visible=False)
    fig.update_xaxes(type='category', nticks=10) 
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
        
        # 透過強化的引擎，確保 00981A 與 00981 無論歷史如何轉板，都能完美抓出全段
        etf_price_map = get_bulk_prices(["00981A", "00981"], start_d, datetime.now() + timedelta(days=1))
        
        df_etf_A = etf_price_map.get("00981A", pd.DataFrame())
        df_etf_B = etf_price_map.get("00981", pd.DataFrame())
        
        if not df_etf_A.empty and not df_etf_B.empty:
            df_etf = df_etf_A.combine_first(df_etf_B)
        elif not df_etf_A.empty:
            df_etf = df_etf_A
        else:
            df_etf = df_etf_B
        
        if not df_etf.empty:
            df_cw = get_etf_cash_history(df_files)
            df_etf_comb = df_etf.join(df_cw, how='left')
            df_etf_comb['Cash_Weight'] = df_etf_comb['Cash_Weight'].ffill().fillna(0)
            
            str_dates = df_etf_comb.index.strftime('%Y-%m-%d')

            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05,
                subplot_titles=("<b>00981a K線</b>", "<b>現金權重走勢 (%)</b>")
            )
            
            fig.add_trace(go.Candlestick(
                x=str_dates, open=df_etf_comb['Open'], high=df_etf_comb['High'],
                low=df_etf_comb['Low'], close=df_etf_comb['Close'], name='K線',
                increasing_line_color='#EF5350', increasing_fillcolor='#EF5350',
                decreasing_line_color='#26A69A', decreasing_fillcolor='#26A69A'
            ), row=1, col=1)
            
            fig.add_trace(go.Scatter(
                x=str_dates, y=df_etf_comb['Cash_Weight'], mode='lines', 
                line=dict(color='#2962FF', width=2), fill='tozeroy', fillcolor='rgba(41, 98, 255, 0.1)', name='現金權重'
            ), row=2, col=1)

            fig.update_layout(**tv_layout, height=500, xaxis_rangeslider_visible=False)
            fig.update_xaxes(type='category', nticks=10)
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
            st.dataframe(
                df_show.style.format({'持股數': '{:,.0f}'}), 
                use_container_width=True, height=400, hide_index=True, key="dashboard_weight_table"
            )

    st.divider()
    st.subheader("⚠️ 股價跌破 ETF 成本線")
    
    if st.button("一鍵極速分析", type="primary", key="btn_calc_cost"):
        st.session_state['run_dashboard_analysis'] = True

    if st.session_state.get('run_dashboard_analysis'):
        report_data = []
        sids = df_latest['ID'].tolist()
        
        with st.spinner("正在計算成本分析..."):
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
                            report_data.append({
                                "代號": sid, "名稱": name, "現價": round(curr_price, 2),
                                "981成本": round(curr_cost, 2), "帳面損益": diff_pct
                            })
        
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
                
                color = "#26A69A" if row['帳面損益'] < 0 else "#EF5350"
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
    
    if curr_record.empty:
        st.warning(f"無 {pick_date} 資料。")
    else:
        curr_idx = curr_record.index[0]
        prev_idx = curr_idx - 1
        
        if prev_idx < 0:
            st.warning("這是第一筆資料，無前一日可比較。")
        else:
            path_curr = curr_record.iloc[0]['path']
            path_prev = df_files.iloc[prev_idx]['path']
            
            df_t = parse_excel_holding(path_curr)
            df_y = parse_excel_holding(path_prev)
            
            m = pd.merge(df_y[['ID', 'Name', 'Shares_num']], 
                         df_t[['ID', 'Name', 'Shares_num', 'Weight_str', 'Weight_num']], 
                         on='ID', how='outer', suffixes=('_old', '_new'))
            
            m['Name'] = m['Name_new'].combine_first(m['Name_old']).fillna("未知")
            m = m.fillna(0)
            m['股數變化'] = m['Shares_num_new'] - m['Shares_num_old']
            
            df_change = m[(m['Shares_num_old'] != 0) | (m['Shares_num_new'] != 0)].copy()
            
            sids_change = df_change[df_change['股數變化'] != 0]['ID'].tolist()
            price_map = {}
            if sids_change:
                dl_start = pick_date_ts - timedelta(days=7) 
                bulk_p_map = get_bulk_prices(sids_change, dl_start, pick_date_ts + timedelta(days=1))
                
                for sid in sids_change:
                    s_data = bulk_p_map.get(sid, pd.DataFrame())
                    if not s_data.empty:
                        valid_p = s_data[s_data.index <= pick_date_ts]
                        if not valid_p.empty:
                            price_map[sid] = valid_p['Close'].iloc[-1]
                        else: price_map[sid] = 0
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
                    '股數變化': int(row['股數變化']), '差額': diff_txt, 
                    'Weight_num': row['Weight_num'], 
                    'Is_Zero': int(row['Shares_num_new']) == 0
                })
            
            df_display = pd.DataFrame(display_rows)
            
            if not df_display.empty:
                df_display = df_display.sort_values(by=['Is_Zero', 'Weight_num'], ascending=[True, False])
                
                st.subheader("📋 持股變化表")
                st.dataframe(
                    df_display.drop(columns=['Weight_num', 'Is_Zero']).style.applymap(
                        lambda v: 'color: #EF5350' if v > 0 else 'color: #26A69A' if v < 0 else '', 
                        subset=['股數變化']
                    ).format({'前股數': '{:,}', '今股數': '{:,}', '股數變化': '{:,}'}),
                    use_container_width=True, hide_index=True, key="daily_change_table"
                )
                
                st.divider()
                st.subheader("📈 變動個股技術分析")
                
                changed_stocks = df_display[df_display['股數變化'] != 0]
                if not changed_stocks.empty:
                    target_label = st.selectbox(
                        "選擇有變動的股票:", 
                        changed_stocks['股票代號'] + " " + changed_stocks['股票名稱'],
                        key="daily_stock_selector"
                    )
                    
                    if target_label:
                        tsid = target_label.split(" ")[0]
                        tname = target_label.split(" ")[1]
                        draw_analysis_chart(tsid, tname, df_history_cache, "daily")
                else:
                    st.info("當日無任何變動股可供繪圖。")
            else:
                st.info("該日持股無變化。")
