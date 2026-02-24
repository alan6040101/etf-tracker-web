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

# ---------------- 0. 核心工具函式 ----------------

def extract_date_from_filename(filename):
    """從檔名提取日期 (支援 20240101 或 1130101 格式)"""
    digits = re.sub(r'\D', '', os.path.basename(filename))
    # 找 202xxxxx
    match8 = re.search(r'(20\d{6})', digits)
    if match8: return match8.group(1)
    # 找 11xxxxx (民國年)
    match7 = re.search(r'(1\d{2})(\d{2})(\d{2})', digits)
    if match7:
        year = int(match7.group(1)) + 1911
        return f"{year}{match7.group(2)}{match7.group(3)}"
    return None

def clean_number(val):
    if pd.isna(val) or str(val).strip() == "": return 0.0
    s = str(val).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
    try:
        return float(s)
    except:
        return 0.0

@st.cache_data(ttl=600)
def sync_00981a_data():
    """從 GitHub 同步資料 (增加錯誤處理)"""
    repo_url = "https://github.com/alan6040101/00981a-data.git"
    dir_name = "data_00981a"
    
    # 移除舊資料夾 (忽略錯誤)
    if os.path.exists(dir_name):
        try: shutil.rmtree(dir_name)
        except: pass
    
    # 重新 Clone
    try:
        subprocess.run(["git", "clone", repo_url, dir_name, "-q"], check=True)
    except subprocess.CalledProcessError:
        # 如果 Clone 失敗，但資料夾存在，嘗試用既有的
        if not os.path.exists(dir_name):
            return pd.DataFrame()

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
    """讀取 Excel (增強版: 暴力搜尋表頭)"""
    try:
        # 先讀前 30 行，不設 Header
        df_raw = pd.read_excel(path, header=None, nrows=30)
    except:
        return pd.DataFrame()

    target_idx = -1
    # 尋找含有關鍵字的列
    for idx, row in df_raw.iterrows():
        row_str = "".join([str(x) for x in row.fillna("")])
        # 只要同一行出現 '名稱' 且出現 '股數'/'單位數'/'Shares' 就算找到
        if ('名稱' in row_str or 'Name' in row_str) and \
           ('股數' in row_str or 'Shares' in row_str or '單位數' in row_str):
            target_idx = idx
            break
    
    if target_idx == -1: return pd.DataFrame() # 找不到表頭

    try:
        # 重新讀取正確的 Header
        df = pd.read_excel(path, header=target_idx)
        df = df.dropna(how='all', axis=1) # 移除全空欄
        
        # 欄位對應
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
            
        # 清理資料
        df['ID'] = df['ID'].astype(str).str.replace(r'\.TW|\.TWO', '', regex=True).str.strip()
        df['Shares'] = df['Shares'].apply(clean_number)
        
        if 'Weight' in df.columns:
            df['Weight_Val'] = df['Weight'].apply(clean_number)
        else:
            df['Weight_Val'] = 0.0
            
        if 'Name' not in df.columns: df['Name'] = df['ID']

        return df[['ID', 'Name', 'Shares', 'Weight_Val']]
        
    except Exception:
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def get_stock_price_robust(ticker_list, period="1y"):
    """穩健的股價下載函式 (處理 Streamlit Cloud 問題)"""
    if not ticker_list: return pd.DataFrame()
    
    try:
        # threads=False 在 Streamlit Cloud 比較穩定
        data = yf.download(ticker_list, period=period, group_by='ticker', 
                          auto_adjust=True, threads=False, progress=False)
        return data
    except:
        return pd.DataFrame()

def calculate_cost_line(history_files, target_sid, price_df):
    """計算成本線邏輯"""
    # 確保按日期 舊 -> 新
    sorted_files = history_files.sort_values('date', ascending=True)
    
    # 建立持股時間序列
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
    
    if price_df.empty: return [], [], [], []
    
    # 對齊
    price_df.index = pd.to_datetime(price_df.index)
    aligned_shares = df_holdings.reindex(price_df.index, method='ffill').fillna(0)['Shares']
    aligned_diff = aligned_shares.diff().fillna(0)
    
    dates = price_df.index
    closes = price_df['Close']
    
    costs = []
    avg_cost = 0.0
    total_cost = 0.0
    curr_shares = 0
    
    for p, s, d in zip(closes, aligned_shares, aligned_diff):
        if s <= 10: # 持股歸零
            avg_cost = 0; total_cost = 0; costs.append(None)
            curr_shares = 0
        else:
            if curr_shares <= 10: # 新建倉
                avg_cost = p; total_cost = s * p
            else:
                if d > 0: total_cost += d * p # 買進
                elif d < 0: total_cost += d * avg_cost # 賣出
            
            if s > 0: avg_cost = total_cost / s
            else: avg_cost = 0
            
            costs.append(avg_cost)
            curr_shares = s
            
    return costs, dates, aligned_shares, aligned_diff

# ---------------- 功能 1: 總覽 ----------------

def render_overview(file_list):
    st.header("📈 00981a 總覽")
    
    if file_list.empty:
        st.error("❌ 無法找到任何資料檔案。請檢查 GitHub 連結是否正確，或檔案是否為 .xlsx 格式。")
        return

    col1, col2 = st.columns([2, 1])
    
    # --- 1. ETF 股價圖 (含 fallback 機制) ---
    with col1:
        st.subheader("00981a 近一年走勢")
        
        # 優先嘗試使用者指定的代號
        target_ticker = "00981A.TW"
        etf_price = get_stock_price_robust([target_ticker])
        
        # 如果抓不到，嘗試標準代號 00981.TW (備援)
        if etf_price.empty:
            # st.warning(f"⚠️ 無法取得 {target_ticker} 資料，嘗試使用 00981.TW...")
            target_ticker = "00981.TW"
            etf_price = get_stock_price_robust([target_ticker])
        
        if not etf_price.empty:
            # 處理 MultiIndex (如果只有一個 ticker，可能是單層或多層)
            p_df = etf_price
            if isinstance(etf_price.columns, pd.MultiIndex):
                if target_ticker in etf_price.columns.levels[0]:
                    p_df = etf_price[target_ticker]
            
            fig = go.Figure(data=[go.Candlestick(x=p_df.index,
                            open=p_df['Open'], high=p_df['High'],
                            low=p_df['Low'], close=p_df['Close'])])
            fig.update_layout(height=400, xaxis_rangeslider_visible=False, template="plotly_white", margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.error(f"❌ 無法取得 ETF 股價 (已嘗試 00981A.TW 與 00981.TW)。請稍後再試。")

    # --- 2. 讀取最新持股 ---
    latest_file = file_list.iloc[0]
    st.write(f"📅 最新持股日期: **{latest_file['date']}**")
    
    df_holdings = read_excel_holdings(latest_file['path'])
    
    if df_holdings.empty:
        st.error(f"❌ 無法解析檔案: {os.path.basename(latest_file['path'])}。請檢查 Excel 內容是否包含 [代號, 名稱, 股數] 等欄位。")
        # 列出一些除錯資訊
        try:
            raw = pd.read_excel(latest_file['path'], nrows=5)
            st.write("檔案前 5 行內容 (用於除錯):", raw)
        except: pass
        return

    # --- 3. 計算成分股 ---
    with st.spinner("計算成分股成本與現價中..."):
        sids = df_holdings['ID'].tolist()
        tickers = [f"{s}.TW" for s in sids]
        
        # 一次下載所有歷史
        all_hist = get_stock_price_robust(tickers)
        
        curr_price_map = {}
        cost_map = {}
        
        for sid in sids:
            t = f"{sid}.TW"
            p_df = pd.DataFrame()
            
            # 提取個股 DataFrame
            try:
                if not all_hist.empty:
                    if isinstance(all_hist.columns, pd.MultiIndex):
                        if t in all_hist.columns.levels[0]:
                            p_df = all_hist[t]
                    else:
                        # 只有一檔股票時
                        p_df = all_hist
            except: pass
            
            if not p_df.empty:
                # 現價
                curr_price_map[sid] = p_df['Close'].iloc[-1]
                # 成本
                c_line, _, _, _ = calculate_cost_line(file_list, sid, p_df)
                valid = [x for x in c_line if x is not None]
                cost_map[sid] = valid[-1] if valid else 0
            else:
                curr_price_map[sid] = 0
                cost_map[sid] = 0

    df_holdings['現價'] = df_holdings['ID'].map(curr_price_map)
    df_holdings['估算成本'] = df_holdings['ID'].map(cost_map)
    
    # 避免除以零
    df_holdings['損益率(%)'] = df_holdings.apply(
        lambda x: ((x['現價'] - x['估算成本'])/x['估算成本']*100) if x['估算成本'] > 0 else 0, 
        axis=1
    )
    df_holdings['市值'] = df_holdings['Shares'] * df_holdings['現價']

    # --- 4. 警示表格 (低於成本) ---
    with col2:
        st.subheader("⚠️ 股價低於成本")
        below_cost = df_holdings[df_holdings['損益率(%)'] < 0].sort_values('損益率(%)')
        st.metric("低於成本檔數", f"{len(below_cost)} 檔")
        
        if not below_cost.empty:
            st.dataframe(
                below_cost[['ID', 'Name', '現價', '估算成本', '損益率(%)']].style.format({
                    '現價': '{:.2f}', '估算成本': '{:.2f}', '損益率(%)': '{:.2f}'
                }).applymap(lambda v: 'color: green' if v < 0 else '', subset=['損益率(%)']),
                hide_index=True, use_container_width=True
            )
        else:
            st.success("無低於成本個股")

    # --- 5. 完整清單 ---
    st.subheader("📋 最新完整持股清單")
    df_show = df_holdings.sort_values('Weight_Val', ascending=False)
    
    st.dataframe(
        df_show[['ID', 'Name', 'Shares', 'Weight_Val', '現價', '估算成本', '損益率(%)']].style.format({
            'Shares': '{:,.0f}', 'Weight_Val': '{:.2f}', 
            '現價': '{:.2f}', '估算成本': '{:.2f}', '損益率(%)': '{:.2f}'
        }).background_gradient(subset=['損益率(%)'], cmap='RdYlGn', vmin=-10, vmax=10),
        use_container_width=True, hide_index=True,
        column_config={"ID": "代號", "Name": "名稱", "Shares": "股數", "Weight_Val": "權重(%)"}
    )

# ---------------- 功能 2: 每日變化 ----------------

def render_daily_change(file_list):
    st.header("📅 每日持倉變化")
    
    if file_list.empty:
        st.error("無資料可供分析")
        return

    dates_str = file_list['date'].tolist()
    # 轉換為 date 物件
    valid_dates = []
    for d in dates_str:
        try:
            valid_dates.append(datetime.strptime(d, "%Y%m%d").date())
        except: pass
        
    if not valid_dates:
        st.error("無法解析檔案日期格式")
        return

    col_sel, col_info = st.columns([1, 3])
    with col_sel:
        pick_date = st.date_input("選擇查詢日期", valid_dates[0])
        pick_date_str = pick_date.strftime("%Y%m%d")
    
    if pick_date_str not in dates_str:
        st.warning(f"❌ {pick_date_str} 當日無資料")
        return

    # 找當日與前一日
    idx = dates_str.index(pick_date_str)
    curr_file = file_list.iloc[idx]
    prev_file = file_list.iloc[idx+1] if idx + 1 < len(file_list) else None

    with col_info:
        if prev_file:
            st.info(f"比較: {prev_file['date']} ⮕ {curr_file['date']}")
        else:
            st.warning("這是第一筆資料，無法比較")
            return

    df_curr = read_excel_holdings(curr_file['path'])
    df_prev = read_excel_holdings(prev_file['path'])
    
    if df_curr.empty or df_prev.empty:
        st.error("讀取 Excel 失敗")
        return

    # 合併
    merged = pd.merge(df_prev[['ID', 'Name', 'Shares']], 
                      df_curr[['ID', 'Name', 'Shares', 'Weight_Val']], 
                      on='ID', how='outer', suffixes=('_old', '_new'))
    
    merged['Name'] = merged['Name_new'].combine_first(merged['Name_old']).fillna("未知")
    merged = merged.fillna(0)
    merged['股數變化'] = merged['Shares_new'] - merged['Shares_old']
    
    # 下載當日股價算金額
    sids_change = merged[merged['股數變化'] != 0]['ID'].tolist()
    price_map = {}
    
    if sids_change:
        with st.spinner("下載變動個股資料..."):
            target_dt = pd.to_datetime(pick_date_str)
            tickers = [f"{s}.TW" for s in sids_change]
            # 用較小區間加快速度
            data = get_stock_price_robust(tickers, period="1mo")
            
            for sid in sids_change:
                t = f"{sid}.TW"
                p = 0
                try:
                    if isinstance(data.columns, pd.MultiIndex):
                        if t in data.columns.levels[0]: df_p = data[t]
                        else: df_p = pd.DataFrame()
                    else: df_p = data
                    
                    # 找最接近該日的股價
                    df_p = df_p[df_p.index <= target_dt]
                    if not df_p.empty: p = df_p['Close'].iloc[-1]
                except: pass
                price_map[sid] = p
                
    merged['當日股價'] = merged['ID'].map(price_map).fillna(0)
    merged['增減金額'] = merged['股數變化'] * merged['當日股價']
    
    # 顯示表格
    changes = merged[merged['股數變化'] != 0].copy().sort_values('Weight_Val', ascending=False)
    
    st.subheader("📊 增減明細")
    if not changes.empty:
        def style_change(v):
            if v > 0: return 'color: red'
            elif v < 0: return 'color: green'
            return ''
            
        st.dataframe(
            changes[['ID', 'Name', 'Shares_old', 'Shares_new', '股數變化', '增減金額', 'Weight_Val']].style.format({
                'Shares_old': '{:,.0f}', 'Shares_new': '{:,.0f}',
                '股數變化': '{:+,.0f}', '增減金額': '{:,.0f}', 'Weight_Val': '{:.2f}'
            }).map(style_change, subset=['股數變化', '增減金額']),
            use_container_width=True,
            column_config={"Shares_old": "前股數", "Shares_new": "今股數", "Weight_Val": "權重(%)"}
        )
        
        # --- 詳細圖表 ---
        st.divider()
        st.subheader("📈 個股詳情")
        
        sel_sid = st.selectbox("選擇股票查看", changes['ID'].tolist(), 
                               format_func=lambda x: f"{x} {changes[changes['ID']==x]['Name'].values[0]}")
        
        if sel_sid:
            with st.spinner(f"繪製 {sel_sid} 圖表中..."):
                t_sid = f"{sel_sid}.TW"
                df_hist = get_stock_price_robust([t_sid], period="2y") # 抓長一點算成本
                
                cost_vals, dates, shares_vals, diff_vals = calculate_cost_line(file_list, sel_sid, df_hist)
                
                if not dates.empty:
                    # 修正: 確保 amounts 沒有 NaN
                    amounts = (diff_vals * df_hist['Close']).fillna(0)
                    
                    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                                       row_heights=[0.5, 0.25, 0.25],
                                       subplot_titles=("股價 vs 成本", "持股水位", "增減金額"))
                    
                    fig.add_trace(go.Candlestick(x=dates, open=df_hist['Open'], high=df_hist['High'],
                                                low=df_hist['Low'], close=df_hist['Close'], name="股價"), row=1, col=1)
                    fig.add_trace(go.Scatter(x=dates, y=cost_vals, mode='lines', 
                                            line=dict(color='orange', dash='dot'), name="成本"), row=1, col=1)
                    
                    fig.add_trace(go.Scatter(x=dates, y=shares_vals, mode='lines', fill='tozeroy',
                                            line=dict(color='blue'), name="持股"), row=2, col=1)
                    
                    # 修正顏色
                    colors = ['red' if x > 0 else 'green' for x in amounts]
                    fig.add_trace(go.Bar(x=dates, y=amounts, marker_color=colors, name="金額"), row=3, col=1)
                    
                    fig.update_layout(height=800, xaxis_rangeslider_visible=False, showlegend=False, template="plotly_white")
                    
                    # 縮放最近 6 個月
                    last = dates[-1]
                    start = last - timedelta(days=180)
                    fig.update_xaxes(range=[start, last], row=3, col=1)
                    
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.warning("無歷史股價資料")

    else:
        st.info("該日無持股變動")

# ---------------- 主程式 ----------------

def main():
    st.sidebar.title("🚀 00981a 戰情室")
    
    with st.sidebar:
        st.write("🔄 資料同步中...")
        df_files = sync_00981a_data()
        
        if df_files.empty:
            st.error("無法同步 GitHub 資料，請確認網路或 Repo 權限。")
            return
            
        st.success(f"完成！共 {len(df_files)} 份資料")
        menu = st.radio("選單", ["總覽 (Overview)", "每日持倉變化 (Daily Changes)"])
        st.markdown("---")

    if menu == "總覽 (Overview)":
        render_overview(df_files)
    elif menu == "每日持倉變化 (Daily Changes)":
        render_daily_change(df_files)

if __name__ == "__main__":
    main()
