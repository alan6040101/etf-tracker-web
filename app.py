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
import io
import shutil
import subprocess
import warnings

# 設定頁面寬度與標題
st.set_page_config(page_title="ETF 追蹤儀表板", layout="wide")
st.title("📊 ETF 持股變化與成本追蹤儀表板")

warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------- 0. 工具函式 ----------------

def extract_date_from_filename(filename):
    digits = re.sub(r'\D', '', filename)
    match8 = re.search(r'20\d{6}', digits)
    if match8: return match8.group(0)
    match7 = re.search(r'(1\d{2})(\d{2})(\d{2})', digits)
    if match7:
        year = int(match7.group(1)) + 1911
        return f"{year}{match7.group(2)}{match7.group(3)}"
    return None

@st.cache_data(ttl=3600)  # 快取 git clone 結果，避免每次重新整理都重抓
def sync_all_data(repos):
    all_files = {}
    progress_bar = st.progress(0)
    status_text = st.empty()
    
    total_repos = len(repos)
    for idx, (etf_name, url) in enumerate(repos.items()):
        status_text.text(f"正在同步 {etf_name} 資料...")
        dir_name = f"data_{etf_name}"
        
        # 清除舊資料 (Streamlit Cloud 環境可能會有權限問題，使用 try-except)
        if os.path.exists(dir_name):
            try:
                shutil.rmtree(dir_name)
            except Exception as e:
                # 如果無法刪除，嘗試直接 pull (簡化處理，這裡假設直接重新 clone)
                pass

        # 使用 subprocess 取代 !git clone
        try:
            subprocess.run(["git", "clone", url, dir_name, "-q"], check=True)
        except subprocess.CalledProcessError:
            st.error(f"無法複製 {etf_name} 的資料庫，請檢查 URL。")
            continue

        files = glob.glob(f"{dir_name}/**/*.xlsx", recursive=True)
        local_list = []
        for f in files:
            if os.path.basename(f).startswith("~$"): continue
            d_str = extract_date_from_filename(f)
            if d_str:
                # 為了避免改檔名權限問題，直接讀取原始路徑，在 DataFrame 紀錄日期
                local_list.append({'date': d_str, 'path': f})
        
        if local_list:
            all_files[etf_name] = pd.DataFrame(local_list).sort_values('date')
        else:
            all_files[etf_name] = pd.DataFrame()
            
        progress_bar.progress((idx + 1) / total_repos)
    
    status_text.text("資料同步完成！")
    progress_bar.empty()
    return all_files

def clean_number(val):
    if pd.isna(val) or str(val).strip() == "": return None
    s = str(val).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
    try:
        return float(s)
    except:
        return None

def find_header_and_map_cols(path):
    try:
        df_raw = pd.read_excel(path, header=None, nrows=30)
    except: return pd.DataFrame()

    target_header_idx = -1
    for idx, row in df_raw.iterrows():
        row_str = "".join([str(x) for x in row.fillna("")])
        if any(k in row_str for k in ['代號', '代碼', 'ID', 'Code']) and \
           any(k in row_str for k in ['名稱', '股票', 'Name', 'Security']):
            target_header_idx = idx
            break

    if target_header_idx == -1: return pd.DataFrame()

    df = pd.read_excel(path, header=target_header_idx)
    df = df.dropna(how='all', axis=1)

    mapping = {
        'ID': ['代號', '代碼', 'ID', 'Code'],
        'Name': ['名稱', '股票', 'Name', 'Security'],
        'Shares': ['股數', '持股', 'Shares', 'Units'],
        'Weight': ['權重', '比例', 'Weight', '%']
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
            df['Shares_n'] = df['Shares'].apply(lambda x: clean_number(x) or 0.0)
        else:
            df['Shares_n'] = 0.0

        if 'Weight' in df.columns:
            df['Weight_num'] = df['Weight'].apply(lambda x: clean_number(x) or 0.0)
            df['Weight_str'] = df['Weight'].astype(str)
        else:
            df['Weight_num'] = 0.0
            df['Weight_str'] = "0%"

        return df[['ID', 'Name', 'Shares_n', 'Weight_str', 'Weight_num']]

    return pd.DataFrame()

def format_money_label(val):
    if val is None: return "0"
    abs_val = abs(val)
    if abs_val >= 100000000:
        return f"{val/100000000:,.2f}億"
    elif abs_val >= 10000:
        return f"{val/10000:,.2f}萬"
    return f"{int(round(val)):,}"

def get_history_aligned(df_files, target_sid, price_df):
    data = []
    # 這裡可以優化：不需要每次重讀 Excel，但在 Streamlit 中為了記憶體著想，我們還是維持現狀，但加入簡單的 cache 機制會更好
    for _, row in df_files.iterrows():
        df_step = find_header_and_map_cols(row['path'])
        shares = 0
        if not df_step.empty:
            match = df_step[df_step['ID'] == str(target_sid)]
            shares = match['Shares_n'].values[0] if not match.empty else 0
        data.append({'Date': pd.to_datetime(row['date']), 'Shares': shares})

    df_h = pd.DataFrame(data).sort_values('Date')
    return df_h

@st.cache_data(ttl=43200) # 快取股價資料 12 小時
def batch_download_stocks(sids, start_date, end_date):
    if not sids: return {}
    # st.write(f"⏳ 正在下載 {len(sids)} 檔股票資料...")
    price_cache = {}
    tickers_tw = [f"{sid}.TW" for sid in sids]

    try:
        data_tw = yf.download(tickers_tw, start=start_date, end=end_date, progress=False, group_by='ticker', auto_adjust=True, threads=True)
    except Exception:
        data_tw = pd.DataFrame()

    missing_sids = []

    if len(tickers_tw) == 1:
         sid = sids[0]
         if not data_tw.empty: price_cache[sid] = data_tw
         else: missing_sids.append(sid)
    else:
        for sid in sids:
            ticker = f"{sid}.TW"
            try:
                # yfinance 結構改變，有時是 multi-index
                if ticker in data_tw.columns.levels[0] if isinstance(data_tw.columns, pd.MultiIndex) else ticker in data_tw:
                    df_sid = data_tw[ticker]
                    if not df_sid.isnull().all().all():
                        price_cache[sid] = df_sid.dropna(how='all')
                    else:
                        missing_sids.append(sid)
                else:
                    missing_sids.append(sid)
            except KeyError: missing_sids.append(sid)

    if missing_sids:
        tickers_two = [f"{sid}.TWO" for sid in missing_sids]
        try:
            data_two = yf.download(tickers_two, start=start_date, end=end_date, progress=False, group_by='ticker', auto_adjust=True, threads=True)
            if len(tickers_two) == 1:
                sid = missing_sids[0]
                if not data_two.empty: price_cache[sid] = data_two
            else:
                for sid in missing_sids:
                    ticker = f"{sid}.TWO"
                    try:
                        if ticker in data_two.columns.levels[0] if isinstance(data_two.columns, pd.MultiIndex) else ticker in data_two:
                            df_sid = data_two[ticker]
                            if not df_sid.isnull().all().all(): price_cache[sid] = df_sid.dropna(how='all')
                    except KeyError: pass
        except: pass

    return price_cache

def build_etf_summary(path_t, path_o):
    # 重構內部函式以適應 Streamlit
    def get_etf_data(path):
        data = { 'units': 0, 'cash_amt': 0, 'cash_wgt': 0, 'nav': 0 }
        try:
            df_raw = pd.read_excel(path, header=None)

            def get_nums(row):
                nums = []
                for cell in row:
                    if pd.isna(cell): continue
                    s = str(cell).replace(',', '').replace('%', '').replace('$', '').replace('TWD', '').strip()
                    matches = re.findall(r'[-+]?\d+\.?\d*', s)
                    for m in matches:
                        try:
                            v = float(m)
                            if v != 0 and not (20200000 < v < 20300000):
                                nums.append(v)
                        except: pass
                return nums

            for i in range(len(df_raw)):
                row_str = " ".join(df_raw.iloc[i].astype(str).fillna(""))
                nums = []
                for j in range(5):
                    if i + j < len(df_raw): nums += get_nums(df_raw.iloc[i+j])

                if any(k in row_str for k in ["每單位淨值", "NAV", "Net Asset Value", "淨值"]):
                    if data['nav'] == 0:
                        for n in nums:
                            if 5 < n < 5000: data['nav'] = n; break

                if any(k in row_str for k in ["流通在外", "受益權單位", "Units", "單位數"]):
                    if data['units'] == 0:
                        for n in nums:
                            if n > 10000: data['units'] = n; break

                if any(k in row_str for k in ["現金", "Cash", "TWD"]):
                    for n in nums:
                        if abs(n) > 500 and data['cash_amt'] == 0:
                            data['cash_amt'] = n
                        elif -100 <= n <= 100 and n != 0 and data['cash_wgt'] == 0:
                            data['cash_wgt'] = n
        except: pass
        return data

    dt = get_etf_data(path_t)
    do = get_etf_data(path_o)

    rows = []
    rows.append({
        "項目": "流通在外單位數",
        "比較日": f"{do['units']:,.0f}",
        "今日": f"{dt['units']:,.0f}",
        "變化值": f"{dt['units'] - do['units']:,.0f}"
    })
    rows.append({
        "項目": "現金(金額)",
        "比較日": f"{do['cash_amt']:,.0f}",
        "今日": f"{dt['cash_amt']:,.0f}",
        "變化值": f"{dt['cash_amt'] - do['cash_amt']:,.0f}"
    })
    rows.append({
        "項目": "現金(權重)",
        "比較日": f"{do['cash_wgt']:,.2f}%",
        "今日": f"{dt['cash_wgt']:,.2f}%",
        "變化值": f"{dt['cash_wgt'] - do['cash_wgt']:,.2f}%"
    })
    rows.append({
        "項目": "每單位淨值",
        "比較日": f"{do['nav']:,.4f}",
        "今日": f"{dt['nav']:,.4f}",
        "變化值": f"{dt['nav'] - do['nav']:,.4f}"
    })

    return pd.DataFrame(rows)

# ---------------- 繪圖邏輯 (改為單一股票渲染) ----------------

def plot_single_stock(sid, data_collection):
    data = data_collection.get(sid)
    if not data: return
    
    sname = data['sname']
    price_df = data['price_df']
    h981_raw = data['h981_raw']
    h991_raw = data['h991_raw']

    fig = make_subplots(
        rows=5, cols=1, shared_xaxes=True, vertical_spacing=0.03,
        subplot_titles=("<b>K線與成本</b>", "<b>00981a 持股水位</b>", "<b>00981a 每日增減金額</b>",
                        "<b>00991a 持股水位</b>", "<b>00991a 每日增減金額</b>"),
        row_heights=[0.5, 0.125, 0.125, 0.125, 0.125]
    )

    # ... (保留原有的計算邏輯) ...
    valid_price = price_df[(price_df['Close'] > 0) & (price_df['Low'] > 0)].copy() if not price_df.empty else pd.DataFrame()

    if valid_price.empty:
        common_index = pd.DatetimeIndex([])
        str_dates = []
    else:
        common_index = valid_price.index
        str_dates = common_index.strftime('%Y-%m-%d').tolist()

    def align_holdings(df_raw, target_index):
        if df_raw.empty or target_index.empty:
            return pd.DataFrame({'Shares': 0, 'Diff': 0, 'Amount': 0}, index=target_index)
        df_temp = df_raw.set_index('Date').sort_index()
        df_temp = df_temp[~df_temp.index.duplicated(keep='last')]
        df_aligned = df_temp.reindex(target_index, method='ffill').fillna(0)
        df_aligned['Diff'] = df_aligned['Shares'].diff().fillna(0)
        return df_aligned

    h981_aligned = align_holdings(h981_raw, common_index)
    h991_aligned = align_holdings(h991_raw, common_index)
    
    h981_cost = []
    h991_cost = []
    
    if not valid_price.empty:
        close_prices = valid_price['Close']
        
        # 成本計算邏輯
        def calc_cost(shares_s, diff_s, close_p):
            cost_line = []
            total_cost = 0.0
            avg_cost = 0.0
            for s, d, p in zip(shares_s, diff_s, close_p):
                if s <= 0:
                    avg_cost = 0.0; total_cost = 0.0
                    cost_line.append(None)
                else:
                    if total_cost == 0 and avg_cost == 0:
                        avg_cost = p; total_cost = s * p
                    else:
                        if d > 0: total_cost += d * p
                        elif d < 0: total_cost += d * avg_cost
                        
                        if s > 0: avg_cost = total_cost / s
                        else: avg_cost = 0.0; total_cost = 0.0
                    cost_line.append(avg_cost if avg_cost > 0 else None)
            return cost_line

        h981_cost = calc_cost(h981_aligned['Shares'], h981_aligned['Diff'], close_prices)
        h991_cost = calc_cost(h991_aligned['Shares'], h991_aligned['Diff'], close_prices)
        h981_aligned['Amount'] = h981_aligned['Diff'] * close_prices
        h991_aligned['Amount'] = h991_aligned['Diff'] * close_prices
    else:
        h981_aligned['Amount'] = 0; h991_aligned['Amount'] = 0

    def get_bar_style(s_amt):
        cols = ['red' if x > 0 else 'green' for x in s_amt]
        txts = [format_money_label(x) if x != 0 else "" for x in s_amt]
        return cols, txts

    c981, t981 = get_bar_style(h981_aligned['Amount'])
    c991, t991 = get_bar_style(h991_aligned['Amount'])

    # 繪圖
    if not valid_price.empty:
        fig.add_trace(go.Candlestick(x=str_dates, open=valid_price['Open'], high=valid_price['High'],
                                     low=valid_price['Low'], close=valid_price['Close'], name=f"{sname} 價格"), row=1, col=1)
        fig.add_trace(go.Scatter(x=str_dates, y=h981_cost, name="981成本", line=dict(color='blue', dash='dot', width=1.5)), row=1, col=1)
        fig.add_trace(go.Scatter(x=str_dates, y=h991_cost, name="991成本", line=dict(color='orange', dash='dot', width=1.5)), row=1, col=1)
    
    fig.add_trace(go.Scatter(x=str_dates, y=h981_aligned['Shares'], name="981持股", line=dict(color='blue')), row=2, col=1)
    fig.add_trace(go.Bar(x=str_dates, y=h981_aligned['Amount'], marker_color=c981, text=t981, textposition='outside', name="981金額"), row=3, col=1)
    fig.add_trace(go.Scatter(x=str_dates, y=h991_aligned['Shares'], name="991持股", line=dict(color='orange')), row=4, col=1)
    fig.add_trace(go.Bar(x=str_dates, y=h991_aligned['Amount'], marker_color=c991, text=t991, textposition='outside', name="991金額"), row=5, col=1)

    fig.update_layout(height=1000, xaxis_rangeslider_visible=False, showlegend=True, template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

# ---------------- 主程式 ----------------

def main():
    # 側邊欄輸入
    with st.sidebar:
        st.header("設定")
        # 預設為今天與昨天
        today = datetime.now().date()
        default_o = today - timedelta(days=1)
        
        d_t = st.date_input("今日日期 (Target Date)", today)
        d_o = st.date_input("比較日日期 (Old Date)", default_o)
        
        run_btn = st.button("🚀 開始分析", type="primary")

    if run_btn:
        date_t = d_t.strftime('%Y%m%d')
        date_o = d_o.strftime('%Y%m%d')
        target_dt = pd.to_datetime(d_t)

        repos = {"00981a": "https://github.com/alan6040101/00981a-data.git", 
                 "00991a": "https://github.com/alan6040101/00991a-data.git"}
        
        all_files = sync_all_data(repos)
        
        # 檢查日期檔案是否存在
        file_errors = []
        etf_store = {}
        
        for etf in ["00981a", "00991a"]:
            df_f = all_files[etf]
            if df_f.empty:
                file_errors.append(f"{etf} 無資料")
                continue
                
            path_t_row = df_f[df_f['date'] == date_t]
            path_o_row = df_f[df_f['date'] == date_o]
            
            if path_t_row.empty or path_o_row.empty:
                file_errors.append(f"{etf} 找不到指定日期的檔案 (需有 {date_t} 與 {date_o})")
                continue
                
            p_t = path_t_row['path'].values[0]
            p_o = path_o_row['path'].values[0]
            
            df_t, df_o_data = find_header_and_map_cols(p_t), find_header_and_map_cols(p_o)
            
            m = pd.merge(df_o_data[['ID', 'Name', 'Shares_n']],
                         df_t[['ID', 'Name', 'Shares_n', 'Weight_str', 'Weight_num']],
                         on='ID', how='outer', suffixes=('_old', '_new'))
            
            m['Name'] = m['Name_new'].combine_first(m['Name_old']).fillna("未知")
            m = m.fillna(0)
            m['股數變化'] = m['Shares_n_new'] - m['Shares_n_old']
            m = m[(m['Shares_n_old'] != 0) | (m['Shares_n_new'] != 0)]
            
            etf_store[etf] = {'df': m, 'summary': build_etf_summary(p_t, p_o), 'rows': []}

        if file_errors:
            for err in file_errors:
                st.error(err)
            st.warning("請確認 GitHub 資料庫是否有該日期的檔案。")
            return

        # 準備資料
        all_sids = list(set(etf_store['00981a']['df']['ID']) | set(etf_store['00991a']['df']['ID']))
        sids_with_change = set(etf_store['00981a']['df'][etf_store['00981a']['df']['股數變化'] != 0]['ID']) | \
                           set(etf_store['00991a']['df'][etf_store['00991a']['df']['股數變化'] != 0]['ID'])
        
        st.info(f"總股票數: {len(all_sids)}，變動股票數: {len(sids_with_change)}")
        
        # 下載股價
        min_date_981 = pd.to_datetime(all_files['00981a']['date'].min())
        min_date_991 = pd.to_datetime(all_files['00991a']['date'].min())
        earliest_file_date = min(min_date_981, min_date_991)
        chart_start_date = earliest_file_date - timedelta(days=5)
        end_date = datetime.now() + timedelta(days=1)
        
        with st.spinner("正在下載歷史股價資料..."):
            price_cache = batch_download_stocks(all_sids, chart_start_date, end_date)

        common_buys = []
        plot_candidates = []
        
        # 計算邏輯 (保留原樣)
        for sid in all_sids:
            df_p = price_cache.get(sid, pd.DataFrame())
            if isinstance(df_p, pd.Series): df_p = df_p.to_frame()
            
            today_price = 0
            if not df_p.empty:
                p_slice = df_p[df_p.index <= target_dt]
                if not p_slice.empty: today_price = p_slice['Close'].iloc[-1]
            
            sname = "未知"
            r981 = etf_store['00981a']['df'][etf_store['00981a']['df']['ID'] == sid]
            if not r981.empty: sname = r981['Name'].iloc[0]
            else:
                r991 = etf_store['00991a']['df'][etf_store['00991a']['df']['ID'] == sid]
                if not r991.empty: sname = r991['Name'].iloc[0]

            if sid in sids_with_change:
                plot_candidates.append(sid)

            for etf in ["00981a", "00991a"]:
                m_row = etf_store[etf]['df'][etf_store[etf]['df']['ID'] == sid]
                if not m_row.empty:
                    diff_shares = m_row['股數變化'].iloc[0]
                    diff_amount = diff_shares * today_price if diff_shares != 0 else 0
                    diff_text = format_money_label(diff_amount) if diff_amount != 0 else "0"
                    
                    etf_store[etf]['rows'].append({
                        '股票代號': sid, '股票名稱': sname, '持股權重': m_row['Weight_str'].iloc[0],
                        '前股數': int(m_row['Shares_n_old'].iloc[0]), '今股數': int(m_row['Shares_n_new'].iloc[0]),
                        '股數變化': int(diff_shares), '差額': diff_text,
                        'Weight_num': m_row['Weight_num'].iloc[0]
                    })
                    
                    if etf == '00981a' and not etf_store['00991a']['df'][etf_store['00991a']['df']['ID'] == sid].empty:
                        row991 = etf_store['00991a']['df'][etf_store['00991a']['df']['ID'] == sid]
                        diff_981 = diff_shares
                        diff_991 = row991['股數變化'].iloc[0]
                        if diff_981 > 0 and diff_991 > 0:
                            amt981 = diff_981 * today_price
                            amt991 = diff_991 * today_price
                            common_buys.append({
                                '股票代號': sid, '股票名稱': sname,
                                '00981a買入金額': format_money_label(amt981), '00981a權重': m_row['Weight_str'].iloc[0],
                                '00991a買入金額': format_money_label(amt991), '00991a權重': row991['Weight_str'].iloc[0]
                            })
        
        # 處理 Excel 匯出
        output_io = io.BytesIO()
        with pd.ExcelWriter(output_io, engine='xlsxwriter') as writer:
            fmt_comma = writer.book.add_format({'num_format': '#,##0'})
            fmt_text_red = writer.book.add_format({'font_color': 'red'})
            fmt_text_green = writer.book.add_format({'font_color': 'green'})
            fmt_num_red = writer.book.add_format({'num_format': '#,##0', 'font_color': 'red'})
            fmt_num_green = writer.book.add_format({'num_format': '#,##0', 'font_color': 'green'})
            fmt_header = writer.book.add_format({'bold': True, 'align': 'center', 'border': 1})
            
            # (這裡保留原本複雜的 Excel 格式化邏輯)
            for etf in ["00981a", "00991a"]:
                df_all = pd.DataFrame(etf_store[etf]['rows'])
                if not df_all.empty:
                    df_active = df_all[df_all['今股數'] > 0].sort_values('Weight_num', ascending=False)
                    df_deleted = df_all[df_all['今股數'] == 0].sort_values('前股數', ascending=False)
                    df_final = pd.concat([df_active, df_deleted]).drop(columns=['Weight_num'])
                    
                    sheet_name = f'{etf}個股比較'
                    df_final.to_excel(writer, sheet_name=sheet_name, index=False)
                    ws = writer.sheets[sheet_name]
                    ws.set_column('B:B', 30); ws.set_column('D:F', 20, fmt_comma)
                    ws.conditional_format(f'G2:G{len(df_final)+1}', {'type': 'formula', 'criteria': '=$F2>0', 'format': fmt_text_red})
                    ws.conditional_format(f'G2:G{len(df_final)+1}', {'type': 'formula', 'criteria': '=$F2<0', 'format': fmt_text_green})
                    ws.conditional_format(f'F2:F{len(df_final)+1}', {'type': 'cell', 'criteria': '>', 'value': 0, 'format': fmt_num_red})
                    ws.conditional_format(f'F2:F{len(df_final)+1}', {'type': 'cell', 'criteria': '<', 'value': 0, 'format': fmt_num_green})

                    if etf == "00981a":
                        etf_store[etf]['summary'].to_excel(writer, sheet_name="00981a ETF摘要", index=False)

            if common_buys:
                pd.DataFrame(common_buys).to_excel(writer, sheet_name='當日共同買進', index=False)
            
            # 共同持股邏輯
            m981 = etf_store['00981a']['df']
            m991 = etf_store['00991a']['df']
            dict_981 = m981[m981['Shares_n_new'] > 0].set_index('ID').to_dict('index')
            dict_991 = m991[m991['Shares_n_new'] > 0].set_index('ID').to_dict('index')
            overlap_sids = list(set(dict_981.keys()) & set(dict_991.keys()))
            overlap_data = []
            for sid in overlap_sids:
                r981 = dict_981[sid]; r991 = dict_991[sid]
                overlap_data.append({'股票代號': sid, '股票名稱': r981['Name'], '00981a權重': r981['Weight_str'], '00991a權重': r991['Weight_str']})
            if overlap_data:
                pd.DataFrame(overlap_data).to_excel(writer, sheet_name='共同持股', index=False)

        # 顯示 Excel 下載按鈕
        st.success("✅ 分析完成！")
        st.download_button(
            label="📥 下載 Excel 完整報表",
            data=output_io.getvalue(),
            file_name=f"ETF_Report_{date_t}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

        st.markdown("---")
        st.subheader("📈 個股變動互動圖表")

        if plot_candidates:
            # 在 Session State 中儲存數據供互動使用
            st.session_state['plot_data'] = {}
            for sid in plot_candidates:
                 # 簡易快取：如果 session state 有就不重算 (這裡為了簡化直接算)
                 df_p = price_cache.get(sid, pd.DataFrame())
                 if isinstance(df_p, pd.Series): df_p = df_p.to_frame()
                 h981_raw = get_history_aligned(all_files['00981a'], sid, None)
                 h991_raw = get_history_aligned(all_files['00991a'], sid, None)
                 
                 # 找出名稱
                 name = "未知"
                 row = etf_store['00981a']['df'][etf_store['00981a']['df']['ID']==sid]
                 if not row.empty: name = row['Name'].iloc[0]
                 else:
                     row = etf_store['00991a']['df'][etf_store['00991a']['df']['ID']==sid]
                     if not row.empty: name = row['Name'].iloc[0]
                 
                 st.session_state['plot_data'][sid] = {
                     'sname': name, 'h981_raw': h981_raw, 'h991_raw': h991_raw, 'price_df': df_p
                 }
            
            # 使用 Selectbox 選擇股票，而不是 Plotly 原生下拉選單 (Streamlit 效能最佳解)
            selected_sid_label = st.selectbox(
                "選擇要查看的股票:",
                options=plot_candidates,
                format_func=lambda x: f"{x} {st.session_state['plot_data'][x]['sname']}"
            )
            
            if selected_sid_label:
                plot_single_stock(selected_sid_label, st.session_state['plot_data'])
        else:
            st.info("選定日期區間內無變動股票。")

if __name__ == "__main__":
    main()
