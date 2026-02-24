import streamlit as st
import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt
import datetime

# 設定網頁標題
st.set_page_config(page_title="ETF 追蹤工具", layout="wide")
st.title("📊 ETF 持股與走勢追蹤分析")

# 1. 側邊欄：輸入參數
with st.sidebar:
    st.header("設定參數")
    etf_options = ["00981a", "00991a"]
    selected_etf = st.selectbox("選擇追蹤的 ETF", etf_options)
    
    start_date = st.date_input("開始日期", datetime.date(2025, 1, 1))
    end_date = st.date_input("結束日期", datetime.date.today())
    
    st.info("提示：若需更新 GitHub 資料源，請確認網路連線。")

# 2. 資料獲取邏輯 (這裡放入您原本抓取 GitHub 資料的邏輯)
@st.cache_data(ttl=3600)  # 快取資料一小時，避免重複下載
def fetch_data(etf_name, start, end):
    # 這裡請替換成您原本程式碼中抓取 GitHub 或 CSV 的邏輯
    # 以下為範例：抓取 ETF 價格
    symbol = "3693.TW" if etf_name == "3693" else "0050.TW" # 示意用
    df = yf.download(symbol, start=start, end=end)
    return df

# 3. 畫面主要內容
df_price = fetch_data(selected_etf, start_date, end_date)

col1, col2 = st.columns([2, 1])

with col1:
    st.subheader(f"{selected_etf} 價格走勢與 K 線圖")
    if not df_price.empty:
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.plot(df_price.index, df_price['Close'], label='收盤價')
        # 這裡可以加入您原本的成本線 (Cost Basis) 邏輯
        ax.set_ylabel("價格")
        ax.legend()
        st.pyplot(fig)
    else:
        st.warning("查無價格資料")

with col2:
    st.subheader("持股權重與變動")
    # 這裡放入您處理 GitHub 每日持股變動的邏輯
    # 範例展示一個 DataFrame
    st.write("最新持股清單 (範例)")
    sample_data = pd.DataFrame({
        "股票代碼": ["2330", "2317", "2454"],
        "權重 (%)": [30.5, 12.2, 8.4],
        "每日變動": ["+0.2%", "-0.1%", "+0.5%"]
    })
    st.dataframe(sample_data, use_container_width=True)

# 4. 共同買入清單 (報表功能)
st.divider()
st.subheader("💡 共同買入股票分析")
# 放入您原本比對 00981a 與 00991a 共同持股的邏輯
st.info("此處會顯示兩個 ETF 當日共同增持的股票標的。")

# 5. 下載報告
csv = sample_data.to_csv(index=False).encode('utf-8-sig')
st.download_button(
    label="下載持股分析 Excel (CSV)",
    data=csv,
    file_name=f"{selected_etf}_analysis.csv",
    mime="text/csv",
)