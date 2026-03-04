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
        
        # 精準只抓 00981A，避免 yfinance 合併多標的時產生的 NaN 斷層
        etf_price_map = get_bulk_prices(["00981A"], start_d, datetime.now() + timedelta(days=1))
        df_etf = etf_price_map.get("00981A", pd.DataFrame())
        
        if not df_etf.empty:
            df_cw = get_etf_cash_history(df_files)
            df_etf_comb = df_etf.join(df_cw, how='left')
            df_etf_comb['Cash_Weight'] = df_etf_comb['Cash_Weight'].ffill().fillna(0)
            
            dates = df_etf_comb.index
            dt_all = pd.date_range(start=dates.min(), end=dates.max())
            dt_breaks = [d.strftime("%Y-%m-%d") for d in dt_all if d not in dates]

            fig = make_subplots(
                rows=2, cols=1, shared_xaxes=True, row_heights=[0.7, 0.3], vertical_spacing=0.05,
                subplot_titles=("<b>00981a K線</b>", "<b>現金權重走勢 (%)</b>")
            )
            
            fig.add_trace(go.Candlestick(
                x=dates, open=df_etf_comb['Open'], high=df_etf_comb['High'],
                low=df_etf_comb['Low'], close=df_etf_comb['Close'], name='K線',
                increasing_line_color='#EF5350', increasing_fillcolor='#EF5350',
                decreasing_line_color='#26A69A', decreasing_fillcolor='#26A69A'
            ), row=1, col=1)
            
            fig.add_trace(go.Scatter(
                x=dates, y=df_etf_comb['Cash_Weight'], mode='lines', 
                line=dict(color='#2962FF', width=2), fill='tozeroy', fillcolor='rgba(41, 98, 255, 0.1)', name='現金權重'
            ), row=2, col=1)

            fig.update_layout(**tv_layout, height=500, xaxis_rangeslider_visible=False)
            fig.update_xaxes(type='date', rangebreaks=[dict(values=dt_breaks)], nticks=10)
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
            
            # --- 極速優化核心：預先將歷史資料分組，避免迴圈內做重複的全表掃描 ---
            history_grouped = dict(tuple(df_history_cache.groupby('ID')))
            
            for row in df_latest.itertuples():
                sid, name = row.ID, row.Name
                df_p = price_map.get(sid, pd.DataFrame())

                if not df_p.empty:
                    # 直接從 GroupBy 提取資料，並標記 is_grouped=True
                    df_stock_h = history_grouped.get(sid, pd.DataFrame())
                    _, cost_line, _, _ = calculate_avg_cost_optimized(df_stock_h, sid, df_p, is_grouped=True)
                    
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
