import streamlit as st
import pandas as pd
import requests
import io
import os
from datetime import datetime

# --- 網頁外觀設定 ---
st.set_page_config(page_title="00981A 經理人動向分析", layout="wide")
st.title("📊 00981A (統一) 經理人動向即時分析器")
st.markdown("一鍵從統一投信伺服器下載最新持股 Excel，自動算出今日買賣超 Top 10！")

# --- 核心資料處理函數 ---
def fetch_and_analyze_00981a():
    # 🎯 這裡換成了你親手抓出來的真實網址！
    excel_url = "https://www.ezmoney.com.tw/ETF/Fund/AssetExcelNPOI?fundCode=49YTW"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": "https://www.ezmoney.com.tw/ETF/ETFInfo/Info?FundCode=49YTW:1330" 
    }

    try:
        # 1. 模擬瀏覽器下載 Excel
        response = requests.get(excel_url, headers=headers)
        response.raise_for_status()
        
        # 2. 將下載的 Bytes 內容直接轉給 Pandas 讀取
        excel_data = io.BytesIO(response.content)
        df = pd.read_excel(excel_data)
        
        st.success(f"✅ 成功從統一伺服器取得最新 Excel！共解析出 {len(df)} 筆成分股。")
        
        # 3. 智慧對應 Excel 欄位名稱
        col_mapping = {}
        for col in df.columns:
            col_str = str(col).strip()
            if '代號' in col_str or '代碼' in col_str: col_mapping[col] = '股票代號'
            elif '名稱' in col_str: col_mapping[col] = '股票名稱'
            elif '股數' in col_str: col_mapping[col] = '持有股數'
            
        df = df.rename(columns=col_mapping)
        
        # 檢查必要欄位是否齊全
        required_cols = ['股票代號', '股票名稱', '持有股數']
        if not all(c in df.columns for c in required_cols):
             st.error(f"❌ 解析失敗：找不到必要欄位。Excel 原本的欄位有：{df.columns.tolist()}")
             return None, None
             
        # 清理與格式化資料
        df['股票代號'] = df['股票代號'].astype(str).str.strip()
        df = df.dropna(subset=['股票代號'])
        # 過濾掉非股票的列 (如現金、小計等)
        df = df[df['股票代號'].str.match(r'^[0-9A-Za-z]+$')]
        df['持有股數'] = pd.to_numeric(df['持有股數'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        df_today = df[['股票代號', '股票名稱', '持有股數']].copy()
        
        # --- 進入比對邏輯 ---
        file_path = "00981A_latest.csv"
        
        if os.path.exists(file_path):
            df_yesterday = pd.read_csv(file_path)
            st.info("🔍 找到昨日資料庫，正在計算經理人動向...")
            
            df_yesterday['股票代號'] = df_yesterday['股票代號'].astype(str)
            df_today['股票代號'] = df_today['股票代號'].astype(str)
            
            merged = pd.merge(
                df_yesterday[['股票代號', '股票名稱', '持有股數']], 
                df_today[['股票代號', '股票名稱', '持有股數']], 
                on='股票代號', how='outer', suffixes=('_昨', '_今')
            )
            
            merged.fillna({'持有股數_昨': 0, '持有股數_今': 0}, inplace=True)
            merged['股票名稱'] = merged['股票名稱_今'].combine_first(merged['股票名稱_昨'])
            merged['股數增減'] = merged['持有股數_今'] - merged['持有股數_昨']
            
            action_df = merged[merged['股數增減'] != 0].copy()
            action_df = action_df.sort_values(by='股數增減', ascending=False)
            
            buy_df = action_df[action_df['股數增減'] > 0]
            sell_df = action_df[action_df['股數增減'] < 0]
            
            # 覆寫存檔供明天比對
            df_today.to_csv(file_path, index=False, encoding='utf-8-sig')
            
            return buy_df, sell_df
            
        else:
            st.warning("📝 系統中尚未有 00981A 的昨日資料，今天將建立基準點 (Day 1)。")
            df_today.to_csv(file_path, index=False, encoding='utf-8-sig')
            # 為了畫面效果，第一天先將所有持股視為新建倉
            df_today['持有股數_昨'] = 0
            df_today['持有股數_今'] = df_today['持有股數']
            df_today['股數增減'] = df_today['持有股數']
            buy_df = df_today.sort_values(by='股數增減', ascending=False)
            return buy_df, pd.DataFrame()

    except Exception as e:
        st.error(f"抓取或解析過程中發生錯誤：{e}")
        return None, None

# --- 網頁按鈕與排版區 ---
if st.button("🚀 點我執行抓取與分析", type="primary"):
    with st.spinner("正在前往統一投信伺服器搬運資料中..."):
        buy_df, sell_df = fetch_and_analyze_00981a()
        
        if buy_df is not None:
            st.divider()
            col1, col2 = st.columns(2)
            
            # --- 左側：買超 Top 10 ---
            with col1:
                st.subheader("📈 加碼 / 新建倉 (Top 10)")
                if not buy_df.empty:
                    display_buy = buy_df.head(10)[['股票代號', '股票名稱', '持有股數_昨', '持有股數_今', '股數增減']]
                    display_buy['持有股數_昨'] = display_buy['持有股數_昨'].map('{:,.0f}'.format)
                    display_buy['持有股數_今'] = display_buy['持有股數_今'].map('{:,.0f}'.format)
                    display_buy['股數增減'] = display_buy['股數增減'].apply(lambda x: f"🔺 +{x:,.0f}")
                    st.dataframe(display_buy, use_container_width=True, hide_index=True)
                else:
                    st.info("今日無任何加碼動作")
                    
            # --- 右側：賣超 Top 10 ---
            with col2:
                st.subheader("📉 減碼 / 清倉 (Top 10)")
                if not sell_df.empty:
                    display_sell = sell_df.tail(10).sort_values(by='股數增減', ascending=True)[['股票代號', '股票名稱', '持有股數_昨', '持有股數_今', '股數增減']]
                    display_sell['持有股數_昨'] = display_sell['持有股數_昨'].map('{:,.0f}'.format)
                    display_sell['持有股數_今'] = display_sell['持有股數_今'].map('{:,.0f}'.format)
                    display_sell['股數增減'] = display_sell['股數增減'].apply(lambda x: f"🔻 {x:,.0f}")
                    st.dataframe(display_sell, use_container_width=True, hide_index=True)
                else:
                    st.info("今日無任何減碼動作")
