import pandas as pd
import requests
import os
from datetime import datetime
import json

# 定義你要追蹤的 ETF 清單
ETF_LIST = ['00980A', '00981A', '00982A']

def fetch_today_data(etf_code):
    print(f"====================================")
    print(f"▶ 正在獲取 {etf_code} 的最新持股資料...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    try:
        if etf_code == '00980A':
            # === 野村投信 (00980A) API 爬蟲邏輯 ===
            url = "https://www.nomurafunds.com.tw/API/ETFAPI/api/Fund/GetFundAssets"
            headers["Content-Type"] = "application/json"
            
            # 取得今天的日期作為查詢參數
            today_str = datetime.now().strftime('%Y-%m-%d')
            payload = {
                "FundID": "00980A",
                "SearchDate": today_str
            }
            
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # 智慧解析 JSON：找出包含持股明細的陣列
            records = []
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                # 遍歷字典，找到第一個是 List 且有內容的值 (通常就是持股資料)
                for key, value in data.items():
                    if isinstance(value, list) and len(value) > 0:
                        records = value
                        break
                        
            if not records:
                print("⚠️ 無法在 API 回應中找到持股陣列，可能今日尚未更新或格式變更。")
                return pd.DataFrame()
                
            df = pd.DataFrame(records)
            print(f"✅ 成功抓取！API 原始欄位有：{df.columns.tolist()}")
            
            # 智慧欄位對應：自動辨識英文名稱並轉換為標準中文
            col_mapping = {}
            for col in df.columns:
                col_lower = str(col).lower()
                if any(k in col_lower for k in ['code', '代號', '代碼', 'sym']):
                    col_mapping[col] = '股票代號'
                elif any(k in col_lower for k in ['name', '名稱', 'desc']):
                    col_mapping[col] = '股票名稱'
                elif any(k in col_lower for k in ['share', '股數', 'qty', 'amount']):
                    col_mapping[col] = '持有股數'
            
            df = df.rename(columns=col_mapping)
            
            # 檢查必要欄位是否到齊
            required_cols = ['股票代號', '股票名稱', '持有股數']
            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                print(f"❌ 找不到必要欄位：{missing_cols}，請手動調整 col_mapping。")
                return pd.DataFrame()
                
            # 清理與格式化資料
            df['股票代號'] = df['股票代號'].astype(str).str.strip()
            df = df.dropna(subset=['股票代號'])
            # 過濾掉可能混入的現金或期貨部位 (通常股票代號都是數字)
            df = df[df['股票代號'].str.match(r'^[0-9A-Za-z]+$')] 
            
            df_today = df[['股票代號', '股票名稱', '持有股數']].copy()
            
            # 確保股數是數值型態
            df_today['持有股數'] = pd.to_numeric(df_today['持有股數'], errors='coerce').fillna(0)
            return df_today

        elif etf_code == '00981A':
             print("⏳ 00981A 爬蟲邏輯尚未實作，略過...")
             return pd.DataFrame() 
             
        elif etf_code == '00982A':
             print("⏳ 00982A 爬蟲邏輯尚未實作，略過...")
             return pd.DataFrame()

    except Exception as e:
        print(f"❌ 抓取 {etf_code} 發生錯誤: {e}")
        return pd.DataFrame()


def analyze_and_save(etf_code, df_today):
    """
    比對模組：讀取昨天的資料進行比對，並覆寫今天的資料
    """
    if df_today.empty:
        print(f"⚠️ {etf_code} 今日無有效資料可供分析。\n")
        return

    file_path = f"{etf_code}_latest.csv"
    
    # 1. 檢查有沒有昨天的資料
    if os.path.exists(file_path):
        df_yesterday = pd.read_csv(file_path)
        print(f"🔍 找到 {etf_code} 昨天的資料，開始比對經理人動向...")
        
        # 確保代號格式一致
        df_yesterday['股票代號'] = df_yesterday['股票代號'].astype(str)
        df_today['股票代號'] = df_today['股票代號'].astype(str)
        
        # 合併兩天的資料進行比對
        merged = pd.merge(
            df_yesterday[['股票代號', '股票名稱', '持有股數']], 
            df_today[['股票代號', '股票名稱', '持有股數']], 
            on='股票代號', how='outer', suffixes=('_昨', '_今')
        )
        
        # 處理空值 (新建倉昨天的股數為0，清倉今天的股數為0)
        merged.fillna({'持有股數_昨': 0, '持有股數_今': 0}, inplace=True)
        merged['股票名稱'] = merged['股票名稱_今'].combine_first(merged['股票名稱_昨'])
        
        # 計算股數增減
        merged['股數增減'] = merged['持有股數_今'] - merged['持有股數_昨']
        
        # 篩選出經理人有動作的標的
        action_df = merged[merged['股數增減'] != 0].copy()
        
        if not action_df.empty:
            print(f"\n📊 【 {etf_code} 經理人今日調整動向 】 📊")
            # 依據增減股數排序 (買超在最上面)
            action_df = action_df.sort_values(by='股數增減', ascending=False)
            
            # 排版印出漂亮的比對結果
            print(action_df[['股票代號', '股票名稱', '持有股數_昨', '持有股數_今', '股數增減']].to_string(index=False))
            print("-" * 50)
        else:
            print(f"⏸️ {etf_code} 今日持股與昨日完全相同，經理人無動作。")
            
    else:
         print(f"📝 找不到 {etf_code} 昨天的資料，今天將建立基準點 (Day 1)。")

    # 2. 將今天的資料存檔，覆寫舊檔案
    df_today.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"💾 {etf_code} 最新持股清單已儲存至 {file_path}\n")


if __name__ == "__main__":
    print(f"=== 啟動主動型 ETF 追蹤程式 ===")
    print(f"執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    for etf in ETF_LIST:
        latest_data = fetch_today_data(etf)
        analyze_and_save(etf, latest_data)
        
    print("✅ 程式執行完畢！")
