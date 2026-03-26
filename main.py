import pandas as pd
import requests
import os
from datetime import datetime

# 定義你要追蹤的 ETF 清單
ETF_LIST = ['00980A', '00981A', '00982A']

def fetch_today_data(etf_code):
    print(f"====================================")
    print(f"▶ 正在獲取 {etf_code} 的最新持股資料...")
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    try:
        if etf_code == '00980A':
            # === 野村投信 (00980A) 專屬解析邏輯 ===
            url = "https://www.nomurafunds.com.tw/API/ETFAPI/api/Fund/GetFundAssets"
            headers["Content-Type"] = "application/json"
            
            today_str = datetime.now().strftime('%Y-%m-%d')
            payload = {
                "FundID": "00980A",
                "SearchDate": today_str
            }
            
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # 根據 log 顯示的結構，精準向下挖掘到 'Rows' 的位置
            try:
                tables = data.get("Entries", {}).get("Data", {}).get("Table", [])
                stock_table = next((t for t in tables if t.get("TableTitle") == "股票"), None)
                
                if stock_table and "Rows" in stock_table:
                    rows_data = stock_table["Rows"]
                    df = pd.DataFrame(rows_data, columns=['股票代號', '股票名稱', '持有股數', '權重'])
                    
                    print(f"✅ 成功精準抓取！共找到 {len(df)} 檔成分股。")
                    
                    # 整理格式
                    df['股票代號'] = df['股票代號'].astype(str).str.strip()
                    df['持有股數'] = pd.to_numeric(df['持有股數'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                    
                    # 只回傳我們比對需要的三個欄位
                    df_today = df[['股票代號', '股票名稱', '持有股數']].copy()
                    return df_today
                else:
                    print("⚠️ 在 API 結構中找不到 '股票' 的 Rows 資料。")
                    return pd.DataFrame()
                    
            except AttributeError as e:
                print(f"❌ JSON 結構解析失敗: {e}")
                return pd.DataFrame()

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
        
        # 處理空值
        merged.fillna({'持有股數_昨': 0, '持有股數_今': 0}, inplace=True)
        merged['股票名稱'] = merged['股票名稱_今'].combine_first(merged['股票名稱_昨'])
        
        # 計算股數增減
        merged['股數增減'] = merged['持有股數_今'] - merged['持有股數_昨']
        
        # 篩選出經理人有動作的標的
        action_df = merged[merged['股數增減'] != 0].copy()
        
        if not action_df.empty:
            print(f"\n📊 【 {etf_code} 經理人今日調整動向 】 📊")
            
            # 依據增減股數排序 (正數在最上面，負數在最下面)
            action_df = action_df.sort_values(by='股數增減', ascending=False)
            
            # 分別過濾出「買超」與「賣超」的清單
            buy_df = action_df[action_df['股數增減'] > 0]
            sell_df = action_df[action_df['股數增減'] < 0]
            
            # 1. 印出加碼前 10 大
            if not buy_df.empty:
                print(f"\n📈 加碼 / 新建倉 (前 10 大):")
                print(buy_df.head(10)[['股票代號', '股票名稱', '持有股數_昨', '持有股數_今', '股數增減']].to_string(index=False))
            
            # 2. 印出減碼前 10 大
            if not sell_df.empty:
                print(f"\n📉 減碼 / 清倉 (前 10 大):")
                sell_top10 = sell_df.tail(10).sort_values(by='股數增減', ascending=True)
                print(sell_top10[['股票代號', '股票名稱', '持有股數_昨', '持有股數_今', '股數增減']].to_string(index=False))
                
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
