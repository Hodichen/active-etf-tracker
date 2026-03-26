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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
                # 取得 Table 陣列
                tables = data.get("Entries", {}).get("Data", {}).get("Table", [])
                
                # 找出 TableTitle 是 "股票" 的那個表格
                stock_table = next((t for t in tables if t.get("TableTitle") == "股票"), None)
                
                if stock_table and "Rows" in stock_table:
                    # 取得純粹的資料陣列
                    rows_data = stock_table["Rows"]
                    
                    # 依據 API 順序，直接定義欄位名稱 (代號, 名稱, 股數, 權重)
                    df = pd.DataFrame(rows_data, columns=['股票代號', '股票名稱', '持有股數', '權重'])
                    
                    print(f"✅ 成功精準抓取！共找到 {len(df)} 檔成分股。")
                    
                    # 整理格式
                    df['股票代號'] = df['股票代號'].astype(str).str.strip()
                    # 把股數轉成數字 (如果有逗號就先去掉)
                    df['持有股數'] = pd.to_numeric(df['持有股數'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
                    
                    # 只回傳我們比對需要的三個欄位
                    df_today = df[['股票代號', '股票名稱', '持有股數']].copy()
                    return df_today
                else:
                    print("⚠️ 在 API 結構中找不到 '股票' 的 Rows 資料。")
                    return pd.DataFrame()
                    
            except AttributeError as e:
                print(f"❌ JSON 結構解析失敗，可能野村更改了格式: {e}")
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
if __name__ == "__main__":
    print(f"=== 啟動主動型 ETF 追蹤程式 ===")
    print(f"執行時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    for etf in ETF_LIST:
        latest_data = fetch_today_data(etf)
        analyze_and_save(etf, latest_data)
        
    print("✅ 程式執行完畢！")
