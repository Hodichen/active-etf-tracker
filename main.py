import pandas as pd
import requests
import os
from datetime import datetime

# 定義你要追蹤的 ETF 清單
ETF_LIST = ['00980A', '00981A', '00982A']

def fetch_today_data(etf_code):
    """
    爬蟲模組：負責去投信官網抓取最新持股
    (此處為架構示意，需替換為實際投信官網的解析邏輯)
    """
    print(f"正在獲取 {etf_code} 的最新持股資料...")
    # 假設爬取後整理成以下 DataFrame 格式：
    # 欄位必須包含：['股票代號', '股票名稱', '持有股數']
    
    # --- 這裡是你未來要填入實際 requests 爬蟲邏輯的地方 ---
    # response = requests.get('投信官網持股CSV下載網址')
    # df_today = pd.read_csv(io.StringIO(response.text))
    
    # 建立一個測試用的假資料，確保程式能順利跑通
    dummy_data = {
        '股票代號': ['2330', '2317', '2454'],
        '股票名稱': ['台積電', '鴻海', '聯發科'],
        '持有股數': [150000, 85000, 40000] # 每天抓到的最新股數
    }
    return pd.DataFrame(dummy_data)

def analyze_and_save(etf_code, df_today):
    """
    比對模組：讀取昨天的資料進行比對，並覆寫今天的資料
    """
    file_path = f"{etf_code}_latest.csv"
    
    # 1. 檢查有沒有昨天的資料
    if os.path.exists(file_path):
        df_yesterday = pd.read_csv(file_path)
        print(f"✅ 找到 {etf_code} 昨天的資料，開始比對...")
        
        # 確保兩邊的股票代號都是字串型態，避免比對錯誤
        df_yesterday['股票代號'] = df_yesterday['股票代號'].astype(str)
        df_today['股票代號'] = df_today['股票代號'].astype(str)
        
        # 合併兩天的資料
        merged = pd.merge(
            df_yesterday[['股票代號', '股票名稱', '持有股數']], 
            df_today[['股票代號', '股票名稱', '持有股數']], 
            on='股票代號', how='outer', suffixes=('_昨', '_今')
        )
        
        # 處理空值 (新建倉昨天的股數為0，清倉今天的股數為0)
        merged.fillna({'持有股數_昨': 0, '持有股數_今': 0}, inplace=True)
        
        # 填補股票名稱的空缺 (避免新建倉沒有名字)
        merged['股票名稱'] = merged['股票名稱_今'].combine_first(merged['股票名稱_昨'])
        
        # 計算經理人的動向
        merged['股數增減'] = merged['持有股數_今'] - merged['持有股數_昨']
        
        # 篩選出有變動的標的
        action_df = merged[merged['股數增減'] != 0].copy()
        
        if not action_df.empty:
            print(f"\n--- {etf_code} 經理人今日動向 ---")
            action_df = action_df.sort_values(by='股數增減', ascending=False)
            print(action_df[['股票代號', '股票名稱', '股數增減', '持有股數_今']])
            # 這裡可以加入串接 LINE Notify 的邏輯
        else:
            print(f"--- {etf_code} 今日持股無變動 ---")
            
    else:
         print(f"⚠️ 找不到 {etf_code} 昨天的資料，今天將作為基準日 (Day 1)。")

    # 2. 將今天的資料存檔，覆寫掉舊檔案，成為明天的「昨日資料」
    df_today.to_csv(file_path, index=False, encoding='utf-8-sig')
    print(f"💾 {etf_code} 最新持股已儲存至 {file_path}\n")

if __name__ == "__main__":
    print(f"執行日期：{datetime.now().strftime('%Y-%m-%d')}\n")
    for etf in ETF_LIST:
        latest_data = fetch_today_data(etf)
        analyze_and_save(etf, latest_data)
