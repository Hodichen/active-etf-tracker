if not action_df.empty:
            print(f"\n📊 【 {etf_code} 經理人今日調整動向 】 📊")
            
            # 依據增減股數排序 (正數在最上面，負數在最下面)
            action_df = action_df.sort_values(by='股數增減', ascending=False)
            
            # 分別過濾出「買超」與「賣超」的清單
            buy_df = action_df[action_df['股數增減'] > 0]
            sell_df = action_df[action_df['股數增減'] < 0]
            
            # 1. 印出加碼前 10 大 (取最上面的 10 筆)
            if not buy_df.empty:
                print(f"\n📈 加碼 / 新建倉 (前 10 大):")
                print(buy_df.head(10)[['股票代號', '股票名稱', '持有股數_昨', '持有股數_今', '股數增減']].to_string(index=False))
            
            # 2. 印出減碼前 10 大 (取最下面的 10 筆，並反向排序讓賣最多的在最上面)
            if not sell_df.empty:
                print(f"\n📉 減碼 / 清倉 (前 10 大):")
                sell_top10 = sell_df.tail(10).sort_values(by='股數增減', ascending=True)
                print(sell_top10[['股票代號', '股票名稱', '持有股數_昨', '持有股數_今', '股數增減']].to_string(index=False))
                
            print("-" * 50)
