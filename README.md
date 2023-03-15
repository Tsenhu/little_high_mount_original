# little_high_mount_original

Steps:
'''
1. Update nasdaq list from "http://ftp.nasdaqtrader.com/Trader.aspx?id=symbollookup" --> Downloadable Files --> NASDAQ-Traded
'''
1. Run er_company_info.py (estimate 37 hours)
2. Run hist_er_base_initial.py Frequency quarterly.
(3. Run hist_er_asseble_v2020.1.py Frequency monthly. Last Friday of each month. ---merged into step 2.)
4. Run next_earning.py Frequency weekly. Every Friday or weekend.
