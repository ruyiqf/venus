#coding:GBK
import os
import sys
from v0.interface import OracleDbInf
from v0.interface import WindPyInf

class DataFresh(object):
    """数据提取的总控类，用来产生对应数据源的接口以及上层应用数据
    """
    def __init__(self):
        self.odi = OracleDbInf() 
        self.wpi = WindPyInf('./conf/parameter.json')

    def __del__(self):
        pass

def main():
    df = DataFresh()
    #df.wpi.wind_download_trading_days()
    #df.wpi.wind_download_market_revenue_ratio()
    #df.wpi.wind_download_wholea_revenue_ratio()
    #df.odi.db_download_stock_info()
    #df.wpi.wind_download_stkind()
    #df.wpi.wind_download_sz50_ratio()
    #df.odi.db_download_stock_trade_able()
    #df.odi.db_download_updownlimit_status()
    df.odi.db_download_
    

if __name__ == '__main__':
    main()
