#coding:GBK
import os
import sys
import datetime
from v0.interface import OracleDbInf
from v0.interface import WindPyInf

CUTOFFDASH='-------'
ACCOMPLISH='数据更新完成'

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
    start_time = datetime.datetime.now()
    print(CUTOFFDASH+'更新交易日期(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_trading_days()
    print(CUTOFFDASH+'更新交易月数据(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_trading_month()
    print(CUTOFFDASH+'更新财务数据指标(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_financial_factor()
    print(CUTOFFDASH+'更新市场收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_market_revenue_ratio()
    print(CUTOFFDASH+'更新全A收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_wholea_revenue_ratio()
    print(CUTOFFDASH+'更新股票信息(DB)'+CUTOFFDASH)
    df.odi.db_download_stock_info()
    print(CUTOFFDASH+'更新股票行业信息(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_stkind()
    print(CUTOFFDASH+'更新上证50收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_sz50_ratio()
    print(CUTOFFDASH+'更新中证500收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_zz500_ratio()
    print(CUTOFFDASH+'更新沪深300收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_hs300_ratio()
    print(CUTOFFDASH+'更新H00016指数收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_h00016_ratio()
    print(CUTOFFDASH+'更新H00030指数收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_h00030_ratio()
    print(CUTOFFDASH+'更新H00905指数收益率(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_h00905_ratio()
    print(CUTOFFDASH+'更新盈利预测因子(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_egibs_factor()
    print(CUTOFFDASH+'更新增长率因子(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_egsgro_factor()
    print(CUTOFFDASH+'更新股票是否可交易列表(DB)'+CUTOFFDASH)
    df.odi.db_download_stock_trade_able()
    print(CUTOFFDASH+'更新股票交易状态(DB)'+CUTOFFDASH)
    df.odi.db_download_tradestatus()
    print(CUTOFFDASH+'更新涨跌停状态(DB)'+CUTOFFDASH)
    df.odi.db_download_updownlimit_status()
    print(CUTOFFDASH+'更新行情相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_price_relate_factor()
    print(CUTOFFDASH+'更新价格因子(DB)'+CUTOFFDASH)
    df.odi.db_download_price_factor()
    print(CUTOFFDASH+'更新BE,EP,SP,CFP因子(DB)'+CUTOFFDASH) 
    df.odi.db_download_bescfp_factor()
    print(CUTOFFDASH+'更新TPS因子(DB)'+CUTOFFDASH)
    df.odi.db_download_tps_factor()
    print(CUTOFFDASH+'更新WRATING90因子(DB)'+CUTOFFDASH)
    df.odi.db_download_wrating90_factor()
    print(CUTOFFDASH+'更新ROEFY1相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_fy1_factor()
    print(CUTOFFDASH+'更新ROEFTTM相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_fttm_factor()
    print(CUTOFFDASH+'更新ROEYOF相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_yoy_factor()
    print(CUTOFFDASH+'更新ROECAGR相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_cagr_factor()
    print(CUTOFFDASH+'更新券商等级相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_ratingavg_factor()
    print(CUTOFFDASH+'更新持仓比例相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_valuediff_factor()
    print(CUTOFFDASH+'更新现金流相关因子(DB)'+CUTOFFDASH)
    df.odi.db_download_moneyflow_factor()
    print(CUTOFFDASH+'更新中证500沪深300权重因子(DB)'+CUTOFFDASH)
    df.odi.db_download_ZZ500HS300weight_factor()
    print(CUTOFFDASH+'更新上证50权重因子(DB)'+CUTOFFDASH)
    df.odi.db_download_50weight_factor()
    print(CUTOFFDASH+'更新股票上市日期(DB)'+CUTOFFDASH)
    df.odi.db_download_listdate_factor()
    print(CUTOFFDASH+'更新股票29行业属性(DB)'+CUTOFFDASH)
    df.odi.db_download_industry29_factor()
    print(CUTOFFDASH+'更新个股市值因子'+CUTOFFDASH)
    df.odi.db_download_totalmv_factor()
    print(CUTOFFDASH+'更新EPFWD因子'+CUTOFFDASH)
    df.odi.db_download_epfwd_factor()
    print(CUTOFFDASH+'更新4周盈利预测因子(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_profit_pred_value()
    print(CUTOFFDASH+'更新市值因子(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_market_value()
    print(CUTOFFDASH+'更新股息率因子'+CUTOFFDASH)
    df.odi.db_download_dividend12m_factor()
    print(CUTOFFDASH+'更新总市值'+CUTOFFDASH)
    df.odi.db_download_tmv_factor()
    end_time = datetime.datetime.now()
    print(ACCOMPLISH)
    print('总耗时:%s'%str(end_time - start_time))

if __name__ == '__main__':
    main()
