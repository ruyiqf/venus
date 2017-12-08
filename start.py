#coding:GBK
import os
import sys
import datetime
from v0.interface import OracleDbInf
from v0.interface import WindPyInf

CUTOFFDASH='-------'
ACCOMPLISH='���ݸ������'

class DataFresh(object):
    """������ȡ���ܿ��࣬����������Ӧ����Դ�Ľӿ��Լ��ϲ�Ӧ������
    """
    def __init__(self):
        self.odi = OracleDbInf() 
        self.wpi = WindPyInf('./conf/parameter.json')

    def __del__(self):
        pass

def main():
    df = DataFresh()
    start_time = datetime.datetime.now()
    print(CUTOFFDASH+'���½�������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_trading_days()
    print(CUTOFFDASH+'���½���������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_trading_month()
    print(CUTOFFDASH+'���²�������ָ��(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_financial_factor()
    print(CUTOFFDASH+'�����г�������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_market_revenue_ratio()
    print(CUTOFFDASH+'����ȫA������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_wholea_revenue_ratio()
    print(CUTOFFDASH+'���¹�Ʊ��Ϣ(DB)'+CUTOFFDASH)
    df.odi.db_download_stock_info()
    print(CUTOFFDASH+'���¹�Ʊ��ҵ��Ϣ(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_stkind()
    print(CUTOFFDASH+'������֤50������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_sz50_ratio()
    print(CUTOFFDASH+'������֤500������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_zz500_ratio()
    print(CUTOFFDASH+'���»���300������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_hs300_ratio()
    print(CUTOFFDASH+'����H00016ָ��������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_h00016_ratio()
    print(CUTOFFDASH+'����H00030ָ��������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_h00030_ratio()
    print(CUTOFFDASH+'����H00905ָ��������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_h00905_ratio()
    print(CUTOFFDASH+'����ӯ��Ԥ������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_egibs_factor()
    print(CUTOFFDASH+'��������������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_egsgro_factor()
    print(CUTOFFDASH+'���¹�Ʊ�Ƿ�ɽ����б�(DB)'+CUTOFFDASH)
    df.odi.db_download_stock_trade_able()
    print(CUTOFFDASH+'���¹�Ʊ����״̬(DB)'+CUTOFFDASH)
    df.odi.db_download_tradestatus()
    print(CUTOFFDASH+'�����ǵ�ͣ״̬(DB)'+CUTOFFDASH)
    df.odi.db_download_updownlimit_status()
    print(CUTOFFDASH+'���������������(DB)'+CUTOFFDASH)
    df.odi.db_download_price_relate_factor()
    print(CUTOFFDASH+'���¼۸�����(DB)'+CUTOFFDASH)
    df.odi.db_download_price_factor()
    print(CUTOFFDASH+'����BE,EP,SP,CFP����(DB)'+CUTOFFDASH) 
    df.odi.db_download_bescfp_factor()
    print(CUTOFFDASH+'����TPS����(DB)'+CUTOFFDASH)
    df.odi.db_download_tps_factor()
    print(CUTOFFDASH+'����WRATING90����(DB)'+CUTOFFDASH)
    df.odi.db_download_wrating90_factor()
    print(CUTOFFDASH+'����ROEFY1�������(DB)'+CUTOFFDASH)
    df.odi.db_download_fy1_factor()
    print(CUTOFFDASH+'����ROEFTTM�������(DB)'+CUTOFFDASH)
    df.odi.db_download_fttm_factor()
    print(CUTOFFDASH+'����ROEYOF�������(DB)'+CUTOFFDASH)
    df.odi.db_download_yoy_factor()
    print(CUTOFFDASH+'����ROECAGR�������(DB)'+CUTOFFDASH)
    df.odi.db_download_cagr_factor()
    print(CUTOFFDASH+'����ȯ�̵ȼ��������(DB)'+CUTOFFDASH)
    df.odi.db_download_ratingavg_factor()
    print(CUTOFFDASH+'���³ֱֲ����������(DB)'+CUTOFFDASH)
    df.odi.db_download_valuediff_factor()
    print(CUTOFFDASH+'�����ֽ����������(DB)'+CUTOFFDASH)
    df.odi.db_download_moneyflow_factor()
    print(CUTOFFDASH+'������֤500����300Ȩ������(DB)'+CUTOFFDASH)
    df.odi.db_download_ZZ500HS300weight_factor()
    print(CUTOFFDASH+'������֤50Ȩ������(DB)'+CUTOFFDASH)
    df.odi.db_download_50weight_factor()
    print(CUTOFFDASH+'���¹�Ʊ��������(DB)'+CUTOFFDASH)
    df.odi.db_download_listdate_factor()
    print(CUTOFFDASH+'���¹�Ʊ29��ҵ����(DB)'+CUTOFFDASH)
    df.odi.db_download_industry29_factor()
    print(CUTOFFDASH+'���¸�����ֵ����'+CUTOFFDASH)
    df.odi.db_download_totalmv_factor()
    print(CUTOFFDASH+'����EPFWD����'+CUTOFFDASH)
    df.odi.db_download_epfwd_factor()
    print(CUTOFFDASH+'����4��ӯ��Ԥ������(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_profit_pred_value()
    print(CUTOFFDASH+'������ֵ����(WIND)'+CUTOFFDASH)
    df.wpi.wind_download_market_value()
    print(CUTOFFDASH+'���¹�Ϣ������'+CUTOFFDASH)
    df.odi.db_download_dividend12m_factor()
    print(CUTOFFDASH+'��������ֵ'+CUTOFFDASH)
    df.odi.db_download_tmv_factor()
    end_time = datetime.datetime.now()
    print(ACCOMPLISH)
    print('�ܺ�ʱ:%s'%str(end_time - start_time))

if __name__ == '__main__':
    main()
