#coding:GBK
import os
import sys
import cx_Oracle
import json
import datetime
import time
import scipy.io as sio
import numpy as np
import pandas as pd
from pandas import Series, DataFrame

from ..loghandler import DefaultLogHandler

CONFILE='./conf/oracle.json'
DATAPATH='./data/' #存储matlab的数据目录

class OracleDbInf(object):
    """Oracel数据库的连接接口用来提取本地数据库数据
    """
    def __init__(self):
        self.log = DefaultLogHandler(name=__name__,filepath='./log/db.log',log_type='file')
        with open(CONFILE) as f:
            conf = json.load(f)
        try:
            self.conn = cx_Oracle.connect(conf['dbaddress'])
            self.log.info('成功连接数据库')
        except Exception as e:
            self.log.error(e)
            self.log.error('数据库连接有问题')
        self.tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])

    def db_download_stock_info(self):
        original_stk_info = sio.loadmat(DATAPATH+'stock.mat')

        #需要依赖tdays_data数据
        tdays_data = sio.loadmat(DATAPATH+'tdays_data')['tdays_data']
        l_tdays = len(tdays_data)

        #更新stock.mat数据每日更新一次
        dnow = datetime.datetime.now()
        dnowstr = dnow.strftime('%Y%m%d')
        stklist = self.__extract_data_from_ndarray(original_stk_info['stklist'])

        #提取股票的原有列表信息这里股票的中文名忽略
        stocklist = list() 

        for elt in original_stk_info['stock']:
            tmp = list()
            tmp.append(elt[0][0])
            tmp.append(elt[1][0])
            stocklist.append(tmp)

        cursor = self.conn.cursor()
        sql = 'select s_info_windcode, s_info_name from Asharedescription where cast(s_info_listdate as integer) <= %s and (cast(s_info_delistdate as integer) > %s or s_info_delistdate is null)' % (dnowstr, dnowstr)
        cursor.execute(sql)
        rs = cursor.fetchall()
        newstkmap = self.__convert_dbdata2map(rs)
        diff_list = list(set(newstkmap.keys()).difference(set(stklist)))
        for elt in diff_list:
            stklist.append(elt)
            tmp = list()
            tmp.append(dnow.strftime('%Y/%m/%d')) 
            tmp.append(elt)
            stocklist.append(tmp)

        stklist_array = self.__convert_row2column(np.array(stklist, dtype=np.object))
        stocklist_array = np.array(stocklist, dtype=np.object)
        sio.savemat(DATAPATH+'stock', mdict={'stklist':stklist_array, 'stock':stocklist_array})
        
        #判断A_ST_stock矩阵表文件是否存在
        if(os.path.exists(DATAPATH+'A_ST_stock_d.mat')):
            A_ST_stock_d = sio.loadmat(DATAPATH+'A_ST_stock_d.mat')['A_ST_stock_d']
            if len(A_ST_stock_d) < len(tdays_data):
                colnum = np.shape(A_ST_stock_d)[1]
                if(colnum < len(stklist)):
                    A_ST_stock_d = self.__compensate_original_column(A_ST_stock_d, len(stklist) - colnum)
                self.__write2aststockd_file(stklist,
                    tdays_data[len(A_ST_stock_d):len(tdays_data)],
                    A_ST_stock_d)
            else:
                self.log.info('A_ST_stock_d已经最新无须更新')
        else:
            self.__write2aststockd_file(stklist,
                    tdays_data,
                    np.array([]))
        
    def db_download_stock_trade_able(self):
        """更新stock_trade_able表格
        """
        tdays_data = self.tdays_data 
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'stock_trade_able.mat')):
            stock_trade_able = sio.loadmat(DATAPATH+'stock_trade_able.mat')['stock_trade_able']
            if len(stock_trade_able) < len(tdays_data):
                stock_trade_able = self.__align_column(stock_trade_able, stklist)
                self.__write2stock_trade_able_file(tdays_data[len(stock_trade_able):],
                        stock_trade_able, stklist)
            else:
                self.log.info('已经更新stock_trade_able表')
        else:
            self.__write2stock_trade_able_file(tdays_data, np.array([]), stklist)
        
    def db_download_ind_citic(self):
        """中信行业数据下载
        """
        tdays_data = self.tdays_data 
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]

        ind_code_name_citic = self.__convert_mat2list(sio.loadmat(DATAPATH+'ind_code_name_CITIC.mat')['ind_code_name_CITIC'])
        ind_name_citic = self.__convert_mat2list(sio.loadmat(DATAPATH+'ind_name_CITIC.mat')['ind_name_CITIC'])
        if(os.path.exists(DATAPATH+'ind_of_stock_CITIC.mat')):
            ind_of_stock_citic = sio.loadmat(DATAPATH+'ind_of_stock_CITIC.mat')['ind_of_stock_CITIC']
            if len(ind_of_stock_citic) < len(self.tdays_data):
                self.__write2indcitic_file(tdays_data[len(ind_of_stock_citic):], ind_code_name_citic, ind_name_citic, ind_of_stock_citic, stklist) 
            else:
                self.log.info('中信行业数据已经最新数据不需要更新')
        else:
            ind_of_stock_citic = np.array([])
            self.__write2indcitic_file(tdays_data, ind_code_name_citic, ind_name_citic, np.array([]), stklist) 
    
    def db_download_tradestatus(self):
        """更新tradestatus表格
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'tradestatus.mat')):
            tradestatus = sio.loadmat(DATAPATH+'tradestatus.mat')['tradestatus']
            if len(tradestatus) < len(tdays_data):
                colnum = np.shape(tradestatus)[1]
                if(colnum < len(stklist)):
                    tradestatus = self.__compensate_original_column(tradestatus, len(stklist) - colnum)
                self.__write2tradestatus_file(tdays_data[len(tradestatus):],
                        tradestatus, stklist)
            else:
                self.log.info('已经更新tradestatus表')
        else:
            self.__write2tradestatus_file(tdays_data, np.array([]), stklist)

    def db_download_updownlimit_status(self):
        """更新涨停状态表
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'up_down_limit_status.mat')):
            ud_limit_status = sio.loadmat(DATAPATH+'up_down_limit_status.mat')['up_down_limit_status']
            if len(ud_limit_status) < len(tdays_data):
                colnum = np.shape(ud_limit_status)[1]
                if(colnum < len(stklist)):
                    ud_limit_status = self.__compensate_original_column(ud_limit_status, len(stklist) - colnum)
                self.__write2udlimitstatus_file(tdays_data[len(ud_limit_status):],
                        ud_limit_status, stklist)
            else:
                self.log.info('已经更新up_down_limit_status表')
        else:
            self.__write2udlimitstatus_file(tdays_data, np.array([]), stklist)

    def db_download_price_relate_factor(self):
        """更新价格相关因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'price_original.mat')):
            price_original = sio.loadmat(DATAPATH+'price_original.mat')['price_original']
            if len(price_original) < len(tdays_data):
                adjfactor = sio.loadmat(DATAPATH+'adjfactor.mat')['adjfactor']
                volume = sio.loadmat(DATAPATH+'volume.mat')['volume']
                amount = sio.loadmat(DATAPATH+'amount.mat')['amount']
                TR = sio.loadmat(DATAPATH+'TR.mat')['TR']
                price_original = self.__align_column(price_original, stklist)
                adjfactor = self.__align_column(adjfactor, stklist)
                volume = self.__align_column(volume, stklist)
                amount = self.__align_column(amount, stklist)
                TR = self.__align_column(TR, stklist)
                self.__write2price_factor_file(tdays_data[len(price_original):], price_original,
                        adjfactor, volume,
                        amount, TR, stklist)
            else:
                self.log.info('价格相关的数据因子已经更新完毕')
        else:
            self.__write2price_factor_file(tdays_data, price_original,
                    adjfactor, volume,
                    amount, TR, stklist)

    def db_download_price_factor(self):
        """更新价格因子最高价最低价开盘价
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        adj = sio.loadmat(DATAPATH+'adjfactor.mat')['adjfactor']
        if(os.path.exists(DATAPATH+'open_original.mat')):
            open_original = sio.loadmat(DATAPATH+'open_original.mat')['open_original']
            if(len(open_original) < len(tdays_data)):
                high_original = sio.loadmat(DATAPATH+'high_original.mat')['high_original']
                low_original = sio.loadmat(DATAPATH+'low_original.mat')['low_original']
                open_original = self.__align_column(open_original, stklist)
                high_original = self.__align_column(high_original, stklist)
                low_original = self.__align_column(low_original, stklist)
                self.__write2price_file(tdays_data[len(open_original):], open_original,
                                        high_original, low_original,
                                        stklist, adj)
            else:
                self.log.info('价格因子已经更新完毕')
        else:
            self.__write2price_file(tdays_data, np.array([]),
                                    np.array([]), np.array([]),
                                    stklist, adj)

    def db_download_tps_factor(self):
        """更新TPS相关因子数据
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        pfa = sio.loadmat(DATAPATH+'price_forward_adjusted.mat')['price_forward_adjusted']
        if(os.path.exists(DATAPATH+'daily_factor/TPS.mat')):
            tps = sio.loadmat(DATAPATH+'daily_factor/TPS.mat')['TPS']
            if(len(tps) < len(tdays_data)):
                tps_180 = sio.loadmat(DATAPATH+'daily_factor/TPS_180')['TPS_180']
                tps = self.__align_column(tps, stklist) 
                tps_180 = self.__align_column(tps_180, stklist)
                self.__write2tps_file(tdays_data[len(tps):], tps,
                                      tps_180, stklist, pfa)
            else:
                self.log.info('TPS因子已经更新完毕')
        else:
            self.__write2tps_file(tdays_data, np.array([]),
                                  np.array([]), stklist, pfa)
            
    def db_download_wrating90_factor(self):
        """更新wrating_upgrade90天
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/wrating_upgrade.mat')):
            wrating_upgrade = sio.loadmat(DATAPATH+'daily_factor/wrating_upgrade.mat')['wrating_upgrade']
            if(len(wrating_upgrade) < len(tdays_data)):
                wrating_upgrade = self.__align_column(wrating_upgrade, stklist)
                self.__write2wratingupgrade_file(tdays_data[len(wrating_upgrade):], wrating_upgrade, stklist)
            else:
                self.log.info('wrating90因子已经更新完毕')
        else:
            self.__write2wratingupgrade_file(tdays_data, wrating_upgrade, stklist)

    def db_download_fy1_factor(self):
        """更新盈利预测FY1
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/roe_forcast_FY1.mat')):
            roe_forcast_fy1 = sio.loadmat(DATAPATH+'daily_factor/roe_forcast_FY1.mat')['roe_forcast_FY1']
            if(len(roe_forcast_fy1) < len(tdays_data)):
                pe_forcast_fy1 = sio.loadmat(DATAPATH+'daily_factor/pe_forcast_FY1.mat')['pe_forcast_FY1']
                pb_forcast_fy1 = sio.loadmat(DATAPATH+'daily_factor/pb_forcast_FY1.mat')['pb_forcast_FY1']
                peg_forcast_fy1 = sio.loadmat(DATAPATH+'daily_factor/peg_forcast_FY1.mat')['peg_forcast_FY1']
                roe_forcast_fy1 = self.__align_column(roe_forcast_fy1, stklist)
                pe_forcast_fy1 = self.__align_column(pe_forcast_fy1, stklist)
                pb_forcast_fy1 = self.__align_column(pb_forcast_fy1, stklist)
                peg_forcast_fy1 = self.__align_column(peg_forcast_fy1, stklist)

                self.__write2forcast_file(tdays_data[len(roe_forcast_fy1):], roe_forcast_fy1,
                        pe_forcast_fy1, pb_forcast_fy1,
                        peg_forcast_fy1, stklist)
            else:
                self.log.info('盈利预测ROE,PE,PB,PEG因子已经更新完毕')
        else:
            self.__write2forcast_file(tdays_data, np.array([]),
                    np.array([]), np.array([]),
                    np.array([]), stklist)
    
    def db_download_fttm_factor(self): 
        """更新FTTM因子数据
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/roe_forcast_FTTM.mat')):
            roe_forcast_fttm = sio.loadmat(DATAPATH+'daily_factor/roe_forcast_FTTM.mat')['roe_forcast_FTTM']
            if(len(roe_forcast_fttm) < len(tdays_data)):
                pe_forcast_fttm = sio.loadmat(DATAPATH+'daily_factor/pe_forcast_FTTM.mat')['pe_forcast_FTTM']
                pb_forcast_fttm = sio.loadmat(DATAPATH+'daily_factor/pb_forcast_FTTM.mat')['pb_forcast_FTTM']
                peg_forcast_fttm = sio.loadmat(DATAPATH+'daily_factor/peg_forcast_FTTM.mat')['peg_forcast_FTTM']
                roe_forcast_fttm = self.__align_column(roe_forcast_fttm, stklist)
                pe_forcast_fttm = self.__align_column(pe_forcast_fttm, stklist)
                pb_forcast_fttm = self.__align_column(pb_forcast_fttm, stklist)
                peg_forcast_fttm = self.__align_column(peg_forcast_fttm, stklist)

                self.__write2forcastfttm_file(tdays_data[len(roe_forcast_fttm):], roe_forcast_fttm,
                        pe_forcast_fttm, pb_forcast_fttm,
                        peg_forcast_fttm, stklist)
            else:
                self.log.info('盈利预测ROE,PE,PB,PEG FTTM因子已经更新完毕')
        else:
            self.__write2forcastfttm_file(tdays_data, np.array([]),
                    np.array([]), np.array([]),
                    np.array([]), stklist)

    def db_download_yoy_factor(self): 
        """更新yoy因子数据
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/netprofit_forcast_YOY.mat')):
            netprofit_forcast_yoy = sio.loadmat(DATAPATH+'daily_factor/netprofit_forcast_YOY.mat')['netprofit_forcast_YOY']
            if(len(netprofit_forcast_yoy) < len(tdays_data)):
                roe_forcast_yoy = sio.loadmat(DATAPATH+'daily_factor/roe_forcast_YOY.mat')['roe_forcast_YOY']
                netprofit_forcast_yoy = self.__align_column(netprofit_forcast_yoy, stklist)
                roe_forcast_yoy = self.__align_column(roe_forcast_yoy, stklist)
                self.__write2forcastyoy_file(tdays_data[len(roe_forcast_yoy):], netprofit_forcast_yoy,
                        roe_forcast_yoy, stklist)
            else:
                self.log.info('盈利预测YOY因子已经更新完毕')
        else:
            self.__write2forcastyoy_file(tdays_data, np.array([]),
                    np.array([]), stklist)
        
    def db_download_bescfp_factor(self):
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/BP.mat')):
            BP = sio.loadmat(DATAPATH+'daily_factor/BP.mat')['BP']
            if(len(BP) < len(tdays_data)):
                EP = sio.loadmat(DATAPATH+'daily_factor/EP.mat')['EP']
                SP = sio.loadmat(DATAPATH+'daily_factor/SP.mat')['SP']
                CFP = sio.loadmat(DATAPATH+'daily_factor/CFP.mat')['CFP']
                BP = self.__align_column(BP, stklist)
                EP = self.__align_column(EP, stklist)
                SP = self.__align_column(SP, stklist)
                CFP = self.__align_column(CFP, stklist)
                self.__write2bescfp_file(tdays_data[len(BP):],
                BP, EP, SP, CFP, stklist)
            else:
                self.log.info('BP,EP,SP,CFP更新完毕')
        else:
            self.__write2bescfp_file(tdays_data, np.array([]),
                np.array([]), np.array([]), np.array([]), stklist)

    def db_download_cagr_factor(self):
        """更新cagr因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/netprofit_forcast_CAGR.mat')):
            netprofit_forcast_cagr = sio.loadmat(DATAPATH+'daily_factor/netprofit_forcast_CAGR.mat')['netprofit_forcast_CAGR']
            if(len(netprofit_forcast_cagr) < len(tdays_data)):
                roe_forcast_cagr = sio.loadmat(DATAPATH+'daily_factor/roe_forcast_CAGR.mat')['roe_forcast_CAGR']
                netprofit_forcast_cagr = self.__align_column(netprofit_forcast_cagr, stklist)
                roe_forcast_cagr = self.__align_column(roe_forcast_cagr, stklist)
                self.__write2cagr_file(tdays_data[len(roe_forcast_cagr):], netprofit_forcast_cagr,
                        roe_forcast_cagr, stklist)
            else:
                self.log.info('CAGR更新完毕')
        else:
            self.__write2cagr_file(tdays_data, np.array([]), np.array([]), stklist)

    def db_download_ratingavg_factor(self):
        """更新券商评级因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/rating_avg.mat')):
            rating_avg = sio.loadmat(DATAPATH+'daily_factor/rating_avg.mat')['rating_avg']
            if(len(rating_avg) < len(tdays_data)):
                rating_net_upgrade = sio.loadmat(DATAPATH+'daily_factor/rating_net_upgrade.mat')['rating_net_upgrade']
                rating_instnum = sio.loadmat(DATAPATH+'daily_factor/rating_instnum.mat')['rating_instnum']
                rating_avg = self.__align_column(rating_avg, stklist)
                rating_net_upgrade = self.__align_column(rating_net_upgrade, stklist)
                rating_instnum = self.__align_column(rating_instnum, stklist)
                
                self.__write2broker_file(tdays_data[len(rating_avg):], rating_avg,
                        rating_net_upgrade, rating_instnum, stklist)
            else:
                self.log.info('券商评级因子更新完毕')
        else:
            self.__write2broker_file(tdays_data, np.array([]), np.array([]), np.array([]), stklist)

    def db_download_valuediff_factor(self):
        """更新机构大户散户的占比因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/value_diff_small_trader_act.mat')):
            vdsta = sio.loadmat(DATAPATH+'daily_factor/value_diff_small_trader_act.mat')['value_diff_small_trader_act']

            if(len(vdsta) < len(tdays_data)):
                vdmta = sio.loadmat(DATAPATH+'daily_factor/value_diff_med_trader_act.mat')['value_diff_med_trader_act']
                vdlta = sio.loadmat(DATAPATH+'daily_factor/value_diff_large_trader_act.mat')['value_diff_large_trader_act']
                vdia = sio.loadmat(DATAPATH+'daily_factor/value_diff_institute_act.mat')['value_diff_institute_act']

                odsta = sio.loadmat(DATAPATH+'daily_factor/volume_diff_small_trader_act.mat')['volume_diff_small_trader_act']
                odmta = sio.loadmat(DATAPATH+'daily_factor/volume_diff_med_trader_act.mat')['volume_diff_med_trader_act']
                odlta = sio.loadmat(DATAPATH+'daily_factor/volume_diff_large_trader_act.mat')['volume_diff_large_trader_act']
                odia = sio.loadmat(DATAPATH+'daily_factor/volume_diff_institute_act.mat')['volume_diff_institute_act']
                vdsta = self.__align_column(vdsta, stklist)
                vdmta = self.__align_column(vdmta, stklist)
                vdlta = self.__align_column(vdlta, stklist)
                vdia = self.__align_column(vdia, stklist)
                odsta = self.__align_column(odsta, stklist)
                odmta = self.__align_column(odmta, stklist)
                odlta = self.__align_column(odlta, stklist)
                odia = self.__align_column(odia, stklist)

                self.__write2vdsta_file(tdays_data[len(vdsta):], vdsta, 
                        vdmta, vdlta, 
                        vdia, odsta, 
                        odmta, odlta,
                        odia, stklist)
            else:
                self.log.info('机构散户大户成交量占比因子更新完毕')
        else:
            self.__write2vdsta_file(tdays_data, np.array([]),
                    np.array([]), np.array([]),
                    np.array([]), np.array([]),
                    np.array([]), np.array([]),
                    np.array([]), stklist)

    def db_download_moneyflow_factor(self):
        """更新流动性因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/moneyflow_pct_volume.mat')):
            mf_pct_volume = sio.loadmat(DATAPATH+'daily_factor/moneyflow_pct_volume.mat')['moneyflow_pct_volume']
            if(len(mf_pct_volume) < len(tdays_data)):
                mf_pct_value = sio.loadmat(DATAPATH+'daily_factor/moneyflow_pct_value.mat')['moneyflow_pct_value']
                mf_pct_volume = self.__align_column(mf_pct_volume, stklist)
                mf_pct_value = self.__align_column(mf_pct_value, stklist)
                self.__write2mfpct_file(tdays_data[len(mf_pct_volume):], mf_pct_volume, 
                        mf_pct_value, stklist)
            else:
                self.log.info('流动性因子更新完毕')
        else:
            self.__write2mfpct_file(tdays_data, np.array([]),
                    np.array([]), stklist)

    def db_download_ZZ500HS300weight_factor(self):
        """更新指数权重股
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'ZZ500_weight.mat')):
            zz500_weight = sio.loadmat(DATAPATH+'ZZ500_weight.mat')['ZZ500_weight']
            if(len(zz500_weight) < len(tdays_data)):
                hs300_weight = sio.loadmat(DATAPATH+'HS300_weight.mat')['HS300_weight']
                zz500_weight = self.__align_column(zz500_weight, stklist)
                hs300_weight = self.__align_column(hs300_weight, stklist)
                self.__write2zz500hs300weight_file(tdays_data[len(zz500_weight):], zz500_weight,
                        hs300_weight, stklist)
            else:
                self.log.info('权重因子更新完毕')
        else:
            self.__write2zz500hs300weight_file(tdays_data, np.array([]),
                    np.array([]), stklist)

    def db_download_50weight_factor(self):
        """更新上证50权重
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'SZ50_weight.mat')):
            sz50_weight = sio.loadmat(DATAPATH+'SZ50_weight.mat')['SZ50_weight']
            if(len(sz50_weight) < len(tdays_data)):
                sz50_weight = self.__align_column(sz50_weight, stklist)
                self.__write2sz50weight_file(tdays_data[len(sz50_weight):], sz50_weight, stklist)
            else:
                self.log.info('50权重因子更新完毕')
        else:
            self.__write2sz50weight_file(tdays_data, np.array([]), stklist)

    def db_download_listdate_factor(self):
        """更新上市时间
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'listdate.mat')):
            listdate = sio.loadmat(DATAPATH+'listdate.mat')['listdate']
            listdate = [elt for elt in listdate[0]]
            if(len(listdate) < len(stklist)):
                self.__write2listdate_file(stklist)
            else:
                self.log.info('上市时间更新完毕')
        else:
            self.__write2listdate_file(stklist)
    
    def db_download_industry29_factor(self):
        """更新29个行业因子
        """
        tdays_data = self.tdays_data
        indlist = sio.loadmat(DATAPATH+'ind_code_name_CITIC_29.mat')['ind_code_name_CITIC_29']
        indlist = [elt[0][0] for elt in indlist]
        if(os.path.exists(DATAPATH+'indIndex_CITIC_29.mat')):
            indindex_citic29 = sio.loadmat(DATAPATH+'indIndex_CITIC_29.mat')['indIndex_CITIC_29']
            if(len(indindex_citic29) < len(tdays_data)):
                self.__write2ind29_file(tdays_data[len(indindex_citic29):], indindex_citic29, indlist)
            else:
                self.log.info('上市时间更新完毕')
        else:
            self.__write2ind29_file(tdays_data, np.array([]), indlist)
        
    def db_download_totalmv_factor(self):
        """更新总市值因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/TotalMV.mat')):
            totalmv = sio.loadmat(DATAPATH+'daily_factor/TotalMV.mat')['TotalMV']
            if(len(totalmv) < len(tdays_data)):
                totalmv = self.__align_column(totalmv, stklist)
                self.__write2totalmv_file(tdays_data[len(totalmv):], totalmv, stklist)
            else:
                self.log.info('市值因子更新完毕')
        else:
            self.__write2totalmv_file(tdays_data, np.array([]), stklist)
    
    def db_download_epfwd_factor(self):
        """更新EPFWD因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'newriskfactor/BarraSmallRisk/EPFWD.mat')):
            epfwd = sio.loadmat(DATAPATH+'newriskfactor/BarraSmallRisk/EPFWD.mat')['EPFWD']
            if(len(epfwd) < len(tdays_data)):
                epfwd = self.__align_column(epfwd, stklist)
                self.__write2epfwd_file(tdays_data[len(epfwd):], epfwd, stklist)
            else:
                self.log.info('EPFWD因子更新完毕')
        else:
            self.__write2epfwd_file(tdays_data, np.array([]), stklist)

    def db_download_dividend12m_factor(self):
        """更新股息率因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/dividend12M.mat')):
            dividend12m = sio.loadmat(DATAPATH+'daily_factor/dividend12M.mat')['dividend12M']
            if(len(dividend12m) < len(tdays_data)):
                dividend12m = self.__align_column(dividend12m, stklist)
                self.__write2dividend12m_file(tdays_data[len(dividend12m):], dividend12m, stklist)
            else:
                self.log.info('股息率因子更新完毕')
        else:
            self.__write2dividend12m_file(tdays_data, np.array([]), stklist)

    def db_download_tmv_factor(self):
        """更新总市值因子
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/TMV.mat')):
            tmv = sio.loadmat(DATAPATH+'daily_factor/TMV.mat')['TMV']
            if(len(tmv) < len(tdays_data)):
                tmv = self.__align_column(tmv, stklist)
                self.__write2tmv_file(tdays_data[len(tmv):], tmv, stklist)
            else:
                self.log.info('总市值因子更新完毕')
        else:
            self.__write2tmv_file(tdays_data, np.array([]), stklist)
        
    def __convert_mat2list(self, mat_ndarray):
        """把mat的高维数据转换成list类型
        :mat_ndarray: mat数据格式
        """
        return [elt[0][0] for elt in mat_ndarray]
        
    def __convert_dbdata2map(self, dbresult):
        """把DB格式的数据转化成映射表
        :dbresult: DB的查询结果
        """
        ret = dict()
        for elt in dbresult:
            ret[elt[0]] = elt[1]
        return ret

    def __extract_data_from_ndarray(self, np_array):
        """提取多维数组的数据变成一维行向量
        :np_array:列向量
        """
        tmp = np_array.reshape(1, len(np_array))[0]
        return [elt[0] for elt in tmp]
        
    def __convert_row2column(self, np_array):
        """把行向量转置成列向量
        :np_array:行向量
        """
        return np_array.reshape(len(np_array), 1)
    
    def __compare_two_list(self, srclist, dstlist):
        """判断两个list输出一个0/1的向量
        :srclist: 待判断列表
        :dstlist: 目标列表
        """
        s = set(dstlist)
        return [1 if elt in s else 0 for elt in srclist]
            
    def __write2aststockd_file(self, stklist, datelist, original):
        """把全A股票矩阵写入mat文件
        :stklist: 最新的股票列表
        :datelist: 时间序列
        :original: 原时间数列函数
        """
        cursor = self.conn.cursor()
        tmp = np.zeros((len(datelist), len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i][0][0], '%Y/%m/%d').strftime('%Y%m%d')
            sql1 = 'select s_info_windcode from Asharedescription where cast(s_info_listdate as integer) <= %s and (cast(s_info_delistdate as integer) > %s or s_info_delistdate is null)' % (sdate, sdate)
            sql2 = 'select s_info_windcode from  AShareST where cast(entry_dt as  integer) <= %s and (cast(remove_dt as  integer)> %s or remove_dt is null)' %(sdate, sdate)
            
            cursor.execute(sql1)
            rs1 = cursor.fetchall()
            rs1 = np.array(self.__compare_two_list(stklist, [elt[0] for elt in rs1]))

            cursor.execute(sql2)
            rs2 = cursor.fetchall()
            rs2 = np.array(self.__compare_two_list(stklist, [elt[0] for elt in rs2]))
            tmp[i] = rs1-rs2
            tmp[i][tmp[i] == 0] = np.nan

        if original.size == 0:
            sio.savemat(DATAPATH+'A_ST_stock_d', mdict={'A_ST_stock_d': tmp})
        else:
            original = np.vstack((original,tmp))
            sio.savemat(DATAPATH+'A_ST_stock_d', mdict={'A_ST_stock_d': original})

    def __write2indcitic_file(self, datelist, ind_code_name, ind_name, original, stklist):
        """写入中信行业分类
        :datelist: 时间序列
        :ind_code_name: 行业代码列表数据
        :ind_name: 行业名称列表数据
        :original: 原始股票行业数据序列
        :stklist: 股票列表
        """
        pass
    
    def __convert_dbdata2tuplelist(self, dbdata, elenum):
        """把DB的数据转换成tuple的列表
        :dbdata: DB返回的结果
        :elenum: 每个元祖中包含的元素个数
        """
        ret = dict()
        for i in range(elenum):
            ret[i] = list()
        for i in range(len(dbdata)):
            for j in range(elenum):
                ret[j].append(dbdata[i][j])
        return ret
    
    def __compensate_original_column(self, original, columnnum):
        """补足原始数据的列宽
        :original: 原始矩阵数据
        :columnnum: 需要补足的列宽数目
        """
        rownum = np.shape(original)[0]
        for i in range(columnnum):
            s = Series(np.zeros(rownum))
            col = self.__convert_row2column(s.replace(0, np.nan).values)
            original = np.concatenate([original, col], axis=1)
        return original

    def __align_column(self, original, target):
        """判断是否需要补足数据的列宽
        :original: 原始数据
        :target: 目标数据
        """
        if len(original) < len(target):
            colnum = np.shape(original)[1]
            original = self.__compensate_original_column(original, len(target) - colnum)
        return original
        
    def __write2stock_trade_able_file(self, datelist, original, stklist):
        """更新股票交易列表的状态
        :datelist: 时间序列
        :original: 原有数据序列
        :stklist: 股票列表
        """
        tmp_array = np.zeros((len(datelist), len(stklist)))
        cursor = self.conn.cursor()
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,s_dq_tradestatus from  AShareEODPrices  WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs1 = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs1, 2)
            s_info_windcode = ret[0]
            s_dq_tradestatus = ret[1]
            data1 = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k])
                    status = (np.nan if s_dq_tradestatus[idx] == None else s_dq_tradestatus[idx])
                    if(status == '交易' or
                       status == 'XD' or
                       status == 'XR' or
                       status == 'DR'):
                        data1.append(1)
                    else:
                        data1.append(0)
                else:
                    data1.append(0)

            sq2 = 'select s_info_windcode,up_down_limit_status from AShareEODDerivativeIndicator   WHERE trade_dt=%s' % sdate
            cursor.execute(sq2)
            rs2 = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs2, 2)
            s_info_windcode = ret[0]
            up_down_limit_status = ret[1]
            data2 = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k])
                    status = up_down_limit_status[idx]
                    data2.append(1 if status == 0 else 0)
                else:
                    data2.append(0)
             
            tmp_array[i] = np.array(data1) & np.array(data2)
        if original.size == 0:
            sio.savemat(DATAPATH+'stock_trade_able.mat', mdict={'stock_trade_able':tmp_array})
        else:
            original = np.vstack((original, tmp_array))
            sio.savemat(DATAPATH+'stock_trade_able.mat', mdict={'stock_trade_able':original})
    
    def __write2tradestatus_file(self, datelist, original, stklist):
        """更新股票交易列表(tradestatus)的状态
        :datelist: 时间序列
        :original: 原有数据序列
        :stklist: 股票列表
        """
        tmp_array = np.zeros((len(datelist), len(stklist)))
        cursor = self.conn.cursor()
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            print(sdate)
            sql = 'select s_info_windcode,s_dq_tradestatus from  AShareEODPrices  WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            s_dq_tradestatus = ret[1]
            data = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    status = s_dq_tradestatus[s_info_windcode.index(stklist[k])] 
                    if(status == '交易' or
                       status == 'XD' or
                       status == 'XR' or
                       status == 'DR'):
                        data.append(1)
                    else:
                        data.append(0)
                else:
                    data.append(np.nan)
            tmp_array[i] = np.array(data)
        if original.size == 0:
            sio.savemat(DATAPATH+'tradestatus.mat', mdict={'tradestatus':tmp_array})
        else:
            original = np.vstack((original, tmp_array))
            sio.savemat(DATAPATH+'tradestatus.mat', mdict={'tradestatus':original})

    def __write2udlimitstatus_file(self, datelist, original, stklist):
        """更新股票涨停的状态
        :datelist: 时间序列
        :original: 原有数据序列
        :stklist: 股票列表0为没有涨跌停,1为涨停,-1为跌停,2为没有找到该股票
        """
        tmp_array = np.zeros((len(datelist), len(stklist))) 
        cursor = self.conn.cursor()
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,up_down_limit_status from AShareEODDerivativeIndicator   WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            up_down_limit_status = ret[1]
            data = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    status = up_down_limit_status[s_info_windcode.index(stklist[k])] 
                    data.append(status)
                else:
                    data.append(float('nan'))
            tmp_array[i] = np.array(data)

        if original.size == 0:
            sio.savemat(DATAPATH+'up_down_limit_status.mat', mdict={'up_down_limit_status':tmp_array})
        else:
            original = np.vstack((original, tmp_array))
            sio.savemat(DATAPATH+'up_down_limit_status.mat', mdict={'up_down_limit_status':original})
    
    def __write2price_factor_file(self, datelist, price_original,
            adjfactor_original, volume_original,
            amount_original, TR_original, stklist):
        """把价格相关的因子写入文件
        :datelist: 时间序列
        :price_original: 原始价格序列
        :adjfactor_original: 复权因子序列
        :volume_original: 成交量序列
        :amount_original: 成交额序列
        :TR_original: 换手率序列
        :stklist_original: 股票代码
        """
        cursor = self.conn.cursor()
        tmp_close = np.zeros((len(datelist), len(stklist)))
        tmp_adjfactor = np.zeros((len(datelist), len(stklist)))
        tmp_volume = np.zeros((len(datelist), len(stklist)))
        tmp_amount = np.zeros((len(datelist), len(stklist)))
        tmp_tr = np.zeros((len(datelist), len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            print(sdate)
            sql = 'select s_info_windcode,s_dq_close,s_dq_adjfactor,s_dq_volume,s_dq_amount from  AShareEODPrices   WHERE trade_dt=%s'%sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 5)
            s_info_windcode = ret[0]
            s_dq_close = ret[1]
            s_dq_adjfactor = ret[2]
            s_dq_volume = ret[3]
            s_dq_amount = ret[4]
            close = list()
            adjfactor = list()
            volume = list()
            amount = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    close.append(s_dq_close[idx])
                    adjfactor.append(s_dq_adjfactor[idx])
                    volume.append(s_dq_volume[idx]*100)
                    amount.append(s_dq_amount[idx]*1000)
                else:
                    close.append(np.nan)
                    adjfactor.append(np.nan)
                    volume.append(np.nan)
                    amount.append(np.nan)

            tmp_close[i] = np.array(close)
            tmp_adjfactor[i] = np.array(adjfactor)
            tmp_volume[i] = np.array(volume)
            tmp_amount[i] = np.array(amount)

            sql = 'select s_info_windcode,turnover_d from  AShareYield   WHERE trade_dt= %s'%sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            turnover_d = ret[1]
            turnover = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k])
                    turnover.append(turnover_d[idx])
                else:
                    turnover.append(np.nan)

            #turnover = Series(turnover).fillna(method='ffill').values
            tmp_tr[i] = turnover

        if price_original.size == 0:
            sio.savemat(DATAPATH+'price_original.mat', mdict={'price_original':tmp_close})
            sio.savemat(DATAPATH+'adjfactor.mat', mdict={'adjfactor':tmp_adjfactor})
            sio.savemat(DATAPATH+'volume.mat', mdict={'volume':tmp_volume})
            sio.savemat(DATAPATH+'amount.mat', mdict={'amount':tmp_amount})
            sio.savemat(DATAPATH+'TR.mat', mdict={'TR':tmp_tr})
            price_original = np.zeros(np.shape(tmp_close))
            price_original = tmp_close
        else:
            price_original = np.vstack((price_original, tmp_close))
            adjfactor_original = np.vstack((adjfactor_original, tmp_adjfactor))
            volume_original = np.vstack((volume_original, tmp_volume))
            amount_original = np.vstack((amount_original, tmp_amount))
            TR_original = np.vstack((TR_original, tmp_tr))
            
            sio.savemat(DATAPATH+'price_original.mat', mdict={'price_original':price_original})
            sio.savemat(DATAPATH+'adjfactor.mat', mdict={'adjfactor':adjfactor_original})
            sio.savemat(DATAPATH+'volume.mat', mdict={'volume':volume_original})
            sio.savemat(DATAPATH+'amount.mat', mdict={'amount':amount_original})
            sio.savemat(DATAPATH+'TR.mat', mdict={'TR':TR_original})

        #计算前复权价格
        price_forward_adjusted = np.zeros(np.shape(price_original))
        for i in range(len(price_original)):
            price_forward_adjusted[i] = price_original[i] * adjfactor_original[i] / adjfactor_original[-1]
        self.price_forward_adjusted = price_forward_adjusted 

        sio.savemat(DATAPATH+'price_forward_adjusted.mat', mdict={'price_forward_adjusted':price_forward_adjusted})
        sio.savemat(DATAPATH+'daily_factor/price_daily_1.mat', mdict={'price_daily_1':price_original})

    def __write2price_file(self, datelist, open_original, high_original,
            low_original, stklist, adjfactor):
        """把价格因子写入文件
        :datelist: 时间序列
        :open_original: 原始开盘价格
        :high_original: 原始最高价格
        :low_original: 原始最低价格
        :stklist: 股票列表
        :adjfactor: 复权因子矩阵
        """
        cursor = self.conn.cursor()
        tmp_open = np.zeros((len(datelist),len(stklist)))
        tmp_high = np.zeros((len(datelist),len(stklist)))
        tmp_low = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            print(sdate)
            sql = 'select s_info_windcode,s_dq_open,s_dq_high,s_dq_low from  AShareEODPrices   WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 4)
            s_info_windcode = ret[0]
            s_dq_open = ret[1]
            s_dq_high = ret[2]
            s_dq_low = ret[3]
            dq_open = list()
            dq_high = list()
            dq_low = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    dq_open.append(s_dq_open[idx])
                    dq_high.append(s_dq_high[idx])
                    dq_low.append(s_dq_low[idx])
                else:
                    dq_open.append(np.nan)
                    dq_high.append(np.nan)
                    dq_low.append(np.nan)

            tmp_open[i] = np.array(dq_open)
            tmp_high[i] = np.array(dq_high)
            tmp_low[i] = np.array(dq_low)
        if open_original.size == 0:
            sio.savemat(DATAPATH+'open_original.mat', mdict={'open_original':tmp_open})
            sio.savemat(DATAPATH+'high_original.mat', mdict={'high_original':tmp_high})
            sio.savemat(DATAPATH+'low_original.mat', mdict={'low_original':tmp_low})
            open_original = np.array(np.shape(tmp_open))
            open_original = tmp_open
            high_original = np.array(np.shape(tmp_hight))
            high_original = tmp_high
            low_original = np.array(np.shape(tmp_low))
            low_original = tmp_low
        else:
            open_original = np.vstack((open_original, tmp_open))
            high_original = np.vstack((high_original, tmp_high))
            low_original = np.vstack((low_original, tmp_low))
            sio.savemat(DATAPATH+'open_original.mat', mdict={'open_original':open_original})
            sio.savemat(DATAPATH+'high_original.mat', mdict={'high_original':high_original})
            sio.savemat(DATAPATH+'low_original.mat', mdict={'low_original':low_original})

        #计算各种前复权的价格
        open_forward_adjusted = np.zeros(np.shape(open_original))
        high_forward_adjusted = np.zeros(np.shape(high_original))
        low_forward_adjusted = np.zeros(np.shape(low_original))
        for i in range(len(open_original)):
            open_forward_adjusted[i] = open_original[i] * adjfactor[i] / adjfactor[-1]
            high_forward_adjusted[i] = high_original[i] * adjfactor[i] / adjfactor[-1]
            low_forward_adjusted[i] = low_original[i] * adjfactor[i] / adjfactor[-1]

        sio.savemat(DATAPATH+'open_forward_adjusted.mat', mdict={'open_forward_adjusted':open_forward_adjusted})
        sio.savemat(DATAPATH+'high_forward_adjusted.mat', mdict={'high_forward_adjusted':high_forward_adjusted})
        sio.savemat(DATAPATH+'low_forward_adjusted.mat', mdict={'low_forward_adjusted':low_forward_adjusted})

    def __write2bescfp_file(self, datelist, bp_original,
        ep_original, sp_original, cfp_original, stklist):
        """把基本面因子写入文件
        :datelist: 时间序列
        :bp_original: BP原始因子数据
        :ep_original: EP原始因子数据
        :sp_original: SP原始因子数据
        :cfp_original: CFP原始因子数据
        :stklist: 股票序列
        """
        cursor = self.conn.cursor()
        tmp_bp = np.zeros((len(datelist),len(stklist)))
        tmp_ep = np.zeros((len(datelist),len(stklist)))
        tmp_sp = np.zeros((len(datelist),len(stklist)))
        tmp_cfp = np.zeros((len(datelist),len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            print(sdate)
            sql = 'select s_info_windcode,s_val_pb_new,s_val_pe_ttm,s_val_ps_ttm,s_val_pcf_ocfttm from  AShareEODDerivativeIndicator   WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 5)
            s_info_windcode = ret[0]
            s_val_pb_new = ret[1]
            s_val_pe_ttm = ret[2]
            s_val_ps_ttm = ret[3]
            s_val_pcf_ocfttm = ret[4]
            bp = list()
            ep = list()
            sp = list()
            cfp = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    bp.append(np.nan if s_val_pb_new[idx] == None else s_val_pb_new[idx])
                    ep.append(np.nan if s_val_pe_ttm[idx] == None else s_val_pe_ttm[idx])
                    sp.append(np.nan if s_val_ps_ttm[idx] == None else s_val_ps_ttm[idx])
                    cfp.append(np.nan if s_val_pcf_ocfttm[idx] == None else s_val_pcf_ocfttm[idx])
                else:
                    bp.append(np.nan)
                    ep.append(np.nan)
                    sp.append(np.nan)
                    cfp.append(np.nan)

            tmp_bp[i] = 1.0 / np.array(bp)
            tmp_ep[i] = 1.0 / np.array(ep)
            tmp_sp[i] = 1.0 / np.array(sp)
            tmp_cfp[i] = 1.0 / np.array(cfp)

        if bp_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/BP.mat', mdict={'BP':tmp_bp})
            sio.savemat(DATAPATH+'daily_factor/EP.mat', mdict={'EP':tmp_ep})
            sio.savemat(DATAPATH+'daily_factor/SP.mat', mdict={'SP':tmp_sp})
            sio.savemat(DATAPATH+'daily_factor/CFP.mat', mdict={'CFP':tmp_cfp})
        else:
            bp_original = np.vstack((bp_original, tmp_bp))
            ep_original = np.vstack((ep_original, tmp_ep))
            sp_original = np.vstack((sp_original, tmp_sp))
            cfp_original = np.vstack((cfp_original, tmp_cfp))
            sio.savemat(DATAPATH+'daily_factor/BP.mat', mdict={'BP':bp_original})
            sio.savemat(DATAPATH+'daily_factor/EP.mat', mdict={'EP':ep_original})
            sio.savemat(DATAPATH+'daily_factor/SP.mat', mdict={'SP':sp_original})
            sio.savemat(DATAPATH+'daily_factor/CFP.mat', mdict={'CFP':cfp_original})

    def __write2tps_file(self, datelist, tps_original,
            tps_180_original, stklist, price_forward_adjusted):
        """把TPS因子写入文件
        :datelist: 时间序列
        :tps_original: TPS原始因子数据
        :tps_180_original: TPS_180原始因子数据
        :stklist: 股票序列
        :price_forward_adjuested: 前复权价格矩阵
        """
        cursor = self.conn.cursor()
        tmp_tps = np.zeros((len(datelist),len(stklist)))
        tmp_tps_180 = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,s_est_price from  AShareStockRatingConsus   WHERE rating_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            s_est_price = ret[1]
            est_price = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    est_price.append(np.nan if s_est_price[idx] == None else s_est_price[idx])
                else:
                    est_price.append(0)
            tmp_tps[i] = np.array(est_price)
         
        if tps_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/TPS.mat', mdict={'TPS':tmp_tps})
            tps_tmps_180 = tmp_tps / price_forward_adjusted  - 1
            sio.savemat(DATAPATH+'daily_factor/TPS_180.mat', mdict={'TPS_180':tmp_tps_180})
        else:
            tps_original = np.vstack((tps_original, tmp_tps))
            tps_180_original = tps_original / price_forward_adjusted  - 1
            sio.savemat(DATAPATH+'daily_factor/TPS.mat', mdict={'TPS':tps_original})
            sio.savemat(DATAPATH+'daily_factor/TPS_180.mat', mdict={'TPS_180':tps_180_original})
            
    def __write2wratingupgrade_file(self, datelist, original, stklist):
        """更新wratingupgrade因子
        :datelist: 时间序列
        :original: 原始因子序列
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_wrating_upgrade = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            print(sdate)
            sql = 'select s_info_windcode,s_wrating_upgrade from  AShareStockRatingConsus WHERE rating_dt=%s and s_wrating_cycle=0263002000' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            s_wrating_upgrade = ret[1]
            wrating_upgrade = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    wrating_upgrade.append(np.nan if s_wrating_upgrade[idx] == None else s_wrating_upgrade[idx])
                else:
                    wrating_upgrade.append(0)
            tmp_wrating_upgrade[i] = np.array(wrating_upgrade)
         
        if original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/wrating_upgrade.mat', mdict={'wrating_upgrade':tmp_wrating_upgrade})
        else:
            original = np.vstack((original, tmp_wrating_upgrade))
            sio.savemat(DATAPATH+'daily_factor/wrating_upgrade.mat', mdict={'wrating_upgrade':original})
        
    def __write2forcast_file(self, datelist, roe_original,
            pe_original, pb_original, peg_original, stklist):
        """把盈利预测因子写入文件
        :datelist: 更新时间序列
        :roe_original: ROE原始因子序列
        :pe_original: PE原始因子序列
        :pb_original: PB原始因子序列
        :peg_original: PEG原始因子序列
        :stklist: 股票序列
        """
        cursor = self.conn.cursor()
        tmp_roe = np.zeros((len(datelist),len(stklist)))
        tmp_pe = np.zeros((len(datelist),len(stklist)))
        tmp_pb = np.zeros((len(datelist),len(stklist)))
        tmp_peg = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            print(sdate)
            sql = "select s_info_windcode,est_roe,est_pe,est_pb,est_peg from  AshareConsensusRollingData   WHERE rolling_type='FY1' and est_dt=%s" % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 5)
            s_info_windcode = ret[0]
            est_roe = ret[1]
            est_pe = ret[2]
            est_pb = ret[3]
            est_peg = ret[4]
            roe = list()
            pe = list()
            pb = list()
            peg = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    roe.append(np.nan if est_roe[idx] == None else est_roe[idx])
                    pe.append(np.nan if est_pe[idx] == None else est_pe[idx])
                    pb.append(np.nan if est_pb[idx] == None else est_pb[idx])
                    peg.append(np.nan if est_peg[idx] == None else est_peg[idx])
                else:
                    roe.append(np.nan)
                    pe.append(np.nan)
                    pb.append(np.nan)
                    peg.append(np.nan)
            tmp_roe[i] = np.array(roe)
            tmp_pe[i] = np.array(pe)
            tmp_pb[i] = np.array(pb)
            tmp_peg[i] = np.array(peg)

        if roe_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_FY1.mat', mdict={'roe_forcast_FY1':tmp_roe})
            sio.savemat(DATAPATH+'daily_factor/pe_forcast_FY1.mat', mdict={'pe_forcast_FY1':tmp_pe})
            sio.savemat(DATAPATH+'daily_factor/pb_forcast_FY1.mat', mdict={'pb_forcast_FY1':tmp_pb})
            sio.savemat(DATAPATH+'daily_factor/peg_forcast_FY1.mat', mdict={'peg_forcast_FY1':tmp_peg})
        else:
            roe_original = np.vstack((roe_original, tmp_roe))
            pe_original = np.vstack((pe_original, tmp_pe))
            pb_original = np.vstack((pb_original, tmp_pb))
            peg_original = np.vstack((peg_original, tmp_peg))
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_FY1.mat', mdict={'roe_forcast_FY1':roe_original})
            sio.savemat(DATAPATH+'daily_factor/pe_forcast_FY1.mat', mdict={'pe_forcast_FY1':pe_original})
            sio.savemat(DATAPATH+'daily_factor/pb_forcast_FY1.mat', mdict={'pb_forcast_FY1':pb_original})
            sio.savemat(DATAPATH+'daily_factor/peg_forcast_FY1.mat', mdict={'peg_forcast_FY1':peg_original})
        
    def __write2forcastfttm_file(self, datelist, roefttm_original,
            pefttm_original, pbfttm_original, pegfttm_original, stklist):
        """把盈利预测因子写入文件
        :datelist: 更新时间序列
        :roefttm_original: ROE原始因子序列
        :pefttm_original: PE原始因子序列
        :pbfttm_original: PB原始因子序列
        :pegfttm_original: PEG原始因子序列
        :stklist: 股票序列
        """
        cursor = self.conn.cursor()
        tmp_roe = np.zeros((len(datelist),len(stklist)))
        tmp_pe = np.zeros((len(datelist),len(stklist)))
        tmp_pb = np.zeros((len(datelist),len(stklist)))
        tmp_peg = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = "select s_info_windcode,est_roe,est_pe,est_pb,est_peg from  AshareConsensusRollingData   WHERE rolling_type='FTTM' and est_dt=%s" % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 5)
            s_info_windcode = ret[0]
            est_roe = ret[1]
            est_pe = ret[2]
            est_pb = ret[3]
            est_peg = ret[4]
            roe = list()
            pe = list()
            pb = list()
            peg = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    roe.append(est_roe[idx])
                    pe.append(est_pe[idx])
                    pb.append(est_pb[idx])
                    peg.append(est_peg[idx])
                else:
                    roe.append(np.nan)
                    pe.append(np.nan)
                    pb.append(np.nan)
                    peg.append(np.nan)
            tmp_roe[i] = np.array(roe)
            tmp_pe[i] = np.array(pe)
            tmp_pb[i] = np.array(pb)
            tmp_peg[i] = np.array(peg)

        if roefttm_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_FTTM.mat', mdict={'roe_forcast_FTTM':tmp_roe})
            sio.savemat(DATAPATH+'daily_factor/pe_forcast_FTTM.mat', mdict={'pe_forcast_FTTM':tmp_pe})
            sio.savemat(DATAPATH+'daily_factor/pb_forcast_FTTM.mat', mdict={'pb_forcast_FTTM':tmp_pb})
            sio.savemat(DATAPATH+'daily_factor/peg_forcast_FTTM.mat', mdict={'peg_forcast_FTTM':tmp_peg})
        else:
            roefttm_original = np.vstack((roefttm_original, tmp_roe))
            pefttm_original = np.vstack((pefttm_original, tmp_pe))
            pbfttm_original = np.vstack((pbfttm_original, tmp_pb))
            pegfttm_original = np.vstack((pegfttm_original, tmp_peg))
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_FTTM.mat', mdict={'roe_forcast_FTTM':roefttm_original})
            sio.savemat(DATAPATH+'daily_factor/pe_forcast_FTTM.mat', mdict={'pe_forcast_FTTM':pefttm_original})
            sio.savemat(DATAPATH+'daily_factor/pb_forcast_FTTM.mat', mdict={'pb_forcast_FTTM':pbfttm_original})
            sio.savemat(DATAPATH+'daily_factor/peg_forcast_FTTM.mat', mdict={'peg_forcast_FTTM':pegfttm_original})
        
    def __write2forcastyoy_file(self, datelist, netprofit_original,
            roeyoy_original, stklist):
        """把盈利预测因子写入文件
        :datelist: 更新时间序列
        :netprofit_original: NETPROFIT原始因子序列
        :roeyoy_original: ROEYOY原始因子序列
        :stklist: 股票序列
        """
        cursor = self.conn.cursor()
        tmp_netprofit = np.zeros((len(datelist), len(stklist)))
        tmp_roe = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = "select s_info_windcode,net_profit,est_roe from  AshareConsensusRollingData   WHERE rolling_type='YOY' and est_dt=%s" % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 3)
            s_info_windcode = ret[0]
            net_profit = ret[1]
            est_roe = ret[2]
            netprofit = list()
            roe = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    netprofit.append(np.nan if net_profit[idx] == None else net_profit[idx])
                    roe.append(np.nan if est_roe[idx] == None else est_roe[idx])
                else:
                    netprofit.append(np.nan)
                    roe.append(np.nan)

            tmp_netprofit[i] = np.array(netprofit)
            tmp_roe[i] = np.array(roe)

        if netprofit_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/netprofit_forcast_YOY.mat', mdict={'netprofit_forcast_YOY':tmp_netprofit})
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_YOY.mat', mdict={'roe_forcast_YOY':tmp_roe})
        else:
            netprofit_original = np.vstack((netprofit_original, tmp_netprofit))
            roeyoy_original = np.vstack((roeyoy_original, tmp_roe))
            sio.savemat(DATAPATH+'daily_factor/netprofit_forcast_YOY.mat', mdict={'netprofit_forcast_YOY':netprofit_original})
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_YOY.mat', mdict={'roe_forcast_YOY':roeyoy_original})

    def __write2cagr_file(self, datelist, netprofit_original, roe_original, stklist):
        """把CAGR因子写入文件
        :datelist: 时间序列
        :netprofit_original: netprofitCAGR的原始数据
        :roe_original: roeCAGR的原始数据
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_roe = np.zeros((len(datelist),len(stklist)))
        tmp_netprofit = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = "select s_info_windcode,net_profit,est_roe from  AshareConsensusRollingData   WHERE rolling_type='CAGR' and est_dt=%s" % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 3)
            s_info_windcode = ret[0]
            net_profit = ret[1]
            est_roe = ret[2]
            netprofit = list()
            roe = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    netprofit.append(np.nan if net_profit[idx] == None else net_profit[idx])
                    roe.append(np.nan if est_roe[idx] == None else est_roe[idx])
                else:
                    netprofit.append(np.nan)
                    roe.append(np.nan)

            tmp_netprofit[i] = np.array(netprofit)
            tmp_roe[i] = np.array(roe)

        if netprofit_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/netprofit_forcast_CAGR.mat', mdict={'netprofit_forcast_CAGR':tmp_netprofit})
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_CAGR.mat', mdict={'roe_forcast_CAGR':tmp_roe})
        else:
            netprofit_original = np.vstack((netprofit_original, tmp_netprofit))
            roe_original = np.vstack((roe_original, tmp_roe))
            sio.savemat(DATAPATH+'daily_factor/netprofit_forcast_CAGR.mat', mdict={'netprofit_forcast_CAGR':netprofit_original})
            sio.savemat(DATAPATH+'daily_factor/roe_forcast_CAGR.mat', mdict={'roe_forcast_CAGR':roe_original})

    def __write2broker_file(self, datelist, rating_avg_original, rating_net_upgrade_original, rating_instnum, stklist):
        """把券商评级的因子写入文件
        :datelist: 时间序列
        :rating_avg_original: rating_avg原始数据
        :rating_net_upgrade_original: rating_net_upgrade原始数据
        :rating_instnum: rating_instum原始数据
        """
        
        cursor = self.conn.cursor()
        tmp_rating_avg = np.zeros((len(datelist),len(stklist)))
        tmp_rating_net_upgrade = np.zeros((len(datelist),len(stklist)))
        tmp_instnum = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,s_wrating_avg,s_wrating_upgrade,s_wrating_downgrade,s_wrating_instnum from  AShareStockRatingConsus   WHERE rating_dt=%s and s_wrating_cycle=0263002000' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 5)
            s_info_windcode = ret[0]
            s_wrating_avg = ret[1]
            s_wrating_upgrade = ret[2]
            s_wrating_downgrade = ret[3]
            s_wrating_instnum = ret[4]
            wrating_avg = list()
            wrating_upgrade = list()
            wrating_downgrade = list()
            wrating_instnum = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    wrating_avg.append(np.nan if s_wrating_avg[idx] == None else s_wrating_avg[idx])
                    wrating_upgrade.append(np.nan if s_wrating_upgrade[idx] == None else s_wrating_upgrade[idx])
                    wrating_downgrade.append(np.nan if s_wrating_downgrade[idx] == None else s_wrating_downgrade[idx])
                    wrating_instnum.append(np.nan if s_wrating_instnum[idx] == None else s_wrating_instnum[idx])
                else:
                    wrating_avg.append(np.nan)
                    wrating_upgrade.append(np.nan)
                    wrating_downgrade.append(np.nan)
                    wrating_instnum.append(np.nan)
            tmp_rating_avg[i] = np.array(wrating_avg)
            tmp_rating_net_upgrade[i] = np.array(wrating_upgrade)
            tmp_instnum[i] = np.array(wrating_instnum)

        if rating_avg_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/rating_avg.mat', mdict={'rating_avg':tmp_rating_avg})
            sio.savemat(DATAPATH+'daily_factor/rating_net_upgrade.mat', mdict={'rating_net_upgrade':tmp_rating_net_upgrade})
            sio.savemat(DATAPATH+'daily_factor/rating_instnum.mat', mdict={'rating_instnum':tmp_instnum})
        else:
            rating_avg_original = np.vstack((rating_avg_original, tmp_rating_avg))
            rating_net_upgrade_original = np.vstack((rating_net_upgrade_original, tmp_rating_net_upgrade))
            rating_instnum_original = np.vstack((rating_instnum, tmp_instnum))
            sio.savemat(DATAPATH+'daily_factor/rating_avg.mat', mdict={'rating_avg':rating_avg_original})
            sio.savemat(DATAPATH+'daily_factor/rating_net_upgrade.mat', mdict={'rating_net_upgrade':rating_net_upgrade_original})
            sio.savemat(DATAPATH+'daily_factor/rating_instnum.mat', mdict={'rating_instnum':rating_instnum_original})

    def __write2vdsta_file(self, datelist, vdsta_original, vdmta_original,
            vdlta_original, vdia_original, odsta_original, odmta_original,
            odlta_original, odia_original, stklist):
        """把机构散户大户成交量占比写入文件
        :datelist: 时间序列
        :vdsta_original: vdsta原始数据序列
        :vdmta_original: vdmta原始数据序列
        :vdlta_original: vdlta原始数据序列
        :vdia_original: vdia原始数据序列
        :odsta_original: odsta原始数据序列
        :odmta_original: odmta原始数据序列
        :odlta_original: odlta原始数据序列
        :odia_original: odia原始数据序列
        :stklist: 股票列表
        """

        cursor = self.conn.cursor()
        tmp_vdsta = np.zeros((len(datelist),len(stklist)))
        tmp_vdmta = np.zeros((len(datelist),len(stklist)))
        tmp_vdlta = np.zeros((len(datelist),len(stklist)))
        tmp_vdia = np.zeros((len(datelist),len(stklist)))

        tmp_odsta = np.zeros((len(datelist),len(stklist)))
        tmp_odmta = np.zeros((len(datelist),len(stklist)))
        tmp_odlta = np.zeros((len(datelist),len(stklist)))
        tmp_odia = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,value_diff_small_trader_act,value_diff_med_trader_act,value_diff_large_trader_act,value_diff_institute_act,volume_diff_small_trader_act,volume_diff_med_trader_act,volume_diff_large_trader_act,volume_diff_institute_act  from  AShareMoneyflow   WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 9)
            s_info_windcode = ret[0]

            value_diff_small_trader_act = ret[1]
            value_diff_med_trader_act = ret[2]
            value_diff_large_trader_act = ret[3]
            value_diff_institude_act = ret[4]
            
            volume_diff_small_trader_act = ret[5]
            volume_diff_med_trader_act = ret[6]
            volume_diff_large_trader_act = ret[7]
            volume_diff_institude_act = ret[8]
            
            vdsta = list()
            vdmta = list()
            vdlta = list()
            vdia = list()

            odsta = list()
            odmta = list()
            odlta = list()
            odia = list()
            
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    vdsta.append(np.nan if value_diff_small_trader_act[idx] == None else value_diff_small_trader_act[idx])
                    vdmta.append(np.nan if value_diff_med_trader_act[idx] == None else value_diff_med_trader_act[idx])
                    vdlta.append(np.nan if value_diff_large_trader_act[idx] == None else value_diff_large_trader_act[idx])
                    vdia.append(np.nan if value_diff_institude_act[idx] == None else value_diff_institude_act[idx])
                    
                    odsta.append(np.nan if volume_diff_small_trader_act[idx] == None else volume_diff_small_trader_act[idx])
                    odmta.append(np.nan if volume_diff_med_trader_act[idx] == None else volume_diff_med_trader_act[idx])
                    odlta.append(np.nan if volume_diff_large_trader_act[idx] == None else volume_diff_large_trader_act[idx])
                    odia.append(np.nan if volume_diff_institude_act[idx] == None else volume_diff_institude_act[idx])
                else:
                    vdsta.append(np.nan)
                    vdmta.append(np.nan)
                    vdlta.append(np.nan)
                    vdia.append(np.nan)
                    
                    odsta.append(np.nan)
                    odmta.append(np.nan)
                    odlta.append(np.nan)
                    odia.append(np.nan)

            tmp_vdsta[i] = vdsta
            tmp_vdmta[i] = vdmta
            tmp_vdlta[i] = vdlta
            tmp_vdia[i] = vdia

            tmp_odsta[i] = odsta
            tmp_odmta[i] = odmta
            tmp_odlta[i] = odlta
            tmp_odia[i] = odia

        if vdsta_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/value_diff_small_trader_act.mat', mdict={'value_diff_small_trader_act':tmp_vdsta})
            sio.savemat(DATAPATH+'daily_factor/value_diff_med_trader_act.mat', mdict={'value_diff_med_trader_act':tmp_vdmta})
            sio.savemat(DATAPATH+'daily_factor/value_diff_large_trader_act.mat', mdict={'value_diff_large_trader_act':tmp_vdlta})
            sio.savemat(DATAPATH+'daily_factor/value_diff_institude_act.mat', mdict={'value_diff_institute_act':tmp_vdia})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_small_trader_act.mat', mdict={'volume_diff_small_trader_act':tmp_odsta})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_med_trader_act.mat', mdict={'volume_diff_med_trader_act':tmp_odmta})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_large_trader_act.mat', mdict={'volume_diff_large_trader_act':tmp_odlta})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_institude_act.mat', mdict={'volume_diff_institute_act':tmp_odia})
        else:
            vdsta_original = np.vstack((vdsta_original, tmp_vdsta))
            vdmta_original = np.vstack((vdmta_original, tmp_vdmta))
            vdlta_original = np.vstack((vdlta_original, tmp_vdlta))
            vdia_original = np.vstack((vdia_original, tmp_vdia))
            odsta_original = np.vstack((odsta_original, tmp_odsta))
            odmta_original = np.vstack((odmta_original, tmp_odmta))
            odlta_original = np.vstack((odlta_original, tmp_odlta))
            odia_original = np.vstack((odia_original, tmp_odia))
            sio.savemat(DATAPATH+'daily_factor/value_diff_small_trader_act.mat', mdict={'value_diff_small_trader_act':vdsta_original})
            sio.savemat(DATAPATH+'daily_factor/value_diff_med_trader_act.mat', mdict={'value_diff_med_trader_act':vdmta_original})
            sio.savemat(DATAPATH+'daily_factor/value_diff_large_trader_act.mat', mdict={'value_diff_large_trader_act':vdlta_original})
            sio.savemat(DATAPATH+'daily_factor/value_diff_institude_act.mat', mdict={'value_diff_institute_act':vdia_original})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_small_trader_act.mat', mdict={'volume_diff_small_trader_act':odsta_original})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_med_trader_act.mat', mdict={'volume_diff_med_trader_act':odmta_original})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_large_trader_act.mat', mdict={'volume_diff_large_trader_act':odlta_original})
            sio.savemat(DATAPATH+'daily_factor/volume_diff_institude_act.mat', mdict={'volume_diff_institute_act':odia_original})

    def __write2mfpct_file(self,  datelist, mf_pct_volume_original, mf_pct_value_original, stklist):
        """把流动性因子写入文件
        :datelist: 时间序列
        :mf_pct_volume_original: 成交量原始数据序列
        :mf_pct_value_original: 成交额原始数据序列
        :stklist: 股票列表
        """
        
        cursor = self.conn.cursor()
        tmp_mf_pct_volume = np.zeros((len(datelist),len(stklist)))
        tmp_mf_pct_value = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,moneyflow_pct_volume,moneyflow_pct_value  from  AShareMoneyflow   WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 3)
            s_info_windcode = ret[0]
            moneyflow_pct_volume = ret[1]
            moneyflow_pct_value = ret[2]
            mf_pct_volume = list()
            mf_pct_value = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    mf_pct_volume.append(np.nan if moneyflow_pct_volume[idx] == None else moneyflow_pct_volume[idx])
                    mf_pct_value.append(np.nan if moneyflow_pct_value[idx] == None else moneyflow_pct_value[idx])
                else:
                    mf_pct_volume.append(np.nan)
                    mf_pct_value.append(np.nan)
            tmp_mf_pct_volume[i] = np.array(mf_pct_volume)
            tmp_mf_pct_value[i] = np.array(mf_pct_value)

        if mf_pct_volume_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/moneyflow_pct_volume.mat', mdict={'moneyflow_pct_volume':tmp_mf_pct_volume})
            sio.savemat(DATAPATH+'daily_factor/moneyflow_pct_value.mat', mdict={'moneyflow_pct_value':tmp_mf_pct_value})
        else:
            mf_pct_volume_original = np.vstack((mf_pct_volume_original, tmp_mf_pct_volume))
            mf_pct_value_original = np.vstack((mf_pct_value_original, tmp_mf_pct_value))
            sio.savemat(DATAPATH+'daily_factor/moneyflow_pct_volume.mat', mdict={'moneyflow_pct_volume':mf_pct_volume_original})
            sio.savemat(DATAPATH+'daily_factor/moneyflow_pct_value.mat', mdict={'moneyflow_pct_value':mf_pct_value_original})

    def __write2zz500hs300weight_file(self, datelist, zz500weight_original, hs300weight_original, stklist):
        """把指数权重因子写入文件
        :datelist: 时间序列
        :zz500weight_original: zz500权重原始因子
        :hs300weight_original: hs300权重原始因子
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_zz500weight = np.zeros((len(datelist),len(stklist)))
        tmp_hs300weight = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_con_windcode,i_weight FROM AINDEXHS300CloseWeight WHERE  TRADE_DT=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_con_windcode = ret[0]
            i_weight = ret[1]
            hs300weight = list()
            for k in range(len(stklist)):
                if stklist[k] in s_con_windcode:
                    idx = s_con_windcode.index(stklist[k]) 
                    hs300weight.append(np.nan if i_weight[idx] == None else i_weight[idx])
                else:
                    hs300weight.append(0)
            sql = 'select s_con_windcode,weight FROM AINDEXCSI500Weight WHERE  TRADE_DT=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_con_windcode = ret[0]
            weight = ret[1]
            zz500weight = list()
            for k in range(len(stklist)):
                if stklist[k] in s_con_windcode:
                    idx = s_con_windcode.index(stklist[k])
                    zz500weight.append(np.nan if weight[idx] == None else weight[idx])
                else:
                    zz500weight.append(0)
            
            tmp_zz500weight[i] = np.array(zz500weight) / 100.0
            tmp_hs300weight[i] = np.array(hs300weight) / 100.0

        if zz500weight_original.size == 0:
            sio.savemat(DATAPATH+'ZZ500_weight.mat', mdict={'ZZ500_weight':tmp_zz500weight})
            sio.savemat(DATAPATH+'HS300_weight.mat', mdict={'HS300_weight':tmp_hs300weight})
        else:
            zz500weight_original = np.vstack((zz500weight_original, tmp_zz500weight))
            hs300weight_original = np.vstack((hs300weight_original, tmp_hs300weight))
            sio.savemat(DATAPATH+'ZZ500_weight.mat', mdict={'ZZ500_weight':zz500weight_original})
            sio.savemat(DATAPATH+'HS300_weight.mat', mdict={'HS300_weight':hs300weight_original})

    def __write2sz50weight_file(self, datelist, sz50weight_original, stklist):
        """把50权重因子写入文件
        :datelist: 时间序列
        :sz50weight_original: 50权重因子原始数据
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_sz50weight = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_con_windcode,weight FROM AIndexSSE50Weight WHERE  TRADE_DT =%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_con_windcode = ret[0]
            weight = ret[1]
            sz50weight = list()
            for k in range(len(stklist)):
                if stklist[k] in s_con_windcode:
                    idx = s_con_windcode.index(stklist[k]) 
                    sz50weight.append(np.nan if weight[idx] == None else weight[idx])
                else:
                    sz50weight.append(0)
            tmp_sz50weight[i] = np.array(sz50weight) / 100.0

        if sz50weight_original.size == 0:
            sio.savemat(DATAPATH+'SZ50_weight.mat', mdict={'SZ50_weight':tmp_sz50weight})
        else:
            sz50weight_original = np.vstack((sz50weight_original, tmp_sz50weight))
            sio.savemat(DATAPATH+'SZ50_weight.mat', mdict={'SZ50_weight':sz50weight_original})
        
    def __write2listdate_file(self, stklist):
        """把上市时间因子写入文件
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_listdate = np.zeros((1,len(stklist)))

        sql = 'select s_info_windcode,s_info_listdate from  AShareDescription'
        cursor.execute(sql)
        rs = cursor.fetchall()
        ret = self.__convert_dbdata2tuplelist(rs, 2)
        s_info_windcode = ret[0]
        s_info_listdate = ret[1]
        listdate = list() 
        for k in range(len(stklist)):
            if stklist[k] in s_info_windcode:
                idx = s_info_windcode.index(stklist[k]) 
                listdate.append(np.nan if s_info_listdate[idx] == None else s_info_listdate[idx])
            else:
                listdate.append(np.nan)
        tmp_listdate[0] = np.array(listdate)
        sio.savemat(DATAPATH+'listdate.mat', mdict={'listdate': tmp_listdate})

    def __write2ind29_file(self, datelist, ind29_original, indlist):
        """把29个中信行业数据写入文件
        :datelist: 时间序列
        :ind29_original: 29个行业原始数据
        :indlist: 29个中信行业列表
        """
        cursor = self.conn.cursor()
        tmp_indcitic_29 = np.zeros((len(datelist), len(indlist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,S_DQ_CLOSE from AIndexIndustriesEODCITICS    WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            S_DQ_CLOSE = ret[1]
            indcitic29 = list()
            for k in range(len(indlist)):
                if indlist[k] in s_info_windcode:
                    idx = s_info_windcode.index(indlist[k]) 
                    indcitic29.append(np.nan if S_DQ_CLOSE[idx] == None else S_DQ_CLOSE[idx])
                else:
                    indcitic29.append(np.nan)
            tmp_indcitic_29[i] = np.array(indcitic29)

        if ind29_original.size == 0:
            sio.savemat(DATAPATH+'indIndex_CITIC_29.mat', mdict={'indIndex_CITIC_29':tmp_indcitic_29})
        else:
            ind29_original = np.vstack((ind29_original, tmp_indcitic_29))
            sio.savemat(DATAPATH+'indIndex_CITIC_29.mat', mdict={'indIndex_CITIC_29':ind29_original})
        
    def __write2totalmv_file(self, datelist, totalmv_original, stklist):
        """把个股市值因子写入文件
        :datelist: 时间序列
        :totalmv_original: 市值的原始因子
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_totalmv = np.zeros((len(datelist),len(stklist)))

        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,S_VAL_MV from  AShareEODDerivativeIndicator   WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            s_val_mv = ret[1]
            totalmv = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k]) 
                    totalmv.append(np.nan if s_val_mv[idx] == None else s_val_mv[idx])
                else:
                    totalmv.append(np.nan)
            tmp_totalmv[i] = np.array(totalmv)

        if totalmv_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/TotalMV.mat', mdict={'TotalMV':tmp_totalmv})
        else:
            totalmv_original = np.vstack((totalmv_original, tmp_totalmv))
            sio.savemat(DATAPATH+'daily_factor/TotalMV.mat', mdict={'TotalMV':totalmv_original})

    def __write2epfwd_file(self, datelist, epfwd_original, stklist):
        """把epfwd因子写入文件
        :datelist: 时间序列
        :epfwd_original: epfwd原始因子
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_epfwd = np.zeros((len(datelist),len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = "select s_info_windcode,est_pe from AShareConsensusRollingData where est_dt=%s and rolling_type='FTTM'" % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_windcode = ret[0]
            est_pe = ret[1]
            epfwd = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_windcode:
                    idx = s_info_windcode.index(stklist[k])
                    epfwd.append(np.nan if est_pe[idx] == None else est_pe[idx])
                else:
                    epfwd.append(np.nan)
            tmp_epfwd[i] = 1.0 / np.array(epfwd)

        if epfwd_original.size == 0:
            sio.savemat(DATAPATH+'newriskfactor/BarraSmallRisk/EPFWD.mat', mdict={'EPFWD':tmp_epfwd})
        else:
            epfwd_original = np.vstack((epfwd_original, tmp_epfwd))
            sio.savemat(DATAPATH+'newriskfactor/BarraSmallRisk/EPFWD.mat', mdict={'EPFWD':epfwd_original})
    
    def __write2dividend12m_file(self, datelist, dividend12m_original, stklist):
        """把新股息率因子写入文件
        :datelist: 时间序列
        :dividend12m_original: 新股息率的原始因子数据
        :stklist: 股票列表
        """
        cursor = self.conn.cursor()
        tmp_dividend12m = np.zeros((len(datelist), len(stklist)))
        for i in range(len(datelist)):
            date = datetime.datetime.strptime(datelist[i], '%Y/%m/%d')
            start_date = str(date.year - 2) + '1231'
            end_date = date.strftime('%Y%m%d')
            sql = "select a.WIND_CODE,a.CASH_DVD_PER_SH_PRE_TAX,a.S_DIV_BASESHARE,a.REPORT_PERIOD from AShareDividend a,(select WIND_CODE,max(REPORT_PERIOD) as REPORT_PERIOD from AShareDividend where REPORT_PERIOD >= %s and DVD_PAYOUT_DT <= %s group by wind_code) b where a.wind_code = b.wind_code and cast(a.REPORT_PERIOD as int) > cast(b.REPORT_PERIOD as int) - 10000 and cast(a.REPORT_PERIOD as int) <= cast(b.REPORT_PERIOD as int) order by wind_code" % (start_date, end_date)
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 4)
            wind_code = ret[0]
            cash_dvd = ret[1]
            s_div_baseshare = ret[2]
            report_period = ret[3]
            dividend = np.array(cash_dvd) * np.array(s_div_baseshare) * 10000
            df = DataFrame({'code':wind_code, 'value':dividend})
            dfsum = df.groupby(by=['code'])['value'].sum()
            wind_code = list(dfsum.index) 
            dividend = dfsum.values
            dividend12m = list()
            for k in range(len(stklist)):
                if stklist[k] in wind_code:
                    idx = wind_code.index(stklist[k])
                    dividend12m.append(np.nan if dividend[idx] == None else dividend[idx])
                else:
                    dividend12m.append(np.nan)
            tmp_dividend12m[i] = np.array(dividend12m)

        if dividend12m_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/dividend12M.mat', mdict={'dividend12M':tmp_dividend12m})
        else:
            dividend12m_original = np.vstack((dividend12m_original, tmp_dividend12m))
            sio.savemat(DATAPATH+'daily_factor/dividend12M.mat', mdict={'dividend12M':dividend12m_original})
         
    def __write2tmv_file(self, datelist, tmv_original, stklist):
        """把总市值因子写入文件
        :datelist:
        :tmv_original:
        :stklist:
        """
        cursor = self.conn.cursor()
        tmp_tmv = np.zeros((len(datelist), len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = "select s_info_windcode,S_VAL_MV from  AShareEODDerivativeIndicator  WHERE TRADE_DT=%s" % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
            s_info_wind_code = ret[0]
            s_val_mv = ret[1]
            tmv = list()
            for k in range(len(stklist)):
                if stklist[k] in s_info_wind_code:
                    idx = s_info_wind_code.index(stklist[k])
                    tmv.append(np.nan if s_val_mv[idx] == None else s_val_mv[idx] * 10000)
                else:
                    tmv.append(np.nan)
            tmp_tmv[i] = np.array(tmv)
        if tmv_original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/TMV.mat', mdict={'TMV':tmp_tmv})
        else:
            tmv_original = np.vstack((tmv_original, tmp_tmv))
            sio.savemat(DATAPATH+'daily_factor/TMV.mat', mdict={'TMV':tmv_original})
        
         
