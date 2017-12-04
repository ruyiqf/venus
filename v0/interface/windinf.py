#coding:GBK
import os
import sys
import time
import scipy.io as sio
import pandas as pd
import numpy as np
import json
from pandas import Series, DataFrame
from WindPy import *

import datetime

from ..loghandler import DefaultLogHandler

DATAPATH = './data/' #存储matlab的数据目录

class WindPyInf(object):
    """Wind数据库提取数据接口用来提取
    """
    def __init__(self, conf):
        """Wind接口的构造函数
        :conf:外层函数需要传递配置文件
        """
        self.log = DefaultLogHandler(name=__name__,filepath='./log/wind.log',log_type='file')
        w.start()
        self.start_day = '2005-01-01'
        with open(conf,'r') as f:
            self.conf = json.load(f)
        self.tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
    
    def wind_download_trading_days(self):
        """提取交易日交易周从2005-01-01提取到现在的交易时间
        """
        dnow = datetime.datetime.now()
        dnowstr = dnow.strftime('%Y-%m-%d')

        #判断交易日期文件是否存在采用增量的方式获取数据
        if(os.path.exists(DATAPATH+'tdays_data.mat')):
            tdays_dict = sio.loadmat(DATAPATH+'tdays_data.mat')
            lastest = datetime.datetime.strptime(tdays_dict['tdays_data'][-1][0][0], '%Y/%m/%d')
            if(lastest >= datetime.datetime.strptime(dnowstr, '%Y-%m-%d')):
                self.log.info('发现时间虫洞检查时间序列%s,%s'%(dnowstr, lastest.strftime('%Y-%m-%d')))
                return
            else:
                start_date = (lastest+datetime.timedelta(days=1)).strftime('%Y-%m-%d')
                original_array = tdays_dict['tdays_data']
                self.__write2tdays_file(start_date, dnowstr, original_array)
        else:
            start_date = self.start_day
            original_array = np.array([])
            self.__write2tdays_file(start_date, dnowstr, original_array)

        #判断交易周期文件是否存在采用增量的方式获取数据
        if(os.path.exists(DATAPATH+'tdays_data_week.mat')):
            tdays_dict = sio.loadmat(DATAPATH+'tdays_data_week.mat')
            lastest = datetime.datetime.strptime(tdays_dict['tdays_data_week'][-1][0][0], '%Y/%m/%d')
            if(lastest >= datetime.datetime.strptime(dnowstr, '%Y-%m-%d')):
                self.log.info('发现时间虫洞检查时间序列%s,%s'%(dnowstr, lastest.strftime('%Y-%m-%d')))
            else:
                start_date = lastest.strftime('%Y-%m-%d')
                original_array = tdays_dict['tdays_data_week']
                self.__write2tweeks_file(start_date, dnowstr, original_array)
        else:
            start_date = self.start_day
            original_array = np.array([])
            self.__write2tweeks_file(start_date, dnowstr, original_array)

    def wind_download_market_revenue_ratio(self):
        """提取交易日指数收益率
        """
        tdays_data = self.tdays_data
        if(os.path.exists(DATAPATH+'Ind_daily.mat')):
            ind_pct_chg = sio.loadmat(DATAPATH+'Ind_daily.mat')['ind_pct_chg']
            if len(ind_pct_chg) < len(tdays_data):
                self.__write2inddaily_file(tdays_data[len(ind_pct_chg):], ind_pct_chg) 
            else:
                self.log.info('交易日指数已经更新')
        else:
            self.__write2inddaily_file(tdays_data, np.array([]))

    def wind_download_wholea_revenue_ratio(self):
        """提取市场全A指数收益率
        """
        tdays_data = self.tdays_data
        if(os.path.exists(DATAPATH+'market.mat')):
            market = sio.loadmat(DATAPATH+'market.mat')['market']
            if len(market) < len(tdays_data):
                self.__write2market_file(tdays_data[len(market):], market)
            else:
                self.info('全A指数已经更新到最新')
        else:
            self.__write2market_file(tdays, np.array([]))

    def wind_download_stkind(self):
        """提取股票的行业属性
        """
        tdays_data = self.tdays_data 
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'ind_of_stock.mat')):
            ind_of_stock = sio.loadmat(DATAPATH+'ind_of_stock.mat')['ind_of_stock']
            if len(ind_of_stock) < len(tdays_data):
                ind_of_stock = self.__align_column(ind_of_stock, stklist)
                self.__write2indstock_file(tdays_data[len(ind_of_stock):len(tdays_data)],
                    ind_of_stock, stklist)
            else:
                self.log.info('行业属性已经更新到最新')
        else:
            ind_of_stock = np.array([])
            self.__write2indstock_file(tdays_data, ind_of_stock, stklist)

    def wind_download_sz50_ratio(self):
        """提取上证50的日收益率数据
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        if(os.path.exists(DATAPATH+'SZ50_daily_ret.mat')):
            SZ50_daily_ret = sio.loadmat(DATAPATH+'SZ50_daily_ret.mat')['SZ50_daily_ret']
            if len(SZ50_daily_ret) < len(tdays_data):
                self.__write2sz50daily_file(tdays_data[len(SZ50_daily_ret):len(tdays_data)], SZ50_daily_ret)
            else:
                self.log.info('SZ50已经更新到最新')
        else:
            self.__write2sz50daily_file(tdays_data, np.array([]))

    def wind_download_zz500_ratio(self):
        """提取中证500的日收益率数据
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        if(os.path.exists(DATAPATH+'ZZ500_daily_ret.mat')):
            ZZ500_daily_ret = sio.loadmat(DATAPATH+'ZZ500_daily_ret.mat')['ZZ500_daily_ret']
            if len(ZZ500_daily_ret) < len(tdays_data):
                self.__write2zz500daily_file(tdays_data[len(ZZ500_daily_ret):len(tdays_data)], ZZ500_daily_ret)
            else:
                self.log.info('ZZ500已经更新到最新')
        else:
            self.__write2zz500daily_file(tdays_data, np.array([]))

        if(os.path.exists(DATAPATH+'ZZ500_overnight_ret.mat')):
            HS300_overnight_ret = sio.loadmat(DATAPATH+'ZZ500_overnight_ret.mat')['ZZ500_overnight_ret']
            if len(HS300_overnight_ret) < len(tdays_data):
                self.__write2zz500overnight_file(tdays_data[len(ZZ500_overnight_ret):],
                    ZZ500_overnight_ret)
            else:
                self.log.info('ZZ500隔夜收益已经更新到最新')
        else:
            self.__write2zz500overnight_file(tdays_data, np.array([]))

        if(os.path.exists(DATAPATH+'ZZ500.mat')):
            ZZ500 = sio.loadmat(DATAPATH+'ZZ500.mat')['ZZ500']
            if len(ZZ500) < len(tdays_data):
                self.__write2zz500_file(tdays_data[len(ZZ500):],
                    ZZ500)
            else:
                self.log.info('ZZ500已经更新到最新')
        else:
            self.__write2zz500_file(tdays_data, np.array([]))
         
    def wind_download_hs300_ratio(self):
        """提取沪深300的日收益率数据
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        if(os.path.exists(DATAPATH+'HS300_daily_ret.mat')):
            HS300_daily_ret = sio.loadmat(DATAPATH+'HS300_daily_ret.mat')['HS300_daily_ret']
            if len(HS300_daily_ret) < len(tdays_data):
                self.__write2hs300daily_file(tdays_data[len(HS300_daily_ret):len(tdays_data)], HS300_daily_ret)
            else:
                self.log.info('HS300已经更新到最新')
        else:
            self.__write2hs300daily_file(tdays_data, np.array([]))

        if(os.path.exists(DATAPATH+'HS300_overnight_ret.mat')):
            HS300_overnight_ret = sio.loadmat(DATAPATH+'HS300_overnight_ret.mat')['HS300_overnight_ret']
            if len(HS300_overnight_ret) < len(tdays_data):
                self.__write2hs300overnight_file(tdays_data[len(HS300_overnight_ret):],
                    HS300_overnight_ret)
            else:
                self.log.info('HS300隔夜收益已经更新到最新')
        else:
            self.__write2hs300overnight_file(tdays_data, np.array([]))
        
        if(os.path.exists(DATAPATH+'HS300.mat')):
            HS300 = sio.loadmat(DATAPATH+'HS300.mat')['HS300']
            if len(HS300) < len(tdays_data):
                self.__write2hs300_file(tdays_data[len(HS300):],
                    HS300)
            else:
                self.log.info('HS300已经更新到最新')
        else:
            self.__write2hs300_file(tdays_data, np.array([]))

    def wind_download_h00016_ratio(self):
        """提权H00016.SH指数收益率
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        if(os.path.exists(DATAPATH+'SZ50_all.mat')):
            SZ50_all = sio.loadmat(DATAPATH+'SZ50_all.mat')['SZ50_all']
            if len(SZ50_all) < len(tdays_data):
                self.__write2sz50all_file(tdays_data[len(SZ50_all):len(tdays_data)], SZ50_all)
            else:
                self.log.info('SZ50_all已经更新到最新')
        else:
            self.__write2sz50all_file(tdays_data, np.array([]))
        
        if(os.path.exists(DATAPATH+'SZ50_all_daily_ret.mat')):
            SZ50_all_daily_ret = sio.loadmat(DATAPATH+'SZ50_all_daily_ret.mat')['SZ50_all_daily_ret']
            if len(SZ50_all_daily_ret) < len(tdays_data):
                self.__write2sz50alldailyret_file(tdays_data[len(SZ50_all_daily_ret):len(tdays_data)], SZ50_all_daily_ret)
            else:
                self.log.info('SZ50_all_daily_ret已经更新到最新')
        else:
            self.__write2sz50alldailyret_file(tdays_data, np.array([]))
        
    def wind_download_h00030_ratio(self):
        """提权H00030.CSI指数收益率
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        if(os.path.exists(DATAPATH+'HS300_all.mat')):
            HS300_all = sio.loadmat(DATAPATH+'HS300_all.mat')['HS300_all']
            if len(HS300_all) < len(tdays_data):
                self.__write2hs300all_file(tdays_data[len(HS300_all):len(tdays_data)], HS300_all)
            else:
                self.log.info('HS300_all已经更新到最新')
        else:
            self.__write2hs300all_file(tdays_data, np.array([]))
        
        if(os.path.exists(DATAPATH+'HS300_all_daily_ret.mat')):
            HS300_all_daily_ret = sio.loadmat(DATAPATH+'HS300_all_daily_ret.mat')['HS300_all_daily_ret']
            if len(HS300_all_daily_ret) < len(tdays_data):
                self.__write2hs300alldailyret_file(tdays_data[len(HS300_all_daily_ret):len(tdays_data)], HS300_all_daily_ret)
            else:
                self.log.info('HS300_all_daily_ret已经更新到最新')
        else:
            self.__write2hs300alldailyret_file(tdays_data, np.array([]))

    def wind_download_h00905_ratio(self):
        """提权H00905.CSI指数收益率
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        if(os.path.exists(DATAPATH+'ZZ500_all.mat')):
            ZZ500_all = sio.loadmat(DATAPATH+'ZZ500_all.mat')['ZZ500_all']
            if len(ZZ500_all) < len(tdays_data):
                self.__write2zz500all_file(tdays_data[len(ZZ500_all):len(tdays_data)], ZZ500_all)
            else:
                self.log.info('ZZ500_all已经更新到最新')
        else:
            self.__write2zz500all_file(tdays_data, np.array([]))
        
        if(os.path.exists(DATAPATH+'ZZ500_all_daily_ret.mat')):
            HS300_all_daily_ret = sio.loadmat(DATAPATH+'ZZ500_all_daily_ret.mat')['ZZ500_all_daily_ret']
            if len(ZZ500_all_daily_ret) < len(tdays_data):
                self.__write2zz500alldailyret_file(tdays_data[len(ZZ500_all_daily_ret):len(tdays_data)], ZZ500_all_daily_ret)
            else:
                self.log.info('ZZ500_all_daily_ret已经更新到最新')
        else:
            self.__write2zz500alldailyret_file(tdays_data, np.array([]))

    def wind_download_market_value(self):
        """提取市值相关因子
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/MV.mat')):
            mv = sio.loadmat(DATAPATH+'daily_factor/MV.mat')['MV']
            if(len(mv) < len(tdays_data)):
                mv = self.__align_column(mv, stklist)
                self.__write2marketvalue_file(tdays_data[len(mv):], mv, stklist)
            else:
                self.log.info('市值因子已经更新到最新')
        else:
            self.__write2marketvalue_file(tdays_data, np.array([]), stklist)

    def wind_download_profit_pred_value(self):
        """提取预测净利润因子数据
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/profit_pred_4w.mat')):
            profit_pred_4w = sio.loadmat(DATAPATH+'daily_factor/profit_pred_4w.mat')['profit_pred_4w']
            if(len(profit_pred_4w) < len(tdays_data)):
                profit_pred_4w = self.__align_column(profit_pred_4w, stklist)
                self.__write2profitpred4w_file(tdays_data[len(profit_pred_4w):], profit_pred_4w, stklist)
            else:
                self.log.info('预测净利润因子已经更新到最新')
        else:
            self.__write2profitpred4w_file(tdays_data, np.array([]), stklist)

    def __convert_row2column(self, np_array):
        """把行向量转置成列向量
        :np_array:行向量
        """
        return np_array.reshape(len(np_array), 1)

    def __write2tdays_file(self, start_date, end_date, original):
        """把交易日期通过增量的方式写入Matlab文件
        :start_date: 起始日期
        :end_date: 结束日期
        :original: 原来下载的时间序列
        """
        tdays = w.tdays(start_date, end_date)
        tdays_array = np.array([elt.strftime('%Y/%m/%d') for elt in tdays.Data[0]], dtype=np.object)
        tdays_array = self.__convert_row2column(tdays_array)
        if original.size > 0:
            tdays_array = np.concatenate([original, tdays_array], axis=0) 
        sio.savemat(DATAPATH+'tdays_data', mdict={'tdays_data':tdays_array})
        self.tdays_array = tdays_array

    def __write2tweeks_file(self, start_date, end_date, original):
        """把交易周通过增量的方式写入Matlab文件
        :start_date: 起始日期
        :end_date: 结束日期
        :original: 原来下载的周期序列
        """
        tweeks = w.tdays(start_date, end_date, 'Period=W')
        tweeks_array = np.array([elt.strftime('%Y/%m/%d') for elt in tweeks.Data[0]], dtype=np.object)
        tweeks_array = self.__convert_row2column(tweeks_array)
        if original.size > 0:
            tweeks_array = np.concatenate([original, tweeks_array], axis=0)
        sio.savemat(DATAPATH+'tdays_data_week', mdict={'tdays_data_week':tweeks_array})
        self.tweeks_array = tweeks_array 

    def __write2inddaily_file(self, datelist, original):
        """根据配置文件获取市场指数收益率
        :datelist: 时间序列 
        :original: 原有的数据
        """
        start_date = datetime.datetime.strptime(datelist[0], '%Y/%m/%d').strftime('%Y%m%d')
        end_date = datetime.datetime.strptime(datelist[-1], '%Y/%m/%d').strftime('%Y%m%d')
        data = w.wsd(self.conf['index_list'], 'pct_chg',
                start_date, end_date,
                'Fill=Previous', 'PriceAdj=F')
        pct_chg = np.zeros((len(datelist), len(data.Codes)))
        ind_name = np.array(w.wsd(self.conf['index_list'], 'sec_name').Data[0])
        ind_code = np.array(w.wsd(self.conf['index_list'], 'trade_code').Data[0])

        if len(datelist) > 1:
            #按照列排列
            for i in range(len(data.Codes)):
                pct_chg[:,i] = np.array(data.Data[i]) / 100.0 

        if len(datelist) == 1:
            pct_chg[0] = np.array(data.Data[0]) / 100.0
            
        if original.size == 0 :
            sio.savemat(DATAPATH+'Ind_daily', mdict={'ind_pct_chg': pct_chg})
            original = np.zeros(np.shape(pct_chg))
            original = pct_chg
        else:
            original = np.vstack((original, pct_chg))
            sio.savemat(DATAPATH+'Ind_daily', mdict={'ind_pct_chg': original})
        self.log.info(ind_name)
        self.log.info(ind_code)

        #计算行业近10天的收益率的动量这里重新计算
        ind_momentum = np.zeros(np.shape(original))
        for i in range(10, len(original)):
            ind_momentum[i] = np.prod(original[i-10:i]+1, axis=0) - 1
        sio.savemat(DATAPATH+'ind_momentum', mdict={'ind_momentum':ind_momentum})
        
    def __write2market_file(self, datelist, original):
        """获取全A指数收益率
        :datelist: 时间序列
        :original: 原有全A数据
        """
        start_date = datetime.datetime.strptime(datelist[0], '%Y/%m/%d').strftime('%Y%m%d')
        end_date = datetime.datetime.strptime(datelist[-1], '%Y/%m/%d').strftime('%Y%m%d')
        market = self.__convert_row2column(np.array(w.wsd(self.conf['whole_a'], 'pct_chg',
                                            start_date, end_date,
                                            'Fill=Previous', 'PriceAdj=F').Data[0]) / 100.0)
        if original.size == 0:
            sio.savemat(DATAPATH+'market', mdict={'market':market})
        else:
            original = np.vstack((original, market))
            sio.savemat(DATAPATH+'market', mdict={'market':original})

    def __write2indstock_file(self, datelist, original, stklist):
        """获取股票的行业属性
        :datelist: 时间序列区间
        :original: 原有行业属性列表
        :stklist: 股票列表函数
        """
        new_ind_name = self.__convert_mat2list(sio.loadmat(DATAPATH+'ind_name.mat')['ind_name'])
        tmp_array = np.zeros(len(stklist))
        for i in range(len(datelist)):
            ind_list = list()
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            data1 = w.wss(','.join(stklist), 'industry2', 'industryType=1;industryStandard=1;tradeDate=%s'%sdate).Data[0]
            data2 = w.wss(','.join(stklist), 'industry2', 'industryType=1;industryStandard=2;tradeDate=%s'%sdate).Data[0]
            for i in range(len(data1)):
                if (data1[i] != None) and (data1[i] in new_ind_name):
                    ind_list.append(new_ind_name.index(data1[i]))
                elif (data2[i] != None) and (data2[i] in new_ind_name):
                    ind_list.append(new_ind_name.index(data2[i]))
                else:
                    ind_list.append(0)
            try:
                tmp_array = np.vstack((tmp_array, np.array(ind_list)))
            except ValueError as e:
                print(data1)
                print('We find a dimension mismatch error')

        tmp_array = np.delete(tmp_array, [0], axis=0)
        if(original.size == 0):
            sio.savemat(DATAPATH+'ind_of_stock', mdict={'ind_of_stock':tmp_array})
        else:
            original = np.vstack((original, tmp_array))
            sio.savemat(DATAPATH+'ind_of_stock', mdict={'ind_of_stock':original})
        
    def __convert_mat2list(self, mat_ndarray):
        """把mat的字符高维数据转换成list类型
        :mat_ndarray: mat数据格式
        """
        try:
            return [elt[0][0] for elt in mat_ndarray]
        except IndexError as e:
            self.log.error('Maybe is data need descendent dimension')
            return [elt[0] for elt in mat_ndarray]
    
    def __convert_time_format(self, datestr):
        """把时间格式%Y/%m/%d转化为%Y%m%d
        """
        return datetime.datetime.strptime(datestr, '%Y/%m/%d').strftime('%Y%m%d')
    
    def __write2sz50daily_file(self, datelist, original):
        """把上证50的日收益率写入mat文件
        :datelist: 更新日期区间
        :original: 原始数据
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('000016.SH','pct_chg',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0]) / 100.0
        data = self.__convert_row2column(data) 
        if original.size == 0:
            sio.savemat(DATAPATH+'SZ50_daily_ret.mat', mdict={'SZ50_daily_ret':data})
        else:
            original = np.vstack((original, data))
            sio.savemat(DATAPATH+'SZ50_daily_ret.mat', mdict={'SZ50_daily_ret':original})

    def __write2zz500daily_file(self, datelist, original):
        """把中证500的日收益率写入mat文件
        :datelist: 更新日期区间
        :original: 原始数据
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('000905.SH','pct_chg',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0]) / 100.0
        data = self.__convert_row2column(data) 
        if original.size == 0:
            sio.savemat(DATAPATH+'ZZ500_daily_ret.mat', mdict={'ZZ500_daily_ret':data})
        else:
            original = np.vstack((original, data))
            sio.savemat(DATAPATH+'ZZ500_daily_ret.mat', mdict={'ZZ500_daily_ret':original})

    def __write2hs300daily_file(self, datelist, original):
        """把沪深300的日收益率写入mat文件
        :datelist: 更新日期区间
        :original: 原始数据
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('000300.SH','pct_chg',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0]) / 100.0
        data = self.__convert_row2column(data) 
        if original.size == 0:
            sio.savemat(DATAPATH+'HS300_daily_ret.mat', mdict={'HS300_daily_ret':data})
        else:
            original = np.vstack((original, data))
            sio.savemat(DATAPATH+'HS300_daily_ret.mat', mdict={'HS300_daily_ret':original})

    def __write2hs300overnight_file(self, datelist, original):
        """把沪深300的隔日收益率写入mat文件
        :datelist: 更新日期区间
        :original: 原始数据
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = w.wsd('000300.SH','pre_close,open',start_date,end_date,'Fill=Previous','PriceAdj=F').Data
        data = np.array(data[1]) / np.array(data[0]) - 1
        data = self.__convert_row2column(data) 
        if original.size == 0:
            sio.savemat(DATAPATH+'HS300_overnight_ret.mat', mdict={'HS300_overnight_ret':data})
        else:
            original = np.vstack((original, data))
            sio.savemat(DATAPATH+'HS300_overnight_ret.mat', mdict={'HS300_overnight_ret':original})

    def __write2zz500overnight_file(self, datelist, original):
        """把中证500的隔日收益率写入mat文件
        :datelist: 更新日期区间
        :original: 原始数据
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = w.wsd('000905.SH','pre_close,open',start_date,end_date,'Fill=Previous','PriceAdj=F').Data
        data = np.array(data[1]) / np.array(data[0]) - 1
        data = self.__convert_row2column(data) 
        if original.size == 0:
            sio.savemat(DATAPATH+'ZZ500_overnight_ret.mat', mdict={'ZZ500_overnight_ret':data})
        else:
            original = np.vstack((original, data))
            sio.savemat(DATAPATH+'ZZ500_overnight_ret.mat', mdict={'ZZ500_overnight_ret':original})

    def __write2hs300_file(self, datelist, original):
        """把沪深300写入mat文件
        :datelist: 更新日期区间
        :original: 原始数据
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('000300.SH','close',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0])
        data = self.__convert_row2column(data)

        if original.size == 0:
            sio.savemat(DATAPATH+'HS300.mat', mdict={'HS300':data})
        else:
            original = np.vstack((original, data))
            sio.savemat(DATAPATH+'HS300.mat', mdict={'HS300':original})
        
    def __write2zz500_file(self, datelist, original):
        """把中证500写入mat文件
        :datelist: 更新日期区间
        :original: 原始数据
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('000905.SH','close',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0])
        data = self.__convert_row2column(data)

        if original.size == 0:
            sio.savemat(DATAPATH+'ZZ500.mat', mdict={'ZZ500':data})
        else:
            original = np.vstack((original, data))
            sio.savemat(DATAPATH+'ZZ500.mat', mdict={'ZZ500':original})

    def __write2marketvalue_file(self, datelist, original, stklist):
        """把市值因子写入文件
        :datelist: 更新时间序列
        :original: 原始市值数据
        :stklist: 股票列表
        """
        tmp_marketvalue = np.zeros((len(datelist), len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            data = w.wss(','.join(stklist), 'mkt_cap_ashare', 'tradeDate=%s'%sdate).Data[0]
            tmp_marketvalue[i] = np.array(data)
        if original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/MV.mat', mdict={'MV':tmp_marketvalue})
        else:
            original = np.vstack((original, tmp_marketvalue))
            sio.savemat(DATAPATH+'daily_factor/MV.mat', mdict={'MV':original})

    def __align_column(self, original, target):
        """判断是否需要补足数据的列宽
        :original: 原始数据
        :target: 目标数据
        """
        if len(original) < len(target):
            colnum = np.shape(original)[1]
            original = self.__compensate_original_column(original, len(target) - colnum)
        return original

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

    def __write2profitpred4w_file(self, datelist, original, stklist):
        """把预测盈利因子写入文件
        :datelist: 更新时间序列
        :original: 原始预测盈利因子
        :stklist: 股票列表
        """
        tmp_profitpred4w = np.zeros((len(datelist), len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            syear = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y')
            data = w.wss(','.join(stklist), 'west_nproc_4w', 'tradeDate=%s;year=%s'%(sdate, syear)).Data[0]
            tmp_profitpred4w[i] = np.array(data)
        if original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/profit_pred_4w.mat', mdict={'profit_pred_4w':tmp_profitpred4w})
        else:
            original = np.vstack((original, tmp_profitpred4w))
            sio.savemat(DATAPATH+'daily_factor/profit_pred_4w.mat', mdict={'profit_pred_4w':original})

    def __write2sz50all_file(self, datelist, sz50_all_original):
        """把H00016指数写入文件
        :datelist:
        :sz50_all_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00016.SH','close',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0])
        data = self.__convert_row2column(data)
        if sz50_all_original.size == 0:
            sio.savemat(DATAPATH+'SZ50_all.mat', mdict={'SZ50_all':data})
        else:
            sz50_all_original = np.vstack((original, data))
            sio.savemat(DATAPATH+'SZ50_all.mat', mdict={'SZ50_all':sz50_all_original})

    def __write2sz50alldailyret_file(self, datelist, sz50_all_daily_ret_original):
        """把H00016指数收益率写入文件
        :datelist:
        :sz50_all_daily_ret_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00016.SH','pct_chg',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0]) / 100.0
        data = self.__convert_row2column(data) 
        if sz50_all_daily_ret_original.size == 0:
            sio.savemat(DATAPATH+'SZ50_all_daily_ret.mat', mdict={'SZ50_all_daily_ret':data})
        else:
            sz50_all_daily_ret_original = np.vstack((sz50_all_daily_ret_original, data))
            sio.savemat(DATAPATH+'SZ50_all_daily_ret.mat', mdict={'SZ50_all_daily_ret':sz50_all_daily_ret_original})
        
    def __write2sz50all_file(self, datelist, sz50_all_original):
        """把H00016指数写入文件
        :datelist:
        :sz50_all_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00016.SH','close',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0])
        data = self.__convert_row2column(data)
        if sz50_all_original.size == 0:
            sio.savemat(DATAPATH+'SZ50_all.mat', mdict={'SZ50_all':data})
        else:
            sz50_all_original = np.vstack((original, data))
            sio.savemat(DATAPATH+'SZ50_all.mat', mdict={'SZ50_all':sz50_all_original})

    def __write2sz50alldailyret_file(self, datelist, sz50_all_daily_ret_original):
        """把H00016指数收益率写入文件
        :datelist:
        :sz50_all_daily_ret_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00016.SH','pct_chg',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0]) / 100.0
        data = self.__convert_row2column(data) 
        if sz50_all_daily_ret_original.size == 0:
            sio.savemat(DATAPATH+'SZ50_all_daily_ret.mat', mdict={'SZ50_all_daily_ret':data})
        else:
            sz50_all_daily_ret_original = np.vstack((sz50_all_daily_ret_original, data))
            sio.savemat(DATAPATH+'SZ50_all_daily_ret.mat', mdict={'SZ50_all_daily_ret':sz50_all_daily_ret_original})

    def __write2hs300_file(self, datelist, hs300_all_original):
        """把H00300指数写入文件
        :datelist:
        :hs300_all_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00300.CSI','close',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0])
        data = self.__convert_row2column(data)
        if hs300_all_original.size == 0:
            sio.savemat(DATAPATH+'HS300_all.mat', mdict={'HS300_all':data})
        else:
            hs300_all_original = np.vstack((hs300_all_original, data))
            sio.savemat(DATAPATH+'HS300_all.mat', mdict={'HS300_all':hs300_all_original})

    def __write2hs300alldailyret_file(self, datelist, hs300_all_daily_ret_original):
        """把H00300指数收益率写入文件
        :datelist:
        :hs300_all_daily_ret_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00300.CSI','pct_chg',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0]) / 100.0
        data = self.__convert_row2column(data) 
        if hs300_all_daily_ret_original.size == 0:
            sio.savemat(DATAPATH+'HS300_all_daily_ret.mat', mdict={'HS300_all_daily_ret':data})
        else:
            hs300_all_daily_ret_original = np.vstack((hs300_all_daily_ret_original, data))
            sio.savemat(DATAPATH+'HS300_all_daily_ret.mat', mdict={'HS300_all_daily_ret':hs300_all_daily_ret_original})

    def __write2zz500_file(self, datelist, zz500_all_original):
        """把H00905指数写入文件
        :datelist:
        :zz500_all_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00905.CSI','close',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0])
        data = self.__convert_row2column(data)
        if hs300_all_original.size == 0:
            sio.savemat(DATAPATH+'ZZ500_all.mat', mdict={'ZZ500_all':data})
        else:
            hs300_all_original = np.vstack((zz500_all_original, data))
            sio.savemat(DATAPATH+'ZZ500_all.mat', mdict={'ZZ500_all':zz500_all_original})

    def __write2zz500alldailyret_file(self, datelist, zz500_all_daily_ret_original):
        """把H00300指数收益率写入文件
        :datelist:
        :zz500_all_daily_ret_original:
        """
        start_date = self.__convert_time_format(datelist[0])
        end_date = self.__convert_time_format(datelist[-1])
        data = np.array(w.wsd('H00905.CSI','pct_chg',start_date,end_date,'Fill=Previous','PriceAdj=F').Data[0]) / 100.0
        data = self.__convert_row2column(data) 
        if zz500_all_daily_ret_original.size == 0:
            sio.savemat(DATAPATH+'ZZ500_all_daily_ret.mat', mdict={'ZZ500_all_daily_ret':data})
        else:
            zz500_all_daily_ret_original = np.vstack((zz500_all_daily_ret_original, data))
            sio.savemat(DATAPATH+'ZZ500_all_daily_ret.mat', mdict={'ZZ500_all_daily_ret':zz500_all_daily_ret_original})

    def __del__(self):
        w.stop()
        

