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
        self.update_date_area = tuple()
        self.stklist = None
    
    def wind_download_trading_days(self):
        """提取交易日交易周从2005-01-01提取到现在的交易时间
        """
        dnow = datetime.datetime.now()
        dnowstr = dnow.strftime('%Y-%m-%d')

        #判断交易日期文件是否存在采用增量的方式获取数据
        if(os.path.exists(DATAPATH+'tdays_data.mat')):
            tdays_dict = sio.loadmat(DATAPATH+'tdays_data.mat')
            lastest = datetime.datetime.strptime(tdays_dict['tdays_data'][-1][0][0], '%Y/%m/%d')
            if(lastest <= datetime.datetime.strptime(dnowstr, '%Y-%m-%d')):
                self.log.info('发现时间虫洞检查时间序列%s,%s'%(dnowstr, lastest.strftime('%Y-%m-%d')))
                return
            else:
                start_date = lastest.strftime('%Y-%m-%d')
                original_array = tdays_dict['tdays_data']
                self.__write2tdays_file(start_date, dnowstr, original_array)
        else:
            start_date = self.start_day
            original_array = np.array([])
            self.__write2tdays_file(start_date, dnowstr, original_array)

        #更新增量区间
        self.update_date_area = (start_date, dnowstr)

        #判断交易周期文件是否存在采用增量的方式获取数据
        if(os.path.exists(DATAPATH+'tdays_data_week.mat')):
            tdays_dict = sio.loadmat(DATAPATH+'tdays_data_week.mat')
            lastest = datetime.datetime.strptime(tdays_dict['tdays_data_week'][-1][0][0], '%Y/%m/%d')
            if(lastest <= datetime.datetime.strptime(dnowstr, '%Y-%m-%d')):
                self.log.info('发现时间虫洞检查时间序列%s,%s'%(dnowstr, lastest.strftime('%Y-%m-%d')))
            else:
                start_date = lastest.strftime('%Y-%m-%d')
                original_array = tdays_dict['tdays_data']
                self.__write2tweeks_file(start_date, dnowstr, original_array)
        else:
            start_date = self.start_day
            original_array = np.array([])
            self.__write2tweeks_file(start_date, dnowstr, original_array)

    def wind_download_market_revenue_ratio(self):
        """提取交易日指数收益率
        """
        if(os.path.exists(DATAPATH+'Ind_daily.mat')):
            ind_dict = sio.loadmat(DATAPATH+'Ind_daily.mat')
            start_date = self.update_date_area[0]
            end_date = self.update_date_area[1]
            self.__write2inddaily_file(start_date, end_date, ind_dict)
        else:
            start_date = self.start_day
            dnow = datetime.datetime.now()
            dnowstr = dnow.strftime('%Y-%m-%d')
            end_date = dnowstr
            ind_dict = dict()
            self.__write2inddaily_file(start_date, end_date, ind_dict)

    def wind_download_wholea_revenue_ratio(self):
        """提取市场全A指数收益率
        """
        if(os.path.exists(DATAPATH+'market.mat')):
            market_dict = sio.loadmat(DATAPATH+'market.mat')
            start_date = self.update_date_area[0]
            end_date = self.update_date_area[1]
            self.__write2market_file(start_date, end_date, market_dict)
        else:
            start_date = self.start_day
            dnow = datetime.datetime.now()
            dnowstr = dnow.strftime('%Y-%m-%d')
            end_date = dnowstr
            market_dict = dict()
            self.__write2market_file(start_date, end_date, market_dict)

    def wind_download_stkind(self):
        """提取股票的行业属性
        """
        if(os.path.exists(DATAPATH+'stock.mat')):
             tmpstklist = sio.loadmat(DATAPATH+'stock.mat')['stklist']
             self.stklist = [elt[0][0] for elt in tmpstklist]
        else:
            self.log.error('无法找到股票列表文件请先下载股票列表')
            return
        
        if(os.path.exists(DATAPATH+'tdays_data.mat')):
            tdays_data = sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data']
            tdays_data = [elt[0][0] for elt in tdays_data]
        else:
            self.log.error('无法找到时间序列文件')
        
        if(os.path.exists(DATAPATH+'ind_of_stock.mat')):
            ind_of_stock = sio.loadmat(DATAPATH+'ind_of_stock.mat')['ind_of_stock']
            if len(ind_of_stock) < len(tdays_data):
                self.__write2indstock_file(tdays_data[len(ind_of_stock):len(tdays_data)],
                    ind_of_stock, self.stklist)
            else:
                self.log.info('行业属性已经更新到最新')
        else:
            ind_of_stock = np.array([])
            self.__write2indstock_file(tdays_data, ind_of_stock, self.stklist)

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

    def wind_download_market_value(self):
        """提取市值相关因子
        """
        tdays_data = self.__convert_mat2list(sio.loadmat(DATAPATH+'tdays_data.mat')['tdays_data'])
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'daily_factor/MV.mat')):
            mv = sio.loadmat(DATAPATH+'daily_factor/MV.mat')['MV']
            if(len(mv) < len(tdays_data)):
                self.__write2marketvalue_file(tdays.data[len(mv):], mv, stklist)
            else:
                self.log.info('市值因子已经更新到最新')
        else:
            self.__write2marketvalue_file(tdays_data, np.array([]), stklist)

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

    def __write2inddaily_file(self, start_date, end_date, original):
        """根据配置文件获取市场指数收益率
        :start_date: 起始日期
        :end_date: 结束日期
        :original: 原有的数据字典dict类型
        """
        data = w.wsd(self.conf['index_list'], 'pct_chg',
                start_date, end_date,
                'Fill=Previous', 'PriceAdj=F')
        pct_chg = np.arange(len(data.Data[0])).reshape(len(data.Data[0]),1)
        ind_name = np.array(w.wsd(self.conf['index_list'], 'sec_name').Data[0])
        ind_code = np.array(w.wsd(self.conf['index_list'], 'trade_code').Data[0])
        
        for elt in data.Codes:
            idx = data.Codes.index(elt)
            pct_chg = np.column_stack((pct_chg, self.__convert_row2column(np.array(data.Data[idx])/100.0)))
        pct_chg = np.delete(pct_chg, [0], axis=1)
        if 'ind_pct_chg' not in original:
            original['ind_pct_chg'] = pct_chg
        else:
            orginal['ind_pct_chg'] = np.column_stack((orginal['ind_pct_chg'], pct_chg))
        self.log.info(ind_name)
        self.log.info(ind_code)
        sio.savemat(DATAPATH+'Ind_daily', mdict=original)

        #计算行业近10天的收益率的动量这里重新计算
        ind_momentum = np.arange(len(pct_chg[0]))
        for i in range(10, len(pct_chg)):
            ind_momentum = np.vstack((ind_momentum, np.prod(pct_chg[i-9:i]+1, axis=0) - 1))
        ind_momentum = np.delete(ind_momentum, [0], axis=0)
        sio.savemat(DATAPATH+'ind_momentum', mdict={'ind_momentum':ind_momentum})
        
    def __write2market_file(self, start_date, end_date, market_dict):
        """获取全A指数收益率
        :start_date: 起始日期
        :end_date: 结束日期
        :market_dict: 原有全A数据字典
        """
        market = self.__convert_row2column(np.array(w.wsd(self.conf['whole_a'], 'pct_chg',
                                            start_date, end_date,
                                            'Fill=Previous', 'PriceAdj=F').Data[0]) / 100.0)
        if 'market' not in market_dict:
            market_dict['market'] = market
        else:
            market_dict['market'] = np.column_stack((market_dict['market'], market))
        sio.savemat(DATAPATH+'market', mdict=market_dict)

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
            sio.savemat(DATAPATH+'new_ind_of_stock', mdict={'ind_of_stock':tmp_array})
        else:
            original = np.vstack((original, tmp_array))
            sio.savemat(DATAPATH+'new_ind_of_stock', mdict={'ind_of_stock':original})
        
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
        tmp_marketvalue = np.zeros((len(datelist), len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i][0][0], '%Y/%m/%d').strftime('%Y%m%d')
            data = w.wss(','.join(stklist), 'mkt_cap_ashare', 'tradeDate=%s'%sdate).Data[0]
            tmp_marketvalue[i] = np.array(data)
        if original.size == 0:
            sio.savemat(DATAPATH+'daily_factor/MV.mat', mdict={'MV':tmp_marketvalue})
        else:
            original = np.vstack((original, tmp_marketvalue))
            sio.savemat(DATAPATH+'daily_factor/MV.mat', mdict={'MV':original})
        
    def __del__(self):
        w.stop()

