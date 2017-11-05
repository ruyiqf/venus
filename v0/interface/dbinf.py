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
        #记录上次更新的时间区间
        self.update_date_area = tuple()
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

        self.update_date_area = (tdays_data[0][0][0],tdays_data[-1][0][0])

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
                self.__write2aststockd_file(stklist,
                    tdays_data[len(A_ST_stock_d):len(tdays_data)],
                    A_ST_stock_d)
            else:
                self.log.info('A_ST_stock_d已经最新无须更新')
        else:
            A_ST_stock_d = np.zeros(len(stklist))
            self.__write2aststockd_file(stklist,
                    tdays_data,
                    A_ST_stock_d)
        
    def db_download_stock_trade_able(self):
        """更新stock_trade_able表格
        """
        tdays_data = self.tdays_data 
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'stock_trade_able.mat')):
            stock_trade_able = sio.loadmat(DATAPATH+'stock_trade_able.mat')['stock_trade_able']
            if len(stock_trade_able) < len(tdays_data):
                self.__write2stock_trade_able_file(tdays_data[len(stock_trade_able):],
                        stock_trade_able, stklist)
            else:
                self.log.info('已经更新stock_trade_able表')
        else:
            self.__write2stock_trade_able_file(tdays_data, np.array([]), stklist)
        
    def db_download_ind_citic(self):
        """中信行业数据下载
        """
        ind_code_name_citic = self.__convert_mat2list(sio.loadmat(DATAPATH+'ind_code_name_CITIC.mat')['ind_code_name_CITIC'])
        ind_name_citic = self.__convert_mat2list(sio.loadmat(DATAPATH+'ind_name_CITIC.mat')['ind_name_CITIC'])
        if(os.path.exists(DATAPATH+'ind_of_stock_CITIC.mat')):
            ind_of_stock_citic = self.__convert_mat2list(sio.loadmat(DATAPATH+'ind_of_stock_CITIC.mat')['ind_of_stock_CITIC'])
            if len(ind_of_stock_citic) < len(self.tdays_data):
                self.__write2indcitic_file(self.tdays_data[len(ind_of_stock_citic):], ind_code_name_citic, ind_name_citic, ind_of_stock_citic) 
            else:
                self.log.info('已经最新数据不需要更新')
        else:
            ind_of_stock_citic = np.array([])
            self.__write2indcitic_file(self.tdays_data, ind_code_name_citic, ind_name_citic, ind_of_stock_citic) 
    
    def db_download_tradestatus(self):
        """更新tradestatus表格
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'tradestatus.mat')):
            tradestatus = sio.loadmat(DATAPATH+'tradestatus.mat')['tradestatus']
            if len(tradestatus) < len(tdays_data):
                self.__write2stock_tradestatus_file(tdays_data[len(tradestatus):],
                        tradestatus, stklist)
            else:
                self.log.info('已经更新tradestatus表')
        else:
            self.__write2stock_tradestatus_file(tdays_data, np.array([]), stklist)

    def db_download_updownlimit_status(self):
        """更新涨停状态表
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'up_down_limit_status.mat')):
            ud_limit_status = sio.loadmat(DATAPATH+'up_down_limit_status.mat')['up_down_limit_status']
            if len(ud_limit_status) < len(tdays_data):
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
                self.__write2price_factor_file(tdays_data[len(price_original):], price_original,
                        adjfactor, volume,
                        amount, TR, stklsit)
            else:
                self.log.info('价格相关的数据因子已经更新完毕')
        else:
            self.__write2price_factor_file(tdays_data, price_original,
                    adjfactor, volume,
                    amount, TR, stklsit)

    def db_download_price_factor(self):
        """更新价格因子最高价最低价开盘价
        """
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'open_original.mat')):
            open_original = sio.loadmat(DATAPATH+'open_original.mat')['open_original']
            if(len(open_original) < len(tdays_data)):
                high_original = sio.loadmat(DATAPATH+'high_original.mat')['high_original']
                low_original = sio.loadmat(DATAPATH+'low_original.mat')['low_original']
                self.__write2price_file(tdays_data[len(open_original):], open_original,
                                        high_original, low_original,
                                        stklist)
            else:
                self.log.info('价格因子已经更新完毕')
         else:
             self.__write2price_file(tdays_data, np.array([]),
                                     np.array([]), np.array([]),
                                     stklist)
            
    def db_download_bescfp_factor(self):
        tdays_data = self.tdays_data
        stklist = sio.loadmat(DATAPATH+'stock.mat')['stock']
        stklist = [elt[1][0] for elt in stklist]
        if(os.path.exists(DATAPATH+'BP.mat')):
            BP = sio.loadmat(DATAPATH+'BP.mat')['BP']
            if(len(BP) < len(tdays_data)):
                EP = sio.loadmat(DATAPATH+'EP.mat')['EP']
                SP = sio.loadmat(DATAPATH+'SP.mat')['SP']
                CFP = sio.loadmat(DATAPATH+'CFP.mat')['CFP']
                self.__write2bescfp_file(tdays_data[len(BP):],
                BP, EP, SP, CFP)
            else:
                self.log.info('BP,EP,SP,CFP更新完毕')
         else:
             self.__write2bescfp_file(tdays_data, np.array([]),
             np.array([]), np.array([]), np.array([]), stklist)

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
            tmp = rs1-rs2
            original = np.vstack((original, tmp))
        if np.all(original[0] == 0):
            original = np.delete(original, [0], axis=0)
        sio.savemat(DATAPATH+'A_ST_stock_d', mdict={'A_ST_stock_d': original})

    def __write2indcitic_file(self, datelist, ind_code_name, ind_name, original):
        """写入中信行业分类
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
        
    def __write2stock_trade_able_file(self, datelist, original, stklist):
        """更新股票交易列表的状态
        :datelist: 时间序列
        :original: 原有数据序列
        :stklist: 股票列表
        """
        tmp_array = np.zeros(len(stklist))
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
                    status = s_dq_tradestatus[s_info_windcode.index(stklist[k])] 
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
                    status = up_down_limit_status[s_info_windcode.index(stklist[k])]
                    data2.append(1 if status == 0 else 0)
                else:
                    data2.append(0)
            tmp_array = np.vstack((tmp_array, np.array(data1) & np.array(data2)))
        np.delete(tmp_array, [0], axis=0)
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
        tmp_array = np.zeros(len(stklist)) 
        cursor = self.conn.cursor()
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
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
                    data.append(0)
            tmp_array = np.vstack((tmp_array, np.array(data)))
        np.delete(tmp_array, [0], axis=0)
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
        tmp_array = np.zeros(len(stklist)) 
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
                    data.append(2)
            tmp_array = np.vstack((tmp_array, np.array(data)))
        np.delete(tmp_array, [0], axis=0)
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
        tmp_close = np.zeros(len(stklist))
        tmp_adjfactor = np.zeros(len(stklist))
        tmp_volume = np.zeros(len(stklist))
        tmp_amount = np.zeros(len(stklist))
        tmp_tr = np.zeros(len(stklist))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
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
                    close.append(-1)
                    adjfactor.append(-1)
                    volume.append(-1)
                    amount.append(-1)
            tmp_close = np.vstack((tmp_close, np.array(close)))
            tmp_adjfactor = np.vstack((tmp_adjfacor, np.array(adjfactor)))
            tmp_volume = np.vstack((tmp_volume, np.array(volume)))
            tmp_amount = np.vstack((tmp_amount, np.array(amount)))
            sql = 'select s_info_windcode,turnover_d from  AShareYield   WHERE trade_dt= %s'%sdate
            cursor = execute(sql)
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
                    turnover.append(float('nan'))

            turnover = Series(turnover).fillna(method='ffill').values
            tmp_tr = np.vstack((tmp_tr, turnover))
                    
        np.delete(tmp_close, [0], axis=0)
        np.delete(tmp_adjfactor, [0], axis=0)
        np.delete(tmp_volume, [0], axis=0)
        np.delete(tmp_amount, [0], axis=0)
        np.delete(tmp_tr, [0], axis=0)

        if price_original.size == 0:
            sio.savemat(DATAPATH+'price_original.mat', mdict={'price_original':tmp_close})
            sio.savemat(DATAPATH+'adjfactor.mat', mdict={'adjfactor':tmp_adjfactor})
            sio.savemat(DATAPATH+'volume.mat', mdict={'volume':tmp_volume})
            sio.savemat(DATAPATH+'amount.mat', mdict={'amount':tmp_amount})
            sio.savemat(DATAPATH+'TR.mat', mdict={'TR':tmp_tr})
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

    def __write2price_file(self, datelist, open_original, high_original, low_original, stklist):
        cursor = self.conn.cursor()
        tmp_open = np.zeros((len(datelist),len(stklist)))
        tmp_high = np.zeros((len(datelist),len(stklist)))
        tmp_low = np.zeros((len(datelist),len(stklist)))
        for i in range(len(datelist)):
            sdate = datetime.datetime.strptime(datelist[i], '%Y/%m/%d').strftime('%Y%m%d')
            sql = 'select s_info_windcode,s_dq_open,s_dq_high,s_dq_low from  AShareEODPrices   WHERE trade_dt=%s' % sdate
            cursor.execute(sql)
            rs = cursor.fetchall()
            ret = self.__convert_dbdata2tuplelist(rs, 2)
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
                    dq_open.append(-1)
                    dq_high.append(-1)
                    dq_low.append(-1)
            tmp_open[i] = np.array(dq_open)
            tmp_high[i] = np.array(dq_high)
            tmp_low[i] = np.array(dq_low)
        if open_original.size == 0:
            sio.savemat(DATAPATH+'open_original.mat', mdict={'open_original':tmp_open})
            sio.savemat(DATAPATH+'high_original.mat', mdict={'open_original':tmp_high})
            sio.savemat(DATAPATH+'low_original.mat', mdict={'open_original':tmp_low})
        else:
            open_original = np.vstack((open_original, tmp_open))
            high_original = np.vstack((high_original, tmp_high))
            low_original = np.vstack((low_original, tmp_low))
            
            sio.savemat(DATAPATH+'open_original.mat', mdict={'open_origianl':open_original})
            sio.savemat(DATAPATH+'high_original.mat', mdict={'high_origianl':high_original})
            sio.savemat(DATAPATH+'low_original.mat', mdict={'low_origianl':low_original})
       
        
        
