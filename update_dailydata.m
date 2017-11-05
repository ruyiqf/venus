% written on 2016/2/3
%clear
%clc
%close all
global w
conn = database('WDIFDATA','qhqidata','qhqidata') ;
javaaddpath 'D:\MATLAB\R2015b\java\jar\toolbox\ojdbc6.jar'
conn=database('orcl','orcl','orcl','oracle.jdbc.driver.OracleDriver', 'jdbc:oracle:thin:@ 192.168.1.235:1521:');
% conn = database('optdb','orcl','orcl');
%%  股票池
FileStr = './data/stock_pool.mat';
if exist(FileStr, 'file') ~= 2
    [NUM,TXT,RAW]=xlsread('股票池0530.xlsx','股票池') ;
    load  ./data/stock;
    stock_pool=zeros(size(stklist));
    l1=size(RAW,1);
    l2=size(stklist,1);
    for  i=2:l1
        i
        for  j=1:l2
            if  strcmp(RAW{i,2},stklist{j,1}(1:end-3))
                stock_pool(j)=1;
                RAW{i,3}=j;
            end
        end
    end
    save('./data/stock_pool.mat','stock_pool');
end
%% 1.更新tdays_data
if now>today()+0.625
    end_date=datestr(today(),'yyyymmdd');%下午三点之后，更新到今天
else
    end_date=datestr(today()-1,'yyyymmdd');%更新到昨天
end
FileStr = './data/tdays_data.mat';
display(' 交易时间下载')
[tdays_data]=w.tdays('2005-01-01',end_date);
save(FileStr,'tdays_data');

FileStr = './data/tdays_data_week.mat';
[tdays_data_week]=w.tdays('2005-01-01',end_date,'Period=W');
tdays_data_week=tdays_data_week(1:end-1);
save(FileStr,'tdays_data_week');
%% 2.申万一级行业，更新历史每日收益率以及wind全市场指数的收益率
FileStr = './data/Ind_daily.mat';
load './data/tdays_data.mat';
end_date = tdays_data(end);
indList = '801010.SI,801020.SI,801030.SI,801040.SI,801050.SI,801080.SI,801110.SI,801120.SI,801130.SI,801140.SI,801150.SI,801160.SI,801170.SI,801180.SI,801200.SI,801210.SI,801230.SI,801710.SI,801720.SI,801730.SI,801740.SI,801750.SI,801760.SI,801770.SI,801780.SI,801880.SI,801890.SI,801194.SI,801191.SI,801193.SI';
[ind_pct_chg]=w.wsd(indList,'pct_chg','2005-01-01',end_date,'Fill=Previous','PriceAdj=F');
ind_pct_chg=ind_pct_chg./100;
[ind_name]=w.wsd(indList,'sec_name');
[ind_code]=w.wsd(indList,'trade_code');
save (FileStr ,'ind_pct_chg','ind_name','ind_code');

market=w.wsd('881001.WI','pct_chg','2005-01-01',end_date,'Fill=Previous','PriceAdj=F')/100;
save( './data/market.mat' ,'market');
%行业近10天的收益率，做动量
ind_momentum=zeros(size(ind_pct_chg));
for i=11:size(ind_momentum,1)
    ind_momentum(i,:)=prod(ind_pct_chg(i-9:i,:)+1)-1;
end
save( './data/ind_momentum.mat' ,'ind_momentum');

%% 5.每交易日：全A（非ST）
display('全A（非ST）:')
FileStr = './data/A_ST_stock_d.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(A_ST_stock_d,1);
else
    start = 0;
    A_ST_stock_d = [];
end
load  './data/stock.mat';
load './data/tdays_data.mat';
l1=length(tdays_data);
l2=length(stock);
start = size(A_ST_stock_d,1);
if l1 > start
    for  i=start+1:l1
        i
        A_ST_temp = NaN(1,l2);%修改成nan
        date=datestr(tdays_data(i),'yyyymmdd');
        %全A股
        eval(['curs=exec(conn, ''select s_info_windcode from  AShareDescription  where cast(s_info_listdate as  integer)<=''''', date,''''' and (cast(s_info_delistdate as  integer)>''''', date,'''''    or s_info_delistdate is null) '');']);
        curs=fetch(curs);
        data1=curs.Data;
        if strcmp(data1,'no data')
            error([FileStr, '出问题']);
        end
        if size(data1,1) == 1
            error([FileStr, '出问题']);
        end
        %ST股票(包含了退市的ST)
        eval(['curs=exec(conn, ''select s_info_windcode from  AShareST where cast(entry_dt as  integer)<=''''', date,''''' and (cast(remove_dt as  integer)>''''', date,'''''    or remove_dt is null) '');']);
        curs=fetch(curs);
        data2=curs.Data(:,1);
        if strcmp(data2,'no data')
            error([FileStr, '出问题']);
        end
        if size(data2,1) == 1
            error([FileStr, '出问题']);
        end
        
        [~,index1] = ismember(data1,stklist);
        [~,index2] = ismember(data2,stklist);
        temp=setdiff(index1,index2);
        A_ST_temp(temp(find(temp>0))) = 1;
        A_ST_stock_d(i,:) = A_ST_temp;
    end
    save('./data/A_ST_stock_d.mat','A_ST_stock_d');
    clear A_ST_stock_d;
end
%% 2.1 所有股票所属申万行业
display('所属行业:')
load  './data/stock.mat';
load './data/tdays_data.mat';
load  './data/A_ST_stock_d.mat';
l1=length(tdays_data);
l2=length(stock);
FileStr = './data/ind_of_stock.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(ind_of_stock,1);
else
    start = 0;
    ind_of_stock = [];
end
code=stklist{1};
for  j=2:l2
    code=[code,',',stklist{j}];
end
load './data/ind_name_before_20140101.mat';
for  k=start+1:l1
    k
    if  k>=2183%20140101之后，用新的行业划分
        load './data/ind_name.mat';
    end
    date=datestr(tdays_data(k),'yyyymmdd');
    data1=w.wss(code,'industry2','industryType=1','industryStandard=1',['tradeDate=' date]);
    data2=w.wss(code,'industry2','industryType=1','industryStandard=2',['tradeDate=' date]);
    
    for  i=1:l2
        if  isnan(data1{i})
            continue;
        end
        flag=0;%是否找到一级行业
        for  j=1:length(ind_name)
            if   strcmp(data1{i},ind_name{j})
                flag=1;
                ind_of_stock(k,i)=j;
            end
        end
        if  flag==0%一级行业没找到，匹配二级行业
            for  j=1:length(ind_name)
                if   strcmp(data2{i},ind_name{j})||strcmp(data2{i},ind_name{j})
                    ind_of_stock(k,i)=j;
                    flag=1;
                    break;
                end
            end
        end
        if  flag==0&&A_ST_stock_d(k,i)==1%退市之后的票行业找不到，比如000549，行业是交运设备
            'error'
            return;
        end
    end
end
save ('./data/ind_of_stock.mat' ,'ind_of_stock');
%% 2.2 所有股票所属中信行业
load  './data/stock.mat';
load './data/tdays_data.mat';
l1=length(tdays_data);
l2=length(stock);
FileStr = './data/ind_of_stock_CITIC.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(ind_of_stock_CITIC,1);
else
    start = 0;
    ind_of_stock_CITIC = [];
end

load './data/ind_code_name_CITIC.mat';
load './data/ind_name_CITIC.mat';
for  k=start+1:l1
    k
    tic
    date=datestr(tdays_data(k),'yyyymmdd');
    eval(['curs=exec(conn, ''select s_info_windcode,s_con_windcode, s_con_indate,s_con_outdate from  AIndexMembersCITICS where cast(s_con_indate as  integer)<=''''', date,''''' and (cast(s_con_outdate as  integer)>''''', date,'''''    or s_con_outdate is null) '');']);
    curs=fetch(curs);
    data_ind1=curs.Data(:,1);
    data_code1=curs.Data(:,2); 
    close(curs);
    %找出中信一级行业
    [~,index1]=ismember(data_ind1,ind_code_name_CITIC(:,1));
    %没有对应上的找出中信二级行业
    ind_nan=data_code1(index1==0);
    if ~isempty(ind_nan)
        [w_wss_data,~,~,~,~,~]=w.wss(ind_nan,'sec_name,industry_citic',strcat('tradeDate=',date),'industryType=2');
        [~,index2]=ismember(w_wss_data(:,2),ind_name_CITIC);
        index1(index1==0)=index2;
    end
    [~,stock_index1]=ismember(data_code1,stklist);
    if sum(stock_index1==0)>0
        index1=index1(stock_index1>0);
        stock_index1=stock_index1(stock_index1>0);    
    end
    ind_of_stock_CITIC(k,stock_index1)=index1;
    toc
end
save ('./data/ind_of_stock_CITIC.mat' ,'ind_of_stock_CITIC');
%% 3.HS300/ZZ500/SZ50日度收益率
date = tdays_data(1);
date1 = tdays_data(end);
SZ50_daily_ret=w.wsd('000016.SH','pct_chg',date,date1,'Fill=Previous','PriceAdj=F')/100;
save ('./data/SZ50_daily_ret.mat' ,'SZ50_daily_ret');
HS300_daily_ret=w.wsd('000300.SH','pct_chg',date,date1,'Fill=Previous','PriceAdj=F')/100;
save ('./data/HS300_daily_ret.mat' ,'HS300_daily_ret');
ZZ500_daily_ret=w.wsd('000905.SH','pct_chg',date,date1,'Fill=Previous','PriceAdj=F')/100;
save ('./data/ZZ500_daily_ret.mat' ,'ZZ500_daily_ret');
%隔夜收益
temp=w.wsd('000300.SH','pre_close,open',date,date1,'Fill=Previous','PriceAdj=F');
HS300_overnight_ret=temp(:,2)./temp(:,1)-1;
save ('./data/HS300_overnight_ret.mat' ,'HS300_overnight_ret');
temp=w.wsd('000905.SH','pre_close,open',date,date1,'Fill=Previous','PriceAdj=F');
ZZ500_overnight_ret=temp(:,2)./temp(:,1)-1;
save ('./data/ZZ500_overnight_ret.mat' ,'ZZ500_overnight_ret');
%指数值
HS300=w.wsd('000300.SH','close',date,date1);
save ('./data/HS300.mat' ,'HS300');
ZZ500=w.wsd('000905.SH','close',date,date1);
save ('./data/ZZ500.mat' ,'ZZ500');
%% 4.更新一些状态信息。股票涨跌停、停牌一天，则为0，否则为1
display('stock_trade_able:')
load  './data/stock.mat';
load  './data/tdays_data.mat';
FileStr = './data/stock_trade_able.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(stock_trade_able,1);
else
    start = 0;
    stock_trade_able = [];
end

l1=length(tdays_data);
if l1 > start
    l2=length(stock);
    
    for  i=start+1:l1
        i
        tradable_temp = NaN(1,l2);%修改成nan
        date=datestr(tdays_data(i),'yyyymmdd');
        %停牌的剔除
        %         tic
        eval(['curs=exec(conn, ''select s_info_windcode,s_dq_tradestatus from  AShareEODPrices  WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        %         toc
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        data1=cell(1,l2);
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            data1(1,k)=data(j,2);
        end
        %         tic
        eval(['curs=exec(conn, ''select s_info_windcode,up_down_limit_status from AShareEODDerivativeIndicator   WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        %         toc
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        data2=nan(1,l2);
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            data2(1,k)=data{j,2};
        end
        %data1返回值有三种情况：交易、 停牌,N（上市）、 XD（除 息),XR （除权),DR（除权除息)
        %data2 0为没有涨跌停，1为涨停，-1为跌停
        
        
        flag1 = strcmp(data1,'交易')|  strcmp(data1,'XD')| strcmp(data1,'XR')| strcmp(data1,'DR');
        flag2 = (data2 == 0);
        
        flag = flag1 & flag2;
        tradable_temp(flag) = 1;
        stock_trade_able(i,:) = tradable_temp;
    end
    save(FileStr,'stock_trade_able');
    clear stock_trade_able;
end
%% tradestatus(是否可以交易)
display('tradestatus:')
load  './data/stock.mat';
load  './data/tdays_data.mat';
FileStr = './data/tradestatus.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(tradestatus,1);
else
    start = 0;
    tradestatus = [];
end

l1=length(tdays_data);
if l1 > start
    l2=length(stock);
    
    for  i=start+1:l1
        i
        tradestatus_temp = NaN(1,l2);%修改成nan
        date=datestr(tdays_data(i),'yyyymmdd');
        %停牌的剔除
        eval(['curs=exec(conn, ''select s_info_windcode,s_dq_tradestatus from  AShareEODPrices  WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        
        data1=cell(1,l2);
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            data1(1,k)=data(j,2);
        end
        %data1返回值有三种情况：交易、 停牌,N（上市）、 XD（除 息),XR （除权),DR（除权除息)
        flag1 = strcmp(data1,'交易')|  strcmp(data1,'XD')| strcmp(data1,'XR')| strcmp(data1,'DR');
        flag = flag1 ;
        tradestatus_temp(flag) = 1;
        tradestatus(i,:) = tradestatus_temp;
    end
    save(FileStr,'tradestatus');
    clear tradestatus;
end

%% up_down_limit_status(是否涨跌停)
display('up_down_limit_status:')
load  './data/stock.mat';
load  './data/tdays_data.mat';
FileStr = './data/up_down_limit_status.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(up_down_limit_status,1);
else
    start = 0;
    up_down_limit_status = [];
end

l1=length(tdays_data);
if l1 > start
    l2=length(stock);
    
    for  i=start+1:l1
        i
        up_down_limit_status_temp = NaN(1,l2);%修改成nan
        date=datestr(tdays_data(i),'yyyymmdd');
        %停牌的剔除
        eval(['curs=exec(conn, ''select s_info_windcode,up_down_limit_status from AShareEODDerivativeIndicator   WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        data2=nan(1,l2);
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            data2(1,k)=data{j,2};
        end
        %data2 0为没有涨跌停，1为涨停，-1为跌停
        
        up_down_limit_status(i,:) = data2;
    end
    save(FileStr,'up_down_limit_status');
    clear up_down_limit_status;
end
%%  全部A股：A_stock_d
display('A_stock_d:')
FileStr = './data/A_stock_d.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(A_stock_d,1);
else
    start = 0;
    A_stock_d = [];
end
load  './data/stock.mat';
load './data/tdays_data.mat';
l1=length(tdays_data);
l2=length(stock);
start = size(A_stock_d,1);

if l1 > start
    for  i=start+1:l1
        i
        A_ST_temp = NaN(1,l2);%修改成nan
        date=datestr(tdays_data(i),'yyyymmdd');
        %全A股
        eval(['curs=exec(conn, ''select s_info_windcode from  AShareDescription  where cast(s_info_listdate as  integer)<=''''', date,''''' and (cast(s_info_delistdate as  integer)>''''', date,'''''    or s_info_delistdate is null) '');']);
        curs=fetch(curs);
        data1=curs.Data;
        if strcmp(data1,'no data')
            error([FileStr, '出问题']);
        end
        if size(data1,1) == 1
            error([FileStr, '出问题']);
        end
        [~,index1] = ismember(data1,stklist);
        A_ST_temp(index1(find(index1>0))) = 1;
        A_stock_d(i,:) = A_ST_temp;
    end
    save('./data/A_stock_d.mat','A_stock_d');
    clear A_stock_d;
end
%% 6.价格数据,复权因子，前复权数据，成交量，成交额,TR（日换手率）
load  './data/stock.mat';
load  './data/tdays_data.mat';
l1=length(tdays_data);
l2=length(stock);
display('每天所有股票价格下载:')
FileStr = './data/price_original.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/adjfactor.mat';
    load  './data/volume.mat';%手
    load  './data/amount.mat';%千元
    load  './data/TR.mat';%百分数换手率
    start = size(price_original,1);
else
    start = 0;
    price_original = [];
    adjfactor=[];
    volume=[];
    amount=[];
    TR=[];
end
start = size(price_original,1);%要求其和adjfactor的矩阵大小一样

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        eval(['curs=exec(conn, ''select s_info_windcode,s_dq_close,s_dq_adjfactor,s_dq_volume,s_dq_amount from  AShareEODPrices   WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        price_original_temp =NaN(1,l2);%修改成nan
        adjfactor_temp =NaN(1,l2);%修改成nan
        volume_temp =NaN(1,l2);%修改成nan
        amount_temp =NaN(1,l2);%修改成nan
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            price_original_temp(1,k)=data{j,2};
            adjfactor_temp(1,k)=data{j,3};
            volume_temp(1,k)=data{j,4}*100;
            amount_temp(1,k)=data{j,5}*1000;
        end
        price_original(i,:) =price_original_temp;
        adjfactor(i,:)  = adjfactor_temp;
        volume(i,:) = volume_temp;
        amount(i,:)  =amount_temp;
        
        
        eval(['curs=exec(conn, ''select s_info_windcode,turnover_d from  AShareYield   WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        TR_temp =zeros(1,l2);
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            TR_temp(1,k)=data{j,2};
        end
        TR(i,:)  = TR_temp;
    end
    %若退市，adjfactor往后赋值，便于计算price_forward_adjusted
    for j=1:l2
        for  i=2:l1
            if  isnan(adjfactor(i-1,j))==0&&isnan(adjfactor(i,j))==1%退市后adjfactor为NaN
                adjfactor(i,j)=adjfactor(i-1,j);
            end
            if  isnan(price_original(i-1,j))==0&&isnan(price_original(i,j))==1%退市后adjfactor为NaN
                price_original(i,j)=price_original(i-1,j);
            end
        end
    end
    save ('./data/price_original.mat' ,'price_original');
    save ('./data/adjfactor.mat' ,'adjfactor');
    save ('./data/volume.mat' ,'volume');
    save ('./data/amount.mat' ,'amount');
    save ('./data/TR.mat' ,'TR');
    
    price_forward_adjusted=zeros(l1,l2);
    for  i=1:l1
        price_forward_adjusted(i,:)=price_original(i,:).*adjfactor(i,:)./adjfactor(end,:);
    end
    save('./data/price_forward_adjusted.mat','price_forward_adjusted');
    
    price_daily_1=price_original;
    save('./data/daily_factor/price_daily_1.mat','price_daily_1');
    clear  'price_original' 'adjfactor' 'volume' 'amount' 'TR' 'price_forward_adjusted' 'price_daily_1';
end

%% 6.价格数据(开、高、低)
load  './data/stock.mat';
load  './data/tdays_data.mat';
l1=length(tdays_data);
l2=length(stock);
display('每天所有股票价格下载(开、高、低):')
FileStr = './data/open_original.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/high_original.mat';
    load  './data/low_original.mat';
    start = size(open_original,1);
else
    start = 0;
    open_original = [];
    high_original = [];
    low_original = [];
end
start = size(open_original,1);%要求其和adjfactor的矩阵大小一样

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        eval(['curs=exec(conn, ''select s_info_windcode,s_dq_open,s_dq_high,s_dq_low from  AShareEODPrices   WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        open_original_temp =NaN(1,l2);%修改成nan
        high_original_temp =NaN(1,l2);%修改成nan
        low_original_temp =NaN(1,l2);%修改成nan
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            open_original_temp(1,k)=data{j,2};
            high_original_temp(1,k)=data{j,3};
            low_original_temp(1,k)=data{j,4};
        end
        open_original(i,:) = open_original_temp;
        high_original(i,:) = high_original_temp;
        low_original(i,:) = low_original_temp;
        
    end
    for j=1:l2
        for  i=2:l1
            if  isnan(open_original(i-1,j))==0&&isnan(open_original(i,j))==1%退市后adjfactor为NaN
                open_original(i,j)=open_original(i-1,j);
            end
            if  isnan(high_original(i-1,j))==0&&isnan(high_original(i,j))==1%退市后adjfactor为NaN
                high_original(i,j)=high_original(i-1,j);
            end
            if  isnan(low_original(i-1,j))==0&&isnan(low_original(i,j))==1%退市后adjfactor为NaN
                low_original(i,j)=low_original(i-1,j);
            end
        end
    end
    save ('./data/open_original.mat' ,'open_original');
    save ('./data/high_original.mat' ,'high_original');
    save ('./data/low_original.mat' ,'low_original');
    
    load  './data/adjfactor.mat';
    open_forward_adjusted=zeros(l1,l2);
    high_forward_adjusted=zeros(l1,l2);
    low_forward_adjusted=zeros(l1,l2);
    for  i=1:l1
        open_forward_adjusted(i,:)=open_original(i,:).*adjfactor(i,:)./adjfactor(end,:);
        high_forward_adjusted(i,:)=high_original(i,:).*adjfactor(i,:)./adjfactor(end,:);
        low_forward_adjusted(i,:)=low_original(i,:).*adjfactor(i,:)./adjfactor(end,:);
    end
    save('./data/open_forward_adjusted.mat','open_forward_adjusted');
    save('./data/high_forward_adjusted.mat','high_forward_adjusted');
    save('./data/low_forward_adjusted.mat','low_forward_adjusted');
end
%% 7.市值因子
display(' 更新市值因子')
load  './data/tdays_data.mat';
load  './data/stock.mat';
FileStr = './data/daily_factor/MV.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(MV,1);
else
    start = 0;
    MV = [];
end
display('每天所有流通市值下载:')
l1=length(tdays_data);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        MV(i,:)= (w.wss(stklist,'mkt_cap_ashare',['tradeDate=',date]))';%市值（不含限售股），这个可以和以前从数据库导出的数据吻合
    end
    save ('./data/daily_factor/MV.mat' ,'MV');
    clear 'MV';
end
% load  './data/tdays_data.mat';
% load  './data/stock.mat';
% FileStr = './data/daily_factor/MV.mat';
% if exist(FileStr, 'file') == 2
%     load(FileStr);
%     start = size(MV,1);
% else
%     start = 0;
%     MV = [];
% end
% display('每天所有流通市值下载:')
% l1=length(tdays_data);
% l2=length(stock);
% 
% start = 0;
% if l1 > start
%     for  i=start+1:l1
%         i
%         date=datestr(tdays_data(i),'yyyymmdd');
%         eval(['curs=exec(conn, ''select s_info_windcode,s_dq_mv from  AShareEODDerivativeIndicator  WHERE trade_dt= ''''', date,''''' '');']);
%         curs=fetch(curs);
%         data=curs.Data;
%         if strcmp(data,'no data')
%             error([FileStr, '出问题']);
%         end
%         if size(data,1) == 1
%             error([FileStr, '出问题']);
%         end
%         MV_temp =NaN(1,l2);%修改成nan
%         len=size(data,1);
%         for  j=1:len
%             k=find(strcmp(stklist,data{j,1}));
%             MV_temp(1,k)=data{j,2}*1e4;%数据库中储存的万元
%         end
%         MV(i,:) = MV_temp;
%     end
%     save ('./data/daily_factor/MV.mat' ,'MV');
%     clear 'MV';
% end
%% 8. ILLIQ_1M(流动冲击因子）：avg(|当日涨跌幅|/当日成交量)，观察期为21天
display('ILLIQ_1M:')
load  './data/amount.mat';
load  './data/price_forward_adjusted.mat';

FileStr = './data/daily_factor/ILLIQ_1M.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(ILLIQ_1M,1);
else
    start = 0;
    ILLIQ_1M = [];
end

l1=length(tdays_data);
l2=length(stock);
len=21;
if l1 > start
    for  i=start+1:l1
        i
        ILLIQ_1M(i,:) = NaN(1,l2);%修改成nan
        if i > len
            for j=1:l2%股票的数量
                temp=abs(price_forward_adjusted(i-len+1:i,j)./price_forward_adjusted(i-len:i-1,j)-1)./amount(i-len+1:i,j);
                if  length(find(isnan(temp) | isinf(temp)))>floor(len/2)
                    ILLIQ_1M(i,j)=nan;
                    continue;
                end
                ILLIQ_1M(i,j)=mean(temp(~isnan(temp) & ~isinf(temp)));
            end
        end
    end
    save ('./data/daily_factor/ILLIQ_1M.mat' ,'ILLIQ_1M');
    clear 'ILLIQ_1M';
end
%% 9. 每天所有BP,EP,SP,CFP下载
display(' BP,EP,SP,CFP')
load  './data/tdays_data.mat';
load  './data/stock.mat';
FileStr = './data/daily_factor/BP.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/EP.mat';
    load  './data/daily_factor/SP.mat';
    load  './data/daily_factor/CFP.mat';
    start = size(BP,1);
else
    start = 0;
    BP = [];
    EP = [];
    SP = [];
    CFP = [];
end
l1=length(tdays_data);
l2=length(stock);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        
        eval(['curs=exec(conn, ''select s_info_windcode,s_val_pb_new,s_val_pe_ttm,s_val_ps_ttm,s_val_pcf_ocfttm from  AShareEODDerivativeIndicator   WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        BP_temp =nan(1,l2);
        EP_temp =nan(1,l2);
        SP_temp =nan(1,l2);
        CFP_temp =nan(1,l2);
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            BP_temp(1,k)=data{j,2};
            EP_temp(1,k)=data{j,3};
            SP_temp(1,k)=data{j,4};
            CFP_temp(1,k)=data{j,5};
        end
        BP(i,:) = 1./BP_temp;
        EP(i,:) = 1./EP_temp;
        SP(i,:) = 1./SP_temp;
        CFP(i,:) = 1./CFP_temp;
    end
    save ('./data/daily_factor/BP.mat' ,'BP');
    save ('./data/daily_factor/EP.mat' ,'EP');
    save ('./data/daily_factor/SP.mat' ,'SP');
    save ('./data/daily_factor/CFP.mat' ,'CFP');
    clear 'BP' 'EP' 'SP' 'CFP' ;
end
%% 10.目标价涨升空间以及经过市值调整后的指标(数据库和wind提取数据不一样)
%     一致预测目标价
% 1、Wind中国盈利预测数据库从2004年起提供数据服务，通过签约方式与各大研究机构进行合作，整理并发布盈利预测与投资评级数据。
% 2、各机构研究报告都有一个相对有效期，本指标只统计尚处于有效期内的预测结果。按不同有效期区分为以下综合值类型：
% 1）万得一致预测：为指定日期近180天内所有机构最新预测数据的算术平均值；
% 2）前瞻预测：为指定日期近90天内所有机构最新预测数据的算术平均值；
% 3）领先预测：为指定日期30天内所有机构最新预测数据的算术平均值；
% 4）大事后预测：为最近业绩报告披露日至指定日期内所有机构最新预测数据的算术平均值。
load  './data/price_forward_adjusted.mat';
load  './data/daily_factor/MV.mat';
load  './data/A_ST_stock_d.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';

display('每天所有TPS_180下载:')
FileStr = './data/daily_factor/TPS.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/TPS_180.mat';
    load  './data/daily_factor/res_TPS_180.mat';
    start = size(TPS,1);
else
    start = 0;
    TPS = [];
    TPS_180=[];
    res_TPS_180=[];
end

l1=length(tdays_data);
start = size(TPS_180,1);
if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        
        eval(['curs=exec(conn, ''select s_info_windcode,s_est_price from  AShareStockRatingConsus   WHERE rating_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        TPS_temp =zeros(1,size(price_forward_adjusted,2));
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            TPS_temp(1,k)=data{j,2};
        end
        TPS(i,:) = TPS_temp;
    end
    save ('./data/daily_factor/TPS.mat' ,'TPS');
    TPS_180=TPS./price_forward_adjusted-1;
    save ('./data/daily_factor/TPS_180.mat' ,'TPS_180');
    
    % TPS_180对MV进行横截面回归，要求MV,tps_180,A_ST_stock_d等数据已经更新
    for i=start+1:l1
        i
        res_TPS_180(i,:) = NaN(1,size(res_TPS_180,2));%初始化为NaN
        ind=find(A_ST_stock_d(i,:)==1 );
        X1=ones(length(ind),1);
        X2=MV(i,ind)';
        X=[X1 X2 ];
        Y=TPS_180(i,ind)';
        [~,~,r]=regress(Y,X);
        res_TPS_180(i,ind)=r';
    end
    save ('./data/daily_factor/res_TPS_180.mat' ,'res_TPS_180');
end
%% 11.profit_pred_4w 一致预测净利润变化率(与4周前比)=(指定交易日一致预测净利润- 4周前一致预测净利润)/ABS (4周前一致预测净利润)*100%
% 数据库无该数据,需要合成:表 AShareConsensusData/AShareEarningEst
display('每天所有profit_pred_4w下载:')
load  './data/daily_factor/profit_pred_4w.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';

l1=length(tdays_data);
start = size(profit_pred_4w,1);
if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        year=datestr(tdays_data(i),'yyyy');
        profit_pred_4w(i,:)=(w.wss(stklist,'west_nproc_4w',['tradeDate=',date],['year=',year]))';
    end
    save ('./data/daily_factor/profit_pred_4w.mat' ,'profit_pred_4w');
end
%% wrating_upgrade 近90天上调家数
%数据库提取
display('wrating_upgrade:')
load  './data/tdays_data.mat';
load  './data/stock.mat';
FileStr = './data/daily_factor/wrating_upgrade.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/wrating_upgrade.mat';
else
    wrating_upgrade= [];
end
l1=length(tdays_data);
start = size(wrating_upgrade,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        eval(['curs=exec(conn, ''select s_info_windcode,s_wrating_upgrade from  AShareStockRatingConsus   WHERE rating_dt= ''''', date,''''' and s_wrating_cycle=0263002000 '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        wrating_upgrade_temp =zeros(1,size(stklist,1));
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            wrating_upgrade_temp(1,k)=data{j,2};
        end
        wrating_upgrade(i,:) = wrating_upgrade_temp;
    end
    save ('./data/daily_factor/wrating_upgrade.mat' ,'wrating_upgrade');
end
% %wind终端提取
% display('每天所有wrating_upgrade下载:')
% load  './data/tdays_data.mat';
% load  './data/stock.mat';
% FileStr = './data/daily_factor/wrating_upgrade.mat';
% if exist(FileStr, 'file') == 2
%     load(FileStr);
%     load  './data/daily_factor/wrating_upgrade.mat';
% else
%     wrating_upgrade= [];
% end
% l1=length(tdays_data);
% start = size(wrating_upgrade,1);
% if l1 > start
%     for  i=start+1:l1
%         i
%         date=datestr(tdays_data(i),'yyyymmdd');
%         year=datestr(tdays_data(i),'yyyy');
%         wrating_upgrade(i,:)=(w.wss(stklist,'wrating_upgrade',['tradeDate=',date],'westPeriod=90'))';
%     end
%     save ('./data/daily_factor/wrating_upgrade.mat' ,'wrating_upgrade');
%     cum_wrating_upgrade=zeros(size(wrating_upgrade));
%     for i=90:l1
%         cum_wrating_upgrade(i,:)=sum(wrating_upgrade(i-90+1:i,:));
%     end
%     save ('./data/daily_factor/cum_wrating_upgrade.mat' ,'cum_wrating_upgrade');
% end
%% 盈利预测
%FY1
display('盈利预测FY1:')
load  './data/tdays_data.mat';
load  './data/stock.mat';

FileStr = './data/daily_factor/roe_forcast_FY1.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/pe_forcast_FY1.mat';
    load  './data/daily_factor/pb_forcast_FY1.mat';
    load  './data/daily_factor/peg_forcast_FY1.mat';
    start = size(roe_forcast_FY1,1);
else
    start = 0;
    roe_forcast_FY1=[];
    
    pe_forcast_FY1=[];
    pb_forcast_FY1=[];
    peg_forcast_FY1=[];
end

l1=length(tdays_data);
l2=length(stklist);
start = size(roe_forcast_FY1,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        
        eval(['curs=exec(conn, ''select s_info_windcode,est_roe,est_pe,est_pb,est_peg from  AshareConsensusRollingData   WHERE rolling_type=''''FY1'''' and est_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        roe_forcast_FY1_temp=nan(1,l2);
        pe_forcast_FY1_temp =nan(1,l2);
        pb_forcast_FY1_temp =nan(1,l2);
        peg_forcast_FY1_temp=nan(1,l2);
        
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            roe_forcast_FY1_temp(1,k)=data{j,2};
            pe_forcast_FY1_temp(1,k)=data{j,3};
            pb_forcast_FY1_temp(1,k)=data{j,4};
            peg_forcast_FY1_temp(1,k)=data{j,5};
        end
        roe_forcast_FY1(i,:) =roe_forcast_FY1_temp;
        pe_forcast_FY1(i,:) =pe_forcast_FY1_temp;
        pb_forcast_FY1(i,:) = pb_forcast_FY1_temp;
        peg_forcast_FY1(i,:)  = peg_forcast_FY1_temp;
    end
    save ('./data/daily_factor/roe_forcast_FY1.mat' ,'roe_forcast_FY1');
    save ('./data/daily_factor/pe_forcast_FY1.mat' ,'pe_forcast_FY1');
    save ('./data/daily_factor/pb_forcast_FY1.mat' ,'pb_forcast_FY1');
    save ('./data/daily_factor/peg_forcast_FY1.mat' ,'peg_forcast_FY1');
end
%% FTTM
display('盈利预测FTTM:')
load  './data/tdays_data.mat';
load  './data/stock.mat';

FileStr = './data/daily_factor/roe_forcast_FTTM.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/pe_forcast_FTTM.mat';
    load  './data/daily_factor/pb_forcast_FTTM.mat';
    load  './data/daily_factor/peg_forcast_FTTM.mat';
    start = size(roe_forcast_FTTM,1);
else
    start = 0;
    roe_forcast_FTTM=[];
    
    pe_forcast_FTTM=[];
    pb_forcast_FTTM=[];
    peg_forcast_FTTM=[];
end

l1=length(tdays_data);
l2=length(stklist);
start = size(roe_forcast_FTTM,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        
        eval(['curs=exec(conn, ''select s_info_windcode,est_roe,est_pe,est_pb,est_peg from  AshareConsensusRollingData   WHERE rolling_type=''''FTTM'''' and est_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        roe_forcast_FTTM_temp=nan(1,l2);
        pe_forcast_FTTM_temp =nan(1,l2);
        pb_forcast_FTTM_temp =nan(1,l2);
        peg_forcast_FTTM_temp=nan(1,l2);
        
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            roe_forcast_FTTM_temp(1,k)=data{j,2};
            pe_forcast_FTTM_temp(1,k)=data{j,3};
            pb_forcast_FTTM_temp(1,k)=data{j,4};
            peg_forcast_FTTM_temp(1,k)=data{j,5};
        end
        roe_forcast_FTTM(i,:)  = roe_forcast_FTTM_temp;
        pe_forcast_FTTM(i,:) = pe_forcast_FTTM_temp;
        pb_forcast_FTTM(i,:) =pb_forcast_FTTM_temp;
        peg_forcast_FTTM(i,:)  = peg_forcast_FTTM_temp;
    end
    save ('./data/daily_factor/roe_forcast_FTTM.mat' ,'roe_forcast_FTTM');
    save ('./data/daily_factor/pe_forcast_FTTM.mat' ,'pe_forcast_FTTM');
    save ('./data/daily_factor/pb_forcast_FTTM.mat' ,'pb_forcast_FTTM');
    save ('./data/daily_factor/peg_forcast_FTTM.mat' ,'peg_forcast_FTTM');
end
%% YOY
display('盈利预测YOY:')
load  './data/tdays_data.mat';
load  './data/stock.mat';

FileStr = './data/daily_factor/netprofit_forcast_YOY.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/roe_forcast_YOY.mat';
    start = size(roe_forcast_YOY,1);
else
    start = 0;
    netprofit_forcast_YOY=[];
    roe_forcast_YOY=[];
end

l1=length(tdays_data);
l2=length(stklist);
start = size(netprofit_forcast_YOY,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        
        eval(['curs=exec(conn, ''select s_info_windcode,net_profit,est_roe from  AshareConsensusRollingData   WHERE rolling_type=''''YOY'''' and est_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        netprofit_forcast_YOY_temp =nan(1,l2);
        roe_forcast_YOY_temp=nan(1,l2);
        len=size(data,1);
        if size(data,2)>1
            for  j=1:len
                k=find(strcmp(stklist,data{j,1}));
                netprofit_forcast_YOY_temp(1,k)=data{j,2};
                roe_forcast_YOY_temp(1,k)=data{j,3};
            end
        end
        netprofit_forcast_YOY(i,:)  = netprofit_forcast_YOY_temp;
        roe_forcast_YOY(i,:)  = roe_forcast_YOY_temp;
    end
    save ('./data/daily_factor/netprofit_forcast_YOY.mat' ,'netprofit_forcast_YOY');
    save ('./data/daily_factor/roe_forcast_YOY.mat' ,'roe_forcast_YOY');
end
%% CAGR
display('盈利预测CAGR:')
load  './data/tdays_data.mat';
load  './data/stock.mat';

FileStr = './data/daily_factor/netprofit_forcast_CAGR.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/roe_forcast_CAGR.mat';
    start = size(netprofit_forcast_CAGR,1);
else
    start = 0;
    netprofit_forcast_CAGR=[];
    roe_forcast_CAGR=[];
end

l1=length(tdays_data);
l2=length(stklist);
start = size(netprofit_forcast_CAGR,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        
        eval(['curs=exec(conn, ''select s_info_windcode,net_profit,est_roe from  AshareConsensusRollingData   WHERE rolling_type=''''CAGR'''' and est_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        netprofit_forcast_CAGR_temp =nan(1,l2);
        roe_forcast_CAGR_temp=nan(1,l2);
        len=size(data,1);
        if size(data,2)>1
            for  j=1:len
                k=find(strcmp(stklist,data{j,1}));
                netprofit_forcast_CAGR_temp(1,k)=data{j,2};
                roe_forcast_CAGR_temp(1,k)=data{j,3};
            end
        end
        netprofit_forcast_CAGR(i,:)  =netprofit_forcast_CAGR_temp;
        roe_forcast_CAGR(i,:)  =roe_forcast_CAGR_temp;
    end
    save ('./data/daily_factor/netprofit_forcast_CAGR.mat' ,'netprofit_forcast_CAGR');
    save ('./data/daily_factor/roe_forcast_CAGR.mat' ,'roe_forcast_CAGR');
end
%% 券商评级
display('券商评级:')
load  './data/tdays_data.mat';
load  './data/stock.mat';

FileStr = './data/daily_factor/rating_avg.mat';%综合评级
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/rating_net_upgrade.mat';%上调家数-下调家数
    load  './data/daily_factor/rating_instnum.mat';%评级机构数目
    start = size(rating_avg,1);
else
    start = 0;
    rating_avg = [];
    rating_net_upgrade=[];
    rating_instnum=[];
end

l1=length(tdays_data);
l2=length(stklist);
start = size(rating_avg,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        
        eval(['curs=exec(conn, ''select s_info_windcode,s_wrating_avg,s_wrating_upgrade,s_wrating_downgrade,s_wrating_instnum from  AShareStockRatingConsus   WHERE rating_dt= ''''', date,''''' and s_wrating_cycle=0263002000 '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        rating_avg_temp =nan(1,l2);
        rating_net_upgrade_temp =nan(1,l2);
        rating_instnum_temp =nan(1,l2);
        
        len=size(data,1);
        for  j=1:len
            k=find(strcmp(stklist,data{j,1}));
            rating_avg_temp(1,k)=data{j,2};
            rating_net_upgrade_temp(1,k)=data{j,3}-data{j,4};
            rating_instnum_temp(1,k)=data{j,5};
        end
        rating_avg(i,:)  = rating_avg_temp;
        rating_net_upgrade(i,:)  = rating_net_upgrade_temp;
        rating_instnum(i,:)  =rating_instnum_temp;
    end
    save ('./data/daily_factor/rating_avg.mat' ,'rating_avg');
    save ('./data/daily_factor/rating_net_upgrade.mat' ,'rating_net_upgrade');
    save ('./data/daily_factor/rating_instnum.mat' ,'rating_instnum');
end

%% 14. rating_avg_chg 过去20日rating_avg的变化
display('rating_avg_chg:');
FileStr = './data/daily_factor/rating_avg_chg.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/Volat.mat';
    start = size(rating_avg_chg,1);%要求rating_avg_chg和Volat矩阵大小相同
else
    start = 0;
    rating_avg_chg = [];
end

load  './data/price_forward_adjusted.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/daily_factor/rating_avg.mat';
l1=length(tdays_data);
l2=length(stock);
temp=zeros(l1,l2);
for  j=1:l2
    for  i=1:l1
        if  isnan(rating_avg(i,j))==0
            temp(i:end,j)=rating_avg(i,j);
        end
    end
end
L=20;

if l1 > start
    for  i=start+1:l1
        i
        rating_avg_chg(i,:) = NaN(1,l2);
        if i > L+1
            rating_avg_chg(i,:)=temp(i,:)./temp(i-L,:)-1;
        end
    end
    save ('./data/daily_factor/rating_avg_chg.mat' ,'rating_avg_chg');
    clear 'rating_avg_chg';
end
%% 12. res_mom(用前三年的数据回归算出残差，最近12个月的残差和（去除最近一个月），作为动量)
display('res_mom:');
FileStr = './data/daily_factor/res_mom.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(res_mom,1);
else
    start = 0;
    res_mom = [];
end
load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/market.mat';
load  './data/price_forward_adjusted.mat';

l1=length(tdays_data);
l2=length(stock);

if l1 > start
    for  i=start+1:l1
        i
        res_mom(i,:) = NaN(1,l2);%修改成nan
        if i > 756
            for  j=1:l2
                Y =price_forward_adjusted(i-755:i,j)./price_forward_adjusted(i-756:i-1,j)-1;
                if  length(find(isnan(Y)))>0
                    res_mom(i,j)=nan;
                    continue;
                end
                X1=ones(756,1);
                X2=market(i-755:i,1);
                X=[X1 X2];
                %回归计算
                [~,~,r]=regress(Y,X);
                res_mom(i,j)=sum(r(end-252:end-21));%如果修改成mean / std，或者变成复利相乘？20160217
            end
        end
    end
    save ('./data/daily_factor/res_mom.mat' ,'res_mom');
    clear  'res_mom';
end

%% 13. 反转、残差反转
display('MOM_1day:');
load  './data/price_forward_adjusted.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';
FileStr = './data/daily_factor/MOM_1day.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(MOM_1day,1);%要求MOM_1day,MOM_63day,MOM_126day矩阵大小相同
else
    start = 0;
    MOM_1day = [];
end

l1=length(tdays_data);
l2=length(stock);

if l1 > start
    for  i=start+1:l1
        i
        MOM_1day_temp = NaN(1,l2);%修改成nan
        if i > 21
            MOM_1day_temp = price_forward_adjusted(i,:)./price_forward_adjusted(i-1,:)-1;
        end
        MOM_1day(i,:) = MOM_1day_temp;
    end
    save ('./data/daily_factor/MOM_1day.mat' ,'MOM_1day');
    clear  'MOM_1day';
end
%%  SMB,HML
%分成6组，观察效果
display('SMB,HML:');
load  './data/daily_factor/MV.mat';
load  './data/daily_factor/BP.mat';
load  './data/tdays_data';
load './data/price_forward_adjusted';
load './data/stock';
FileStr = './data/SMB.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load './data/HML.mat';
    start = size(SMB,1);%要求MOM_21day,MOM_63day,MOM_126day矩阵大小相同
else
    start = 0;
    SMB= [];
    HML=[];
end

l1=length(tdays_data);
l2=length(stock);

if l1 > start
    for  i=max(2,start+1):l1
        ret=zeros(1,6);
            i
            ind=find(isnan(MV(i-1,:))==0&isnan(BP(i-1,:))==0&isnan(price_forward_adjusted(i-1,:))==0);
            t1=median(MV(i-1,ind));
            temp=sort(BP(i-1,ind),'descend');
            t2=temp(floor(length(ind)*0.3));
            t3=temp(floor(length(ind)*0.7));
            class=zeros(1,l2);%每循环一次初始化
            for  j=1:l2
                if  length(find(ind==j))<=0
                    continue;
                end
                if  BP(i-1,j)>t2
                    class(1,j)= 1;
                elseif BP(i-1,j)<=t2&&BP(i-1,j)>t3
                    class(1,j)= 2;
                else
                    class(1,j)= 3;
                end
                if  MV(i-1,j)<t1
                    class(1,j)=class(1,j);
                else
                    class(1,j)=class(1,j)+3;
                end
            end
            for  j=1:6
                temp=find(class(1,:)==j);
                ret(i,j)=mean(price_forward_adjusted(i,temp)./price_forward_adjusted(i-1,temp)-1);
            end
            SMB(i,1)=mean(ret(i,1:3))-mean(ret(i,4:6));
            HML(i,1)=(ret(i,1)+ret(i,4))/2-(ret(i,3)+ret(i,6))/2;
    end
    save ('./data/SMB.mat' ,'SMB');
    save ('./data/HML.mat' ,'HML');
    clear 'SMB','HML';
end
%% 13. 反转、残差反转
display('反转、残差反转:');
load  './data/price_forward_adjusted.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';
FileStr = './data/daily_factor/MOM_21day.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/MOM_63day.mat';
    load  './data/daily_factor/MOM_126day.mat';
    load  './data/daily_factor/res_reversal_5d_Fama_1.mat';
    load  './data/daily_factor/res_reversal_1M_Fama_1.mat';
    load  './data/daily_factor/res_reversal_3M_Fama_1.mat';
    load  './data/daily_factor/res_reversal_6M_Fama_1.mat';
    start = size(MOM_21day,1);%要求MOM_21day,MOM_63day,MOM_126day矩阵大小相同
else
    start = 0;
    MOM_21day = [];
    MOM_63day = [];
    MOM_126day = [];
    res_reversal_1M_Fama_1=[];
    res_reversal_3M_Fama_1=[];
    res_reversal_6M_Fama_1=[];
    res_reversal_5d_Fama_1=[];
end

l1=length(tdays_data);
l2=length(stock);

if l1 > start
    for  i=start+1:l1
        MOM_21day_temp = NaN(1,l2);%修改成nan
        MOM_63day_temp = NaN(1,l2);%修改成nan
        MOM_126day_temp = NaN(1,l2);%修改成nan
        if i > 126
            MOM_126day_temp = price_forward_adjusted(i,:)./price_forward_adjusted(i-126,:)-1;
        end
        if i > 21
            MOM_21day_temp = price_forward_adjusted(i,:)./price_forward_adjusted(i-21,:)-1;
        end
        if i > 63
            MOM_63day_temp = price_forward_adjusted(i,:)./price_forward_adjusted(i-63,:)-1;
        end
        MOM_21day(i,:) = MOM_21day_temp;
        MOM_63day(i,:) = MOM_63day_temp;
        MOM_126day(i,:) = MOM_126day_temp;
    end
    save ('./data/daily_factor/MOM_21day.mat' ,'MOM_21day');
    save ('./data/daily_factor/MOM_63day.mat' ,'MOM_63day');
    save ('./data/daily_factor/MOM_126day.mat' ,'MOM_126day');
    clear 'MOM_126day' 'MOM_63day'  'MOM_21day';
    
    load  './data/market.mat';
    load './data/SMB.mat';
    load './data/HML.mat'
    for  i=max(start,756)+1:l1
        i
        for  j=1:l2
            Y =price_forward_adjusted(i-755:i,j)./price_forward_adjusted(i-756:i-1,j)-1;
            if  length(find(isnan(Y)))>0
                res_reversal_1M_Fama_1(i,j)=nan;
                res_reversal_3M_Fama_1(i,j)=nan;
                res_reversal_6M_Fama_1(i,j)=nan;
                res_reversal_5d_Fama_1(i,j)=nan;
                continue;
            end
            X1=ones(756,1);
            X2=market(i-755:i,1);
            X3=SMB(i-755:i,1);
            X4=HML(i-755:i,1);
            X=[X1 X2 X3 X4];
            %回归计算
            B=regress(Y,X);
            temp=Y-X*B;
            res_reversal_5d_Fama_1(i,j)=sum(temp(end-5:end))/std(temp(end-5:end));
            res_reversal_1M_Fama_1(i,j)=sum(temp(end-21:end))/std(temp(end-21:end));
            res_reversal_3M_Fama_1(i,j)=sum(temp(end-63:end))/std(temp(end-63:end));
            res_reversal_6M_Fama_1(i,j)=sum(temp(end-126:end))/std(temp(end-126:end));
        end
    end
    save ('./data/daily_factor/res_reversal_5d_Fama_1.mat' ,'res_reversal_5d_Fama_1');
    save ('./data/daily_factor/res_reversal_1M_Fama_1.mat' ,'res_reversal_1M_Fama_1');
    save ('./data/daily_factor/res_reversal_3M_Fama_1.mat' ,'res_reversal_3M_Fama_1');
    save ('./data/daily_factor/res_reversal_6M_Fama_1.mat' ,'res_reversal_6M_Fama_1');
    clear 'res_reversal_5d_Fama_1' 'res_reversal_1M_Fama_1'  'res_reversal_3M_Fama_1' 'res_reversal_6M_Fama_1';
end
%% 14. SKEW以及波动率(过去21天涨跌幅的标准差)
display('SKEW:');
FileStr = './data/daily_factor/SKEW.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/Volat.mat';
    start = size(SKEW,1);%要求SKEW和Volat矩阵大小相同
else
    start = 0;
    SKEW = [];
    Volat = [];
end

load  './data/price_forward_adjusted.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';

l1=length(tdays_data);
l2=length(stock);

if l1 > start
    for  i=start+1:l1
        i
        SKEW(i,:) = NaN(1,l2);
        Volat(i,:) = NaN(1,l2);
        if i > 252
            SKEW(i,:)=skewness(price_forward_adjusted(i-251:i,:)./price_forward_adjusted(i-252:i-1,:)-1);
        end
        if i > 21
            Volat(i,:)=std(price_forward_adjusted(i-21+1:i,:)./price_forward_adjusted(i-21:i-1,:)-1);
        end
    end
    save ('./data/daily_factor/SKEW.mat' ,'SKEW');
    save ('./data/daily_factor/Volat.mat' ,'Volat');
    clear 'SKEW' 'Volat';
end
%% 15.换手率,成交量,成交额(过去21天日度的均值)
display('TR:');
load  './data/volume.mat';
load  './data/amount.mat';
load  './data/TR.mat';
FileStr = './data/daily_factor/volume_1M.mat';

load  './data/tdays_data.mat';
load  './data/stock.mat';

if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/Amount_1M.mat';
    load  './data/daily_factor/TR_20d.mat';
    load  './data/daily_factor/TR_5d.mat';
    load  './data/daily_factor/res_TR_5d.mat';
    load  './data/daily_factor/res_TR_20d.mat';
    start = size(volume_1M,1);
else
    start = 0;
    volume_1M = [];
    Amount_1M = [];
    TR_20d = [];
    TR_5d=[];
    res_TR_5d=[];
    res_TR_20d=[];
end

l1=length(tdays_data);
l2=length(stock);

len1=20;
len2=5;

if l1 > start
    load  './data/daily_factor/MV.mat';
    load  './data/A_ST_stock_d.mat';
    for  i=start+1:l1
        i
        TR_20d(i,:) = NaN(1,l2);
        volume_1M(i,:)=NaN(1,l2);
        Amount_1M(i,:)=NaN(1,l2);
        TR_5d(i,:)=NaN(1,l2);
        res_TR_5d(i,:)=NaN(1,l2);
        res_TR_20d(i,:)=NaN(1,l2);
        if i >= len1
            volume_1M(i,:)=mean(volume(i-len1+1:i,:));
            Amount_1M(i,:)=mean(amount(i-len1+1:i,:));
            TR_20d(i,:)=mean(TR(i-len1+1:i,:));
            TR_5d(i,:)=mean(TR(i-len2+1:i,:));
            
            ind=find(A_ST_stock_d(i,:)==1&TR_5d(i,:)>0);
            X1=ones(length(ind),1);
            X2=log(MV(i,ind)');
            X=[X1 X2 ];
            Y=log(TR_5d(i,ind)');
            B=regress(Y,X);
            res_TR_5d(i,ind)=log(TR_5d(i,ind))-(X*B)';
            
            ind=find(A_ST_stock_d(i,:)==1&TR_20d(i,:)>0);
            X1=ones(length(ind),1);
            X2=log(MV(i,ind)');
            X=[X1 X2 ];
            Y=log(TR_20d(i,ind)');
            B=regress(Y,X);
            res_TR_20d(i,ind)=log(TR_20d(i,ind))-(X*B)';
        end
    end
    save ('./data/daily_factor/TR_20d.mat' ,'TR_20d');
    save ('./data/daily_factor/Amount_1M.mat' ,'Amount_1M');
    save ('./data/daily_factor/volume_1M.mat' ,'volume_1M');
    save ('./data/daily_factor/TR_5d.mat' ,'TR_5d');
    save ('./data/daily_factor/res_TR_5d.mat' ,'res_TR_5d');
    save ('./data/daily_factor/res_TR_20d.mat' ,'res_TR_20d');
    clear 'TR_20d'  'Amount_1M'  'volume_1M' 'TR_5d'  'res_TR_5d' 'res_TR_20d';
end
%%  16. 行业中性BP,EP,SP,CFP,FCFEP,FCFFP
display('BP_ind_neutral:');
load  './data/daily_factor/BP.mat';
load  './data/daily_factor/EP.mat';
load  './data/daily_factor/SP.mat';
load  './data/daily_factor/CFP.mat';

FileStr =  './data/daily_factor/BP_ind_neutral.mat';

load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/ind_of_stock.mat';

if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/EP_ind_neutral.mat';
    load  './data/daily_factor/SP_ind_neutral.mat';
    load  './data/daily_factor/CFP_ind_neutral.mat';
    start = size(BP_ind_neutral,1);%要求几个中性的因子矩阵大小相同
else
    start = 0;
    BP_ind_neutral= [];
    EP_ind_neutral = [];
    SP_ind_neutral= [];
    CFP_ind_neutral=[];
end
l1=length(tdays_data);
l2=length(stock);

if l1 > start
    for  i=start+1:l1
        i
        for  j=1:l2%可以优化成按照行业数量来循环
            temp=BP(i,find(ind_of_stock(i,:)==ind_of_stock(i,j)));
            BP_ind_neutral(i,j)=BP(i,j)-mean(temp(find(isnan(temp)==0)));
            temp=EP(i,find(ind_of_stock(i,:)==ind_of_stock(i,j)));
            EP_ind_neutral(i,j)=EP(i,j)-mean(temp(find(isnan(temp)==0)));
            temp=SP(i,find(ind_of_stock(i,:)==ind_of_stock(i,j)));
            SP_ind_neutral(i,j)=SP(i,j)-mean(temp(find(isnan(temp)==0)));
            temp=CFP(i,find(ind_of_stock(i,:)==ind_of_stock(i,j)));
            CFP_ind_neutral(i,j)=CFP(i,j)-mean(temp(find(isnan(temp)==0)));
        end
    end
    save ('./data/daily_factor/BP_ind_neutral.mat' ,'BP_ind_neutral');
    save ('./data/daily_factor/EP_ind_neutral.mat' ,'EP_ind_neutral');
    save ('./data/daily_factor/SP_ind_neutral.mat' ,'SP_ind_neutral');
    save ('./data/daily_factor/CFP_ind_neutral.mat' ,'CFP_ind_neutral');
    clear 'BP_ind_neutral' 'EP_ind_neutral'  'SP_ind_neutral'  'CFP_ind_neutral';
end
%% 17. res_vol_Fama 特质波动率、res_vol_Fama_ratio 特异度
display('res_vol_Fama:');
load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/market.mat';
load  './data/daily_factor/BP.mat';
load  './data/daily_factor/MV.mat';
load  './data/price_forward_adjusted.mat';
load  './data/stock_trade_able.mat';

FileStr = './data/daily_factor/res_vol_Fama.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load './data/daily_factor/res_vol_Fama_ratio.mat';
    start = size(res_vol_Fama,1);
else
    start = 0;
    res_vol_Fama = [];
    res_vol_Fama_ratio=[];
end

l1=length(tdays_data);
l2=length(stock);

if l1 > start
    load './data/SMB.mat';
    load './data/HML.mat'
    lookback=21;
    for  i=start+1:l1
        i
        res_vol_Fama(i,:) = NaN(1,l2);%初始化为nan
        res_vol_Fama_ratio(i,:) = NaN(1,l2);%初始化为nan
        if i > lookback
            for  j=1:l2
                Y =price_forward_adjusted(i-lookback+1:i,j)./price_forward_adjusted(i-lookback:i-1,j)-1;
                if  length(find(isnan(Y)))>0
                    res_vol_Fama(i,j)=nan;
                    res_vol_Fama_ratio(i,j)=nan;
                    continue;
                end
                X1=ones(lookback,1);
                X2=market(i-lookback+1:i,1);
                X3=SMB(i-lookback+1:i,1);
                X4=HML(i-lookback+1:i,1);
                X=[X1 X2 X3 X4];
                %回归计算
                [~,~,r,~,STATS]=regress(Y,X);
                res_vol_Fama(i,j)=std(r);
                res_vol_Fama_ratio(i,j)=1-STATS(1);
            end
        end
    end
    save ('./data/daily_factor/res_vol_Fama.mat' ,'res_vol_Fama');
    save ('./data/daily_factor/res_vol_Fama_ratio.mat' ,'res_vol_Fama_ratio');
    clear 'res_vol_Fama' 'res_vol_Fama_ratio';
end
%% SpreadBias: 参考东方报告
display('SpreadBias:');
FileStr = './data/daily_factor/SpreadBias.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(SpreadBias,1);
else
    start = 0;
    SpreadBias = [];
end
load  './data/price_forward_adjusted.mat';
load  './data/A_ST_stock_d.mat';
[l1,l2]=size(price_forward_adjusted);
L1=250;
L2=30;

if l1 > start
    reference_price=zeros(l1,l2);
    for  i=max(start+1,L1+1):l1-1
        i
        ind=find(A_ST_stock_d(i,:)==1 );
        temp=price_forward_adjusted(i-L1+1:i,ind)./price_forward_adjusted(i-L1:i-1,ind)-1;
        dis=1-corr(temp);
        len=length(ind);
        for  j=1:len
            [Y,I]=sort(dis(:,j));
            if length(find(isnan(Y(2:11))))==0%若是NaN，说明该股票没有价格
                reference_price(i+1,ind(j))=mean(price_forward_adjusted(i+1,ind(I(2:11)))./price_forward_adjusted(i,ind(I(2:11)))-1);
            end
        end
    end
    PriceSpread=log(price_forward_adjusted)-log(cumprod(1+reference_price));
    for  i=max(start+1,L1+1):l1
        SpreadBias(i,:)=(PriceSpread(i,:)-mean(PriceSpread(i-L2+1:i,:)))./std(PriceSpread(i-L2+1:i,:));
    end
    save ('./data/daily_factor/SpreadBias.mat' ,'SpreadBias');
    clear 'SpreadBias' ;
end

%% value_diff_small_trader_act
%数据库提取
display('value_diff_small_trader_act:');
load  './data/tdays_data.mat';
load  './data/stock.mat';
FileStr = './data/daily_factor/value_diff_small_trader_act.mat';
% FileStr = './data/daily_factor/value_diff_large_trader_act.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/value_diff_med_trader_act.mat';
    load  './data/daily_factor/value_diff_large_trader_act.mat';
    load  './data/daily_factor/value_diff_institute_act.mat';
    
    load  './data/daily_factor/volume_diff_small_trader_act.mat';
    load  './data/daily_factor/volume_diff_med_trader_act.mat';
    load  './data/daily_factor/volume_diff_large_trader_act.mat';
    load  './data/daily_factor/volume_diff_institute_act.mat';
else
    value_diff_small_trader_act= [];
    value_diff_med_trader_act= [];
    value_diff_large_trader_act= [];
    value_diff_institute_act= [];
    
    volume_diff_small_trader_act= [];
    volume_diff_med_trader_act= [];
    volume_diff_large_trader_act= [];
    volume_diff_institute_act= [];
end
l1=length(tdays_data);
start = size(value_diff_small_trader_act,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        eval(['curs=exec(conn, ''select s_info_windcode,value_diff_small_trader_act,value_diff_med_trader_act,value_diff_large_trader_act,value_diff_institute_act,volume_diff_small_trader_act,volume_diff_med_trader_act,volume_diff_large_trader_act,volume_diff_institute_act  from  AShareMoneyflow   WHERE trade_dt= ''''', date,''''' '');']);
        %         eval(['curs=exec(conn, ''select s_info_windcode,value_diff_large_trader_act from  AShareMoneyflow   WHERE trade_dt= ''''', date,''''' '');']);
        
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data') 
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        value_diff_small_trader_act_temp =nan(1,size(stklist,1));
        value_diff_med_trader_act_temp =nan(1,size(stklist,1));
        value_diff_large_trader_act_temp =nan(1,size(stklist,1));
        value_diff_institute_act_temp =nan(1,size(stklist,1));
        
        volume_diff_small_trader_act_temp =nan(1,size(stklist,1));
        volume_diff_med_trader_act_temp =nan(1,size(stklist,1));
        volume_diff_large_trader_act_temp =nan(1,size(stklist,1));
        volume_diff_institute_act_temp =nan(1,size(stklist,1));
        len=size(data,1);
        if  len>1
            for  j=1:len
                k=find(strcmp(stklist,data{j,1}));
                value_diff_small_trader_act_temp(1,k)=data{j,2};
                value_diff_med_trader_act_temp(1,k)=data{j,3};
                value_diff_large_trader_act_temp(1,k)=data{j,4};
                value_diff_institute_act_temp(1,k)=data{j,5};
                
                volume_diff_small_trader_act_temp(1,k)=data{j,6};
                volume_diff_med_trader_act_temp(1,k)=data{j,7};
                volume_diff_large_trader_act_temp(1,k)=data{j,8};
                volume_diff_institute_act_temp(1,k)=data{j,9};
            end
        end
        
        value_diff_small_trader_act(i,:) = value_diff_small_trader_act_temp;
        value_diff_med_trader_act(i,:) = value_diff_med_trader_act_temp;
        value_diff_large_trader_act(i,:) = value_diff_large_trader_act_temp;
        value_diff_institute_act(i,:) = value_diff_institute_act_temp;
        
        volume_diff_small_trader_act(i,:) = volume_diff_small_trader_act_temp;
        volume_diff_med_trader_act(i,:) = volume_diff_med_trader_act_temp;
        volume_diff_large_trader_act(i,:) = volume_diff_large_trader_act_temp;
        volume_diff_institute_act(i,:) = volume_diff_institute_act_temp;
    end
    save ('./data/daily_factor/value_diff_small_trader_act.mat' ,'value_diff_small_trader_act');
    save ('./data/daily_factor/value_diff_med_trader_act.mat' ,'value_diff_med_trader_act');
    save ('./data/daily_factor/value_diff_large_trader_act.mat' ,'value_diff_large_trader_act');
    save ('./data/daily_factor/value_diff_institute_act.mat' ,'value_diff_institute_act');
    
    save ('./data/daily_factor/volume_diff_small_trader_act.mat' ,'volume_diff_small_trader_act');
    save ('./data/daily_factor/volume_diff_med_trader_act.mat' ,'volume_diff_med_trader_act');
    save ('./data/daily_factor/volume_diff_large_trader_act.mat' ,'volume_diff_large_trader_act');
    save ('./data/daily_factor/volume_diff_institute_act.mat' ,'volume_diff_institute_act');
end


%% 计算因子
FileStr = './data/daily_factor/value_diff_large_trader_act_10d.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/volume.mat';
load  './data/amount.mat';

if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/value_diff_large_trader_act_10d.mat';
    start = size(value_diff_large_trader_act_10d,1);
else
    start = 0;
    value_diff_large_trader_act_10d=[];
end

l1=length(tdays_data);
l2=length(stock);

len1=10;

if l1 > start
    load  './data/daily_factor/value_diff_large_trader_act.mat';
    load ./data/low_forward_adjusted;
    load ./data/high_forward_adjusted;
    ind=find(high_forward_adjusted==low_forward_adjusted);
    value_diff_large_trader_act(ind)=NaN;
    for  i=start+1:l1
        i
        value_diff_large_trader_act_10d(i,:) = NaN(1,l2);
        if i >= len1
            i
            for  j=1:l2
                temp=value_diff_large_trader_act(i-len1+1:i,j);
                if  length(find(isnan(temp)))>floor(len1/2)
                    value_diff_large_trader_act_10d(i,j)=nan;
                else
                    value_diff_large_trader_act_10d(i,j)=sum(temp(~isnan(temp)))/sum(amount(i-len1+find(~isnan(temp)),j));
                end
            end
        end
    end
    save ('./data/daily_factor/value_diff_large_trader_act_10d.mat' ,'value_diff_large_trader_act_10d');
end
%% 计算因子(20d)
FileStr = './data/daily_factor/value_diff_large_trader_act_20d.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/volume.mat';
load  './data/amount.mat';
load  './data/daily_factor/value_diff_large_trader_act.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/value_diff_large_trader_act_20d.mat';
    start = size(value_diff_large_trader_act_20d,1);
else
    start = 0;
    value_diff_large_trader_act_20d=[];
end

l1=length(tdays_data);
l2=length(stock);

len1=20;

if l1 > start
    load  './data/daily_factor/value_diff_large_trader_act.mat';
    for  i=start+1:l1
        i
        value_diff_large_trader_act_20d(i,:) = NaN(1,l2);
        if i >= len1
            i
            value_diff_large_trader_act_20d(i,:) = sum(value_diff_large_trader_act(i-len1+1:i,:))./sum(amount(i-len1+1:i,:));
        end
    end
    save ('./data/daily_factor/value_diff_large_trader_act_20d.mat' ,'value_diff_large_trader_act_20d');
end

%% value_diff_large_institute_trader_act_10d
FileStr = './data/daily_factor/value_diff_large_institute_trader_act_10d.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/volume.mat';
load  './data/amount.mat';
load  './data/daily_factor/value_diff_large_trader_act.mat';
load  './data/daily_factor/value_diff_institute_act.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/value_diff_large_institute_trader_act_10d.mat';
    start = size(value_diff_large_institute_trader_act_10d,1);
else
    start = 0;
    value_diff_large_institute_trader_act_10d=[];
end

l1=length(tdays_data);
l2=length(stock);

len1=10;

if l1 > start
    for  i=start+1:l1
        i
        value_diff_large_institute_trader_act_10d(i,:) = NaN(1,l2);
        if i >= len1
            i
            value_diff_large_institute_trader_act_10d(i,:) = (sum(value_diff_large_trader_act(i-len1+1:i,:))+sum(value_diff_institute_act(i-len1+1:i,:)))./sum(amount(i-len1+1:i,:));
        end
    end
    save ('./data/daily_factor/value_diff_large_institute_trader_act_10d.mat' ,'value_diff_large_institute_trader_act_10d');
end
%% value_diff_large_institute_trader_act_20d
FileStr = './data/daily_factor/value_diff_large_institute_trader_act_20d.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';
load  './data/volume.mat';
load  './data/amount.mat';
load  './data/daily_factor/value_diff_large_trader_act.mat';
load  './data/daily_factor/value_diff_institute_act.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/value_diff_large_institute_trader_act_20d.mat';
    start = size(value_diff_large_institute_trader_act_20d,1);
else
    start = 0;
    value_diff_large_institute_trader_act_20d=[];
end

l1=length(tdays_data);
l2=length(stock);

len1=20;

if l1 > start
    for  i=start+1:l1
        i
        value_diff_large_institute_trader_act_20d(i,:) = NaN(1,l2);
        if i >= len1
            i
            value_diff_large_institute_trader_act_20d(i,:) = (sum(value_diff_large_trader_act(i-len1+1:i,:))+sum(value_diff_institute_act(i-len1+1:i,:)))./sum(amount(i-len1+1:i,:));
        end
    end
    save ('./data/daily_factor/value_diff_large_institute_trader_act_20d.mat' ,'value_diff_large_institute_trader_act_20d');
end
%% moneyflow_pct_volume
display('moneyflow_pct_volume:');
%数据库提取
load  './data/tdays_data.mat';
load  './data/stock.mat';
FileStr = './data/daily_factor/moneyflow_pct_volume.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/daily_factor/moneyflow_pct_value.mat';
else
    moneyflow_pct_volume= [];
    moneyflow_pct_value= [];
    
end
l1=length(tdays_data);
start = size(moneyflow_pct_volume,1);

if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        eval(['curs=exec(conn, ''select s_info_windcode,moneyflow_pct_volume,moneyflow_pct_value  from  AShareMoneyflow   WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if strcmp(data,'no data')
            error([FileStr, '出问题']);
        end
        if size(data,1) == 1
            error([FileStr, '出问题']);
        end
        moneyflow_pct_volume_temp =nan(1,size(stklist,1));
        moneyflow_pct_value_temp =nan(1,size(stklist,1));
        len=size(data,1);
        if  len>1
            for  j=1:len
                k=find(strcmp(stklist,data{j,1}));
                moneyflow_pct_volume_temp(1,k)=data{j,2};
                moneyflow_pct_value_temp(1,k)=data{j,3};
            end
        end
        
        moneyflow_pct_volume(i,:) = moneyflow_pct_volume_temp;
        moneyflow_pct_value(i,:) = moneyflow_pct_value_temp;
    end
    save ('./data/daily_factor/moneyflow_pct_volume.mat' ,'moneyflow_pct_volume');
    save ('./data/daily_factor/moneyflow_pct_value.mat' ,'moneyflow_pct_value');
end

%% 计算因子
load  './data/daily_factor/moneyflow_pct_volume.mat';
load  './data/daily_factor/moneyflow_pct_value.mat';

FileStr = './data/daily_factor/moneyflow_pct_volume_10d.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';

if exist(FileStr, 'file') == 2
    load  './data/daily_factor/moneyflow_pct_volume_10d.mat';
    load  './data/daily_factor/moneyflow_pct_value_10d.mat';
    start = size(moneyflow_pct_volume_10d,1);
else
    start = 0;
    moneyflow_pct_volume_10d=[];
    moneyflow_pct_value_10d=[];
end
l1=length(tdays_data);
l2=length(stock);
len1=10;

if l1 > start
    for  i=start+1:l1
        i
        moneyflow_pct_volume_10d(i,:) = NaN(1,l2);
        moneyflow_pct_value_10d(i,:) = NaN(1,l2);
        if i >= len1
            moneyflow_pct_volume_10d(i,:) = mean(moneyflow_pct_volume(i-len1+1:i,:));
            moneyflow_pct_value_10d(i,:) = mean(moneyflow_pct_value(i-len1+1:i,:));
        end
    end
    save ('./data/daily_factor/moneyflow_pct_volume_10d.mat' ,'moneyflow_pct_volume_10d');
    save ('./data/daily_factor/moneyflow_pct_value_10d.mat' ,'moneyflow_pct_value_10d');
end

%% 计算因子(20d)
load  './data/daily_factor/moneyflow_pct_volume.mat';
load  './data/daily_factor/moneyflow_pct_value.mat';

FileStr = './data/daily_factor/moneyflow_pct_volume_20d.mat';
load  './data/tdays_data.mat';
load  './data/stock.mat';

if exist(FileStr, 'file') == 2
    load  './data/daily_factor/moneyflow_pct_volume_20d.mat';
    load  './data/daily_factor/moneyflow_pct_value_20d.mat';
    start = size(moneyflow_pct_volume_20d,1);
else
    start = 0;
    moneyflow_pct_volume_20d=[];
    moneyflow_pct_value_20d=[];
end
l1=length(tdays_data);
l2=length(stock);
len1=20;

if l1 > start
    for  i=start+1:l1
        i
        moneyflow_pct_volume_20d(i,:) = NaN(1,l2);
        moneyflow_pct_value_20d(i,:) = NaN(1,l2);
        if i >= len1
            moneyflow_pct_volume_20d(i,:) = mean(moneyflow_pct_volume(i-len1+1:i,:));
            moneyflow_pct_value_20d(i,:) = mean(moneyflow_pct_value(i-len1+1:i,:));
        end
    end
    save ('./data/daily_factor/moneyflow_pct_volume_20d.mat' ,'moneyflow_pct_volume_20d');
    save ('./data/daily_factor/moneyflow_pct_value_20d.mat' ,'moneyflow_pct_value_20d');
end
% %% auction_amount_ratio(有问题，存在负数)
% %数据库提取
% load  './data/tdays_data.mat';
% load  './data/stock.mat';
% load  './data/amount.mat';
% FileStr = './data/daily_factor/auction_amount_ratio.mat';
% if exist(FileStr, 'file') == 2
%     load(FileStr);
% else
%     auction_amount_ratio=[];
% end
% l1=length(tdays_data);
% start = size(auction_amount_ratio,1);
% 
% if l1 > start
%     %深圳的股票
%     ind=[];
%     for  i=1:l2
%         if  str2num(stklist{i}(1))~=6
%             ind=[ind;i];
%         end
%     end
%     for  i=start+1:l1
%         i
%         date=datestr(tdays_data(i),'yyyymmdd');
%         eval(['curs=exec(conn, ''select s_info_windcode,s_li_initiativebuyrate,s_li_initiativesellrate from  AShareL2Indicators   WHERE trade_dt= ''''', date,''''' '');']);
%         
%         curs=fetch(curs);
%         data=curs.Data;
%         temp =nan(1,size(stklist,1));
%         len=size(data,1);
%         if  len>1
%             for  j=1:len
%                 k=find(strcmp(stklist,data{j,1}));
%                 temp(1,k)=sum(cell2mat(data(j,2:end)));
%             end
%         end
%         temp=(amount(i,:)-temp*1e4/2)./amount(i,:);
%         for  j=1:length(ind)
%             temp(:,j)=temp(:,j)*10/13;
%         end
%         auction_amount_ratio = [auction_amount_ratio;temp];
%     end
%     save ('./data/daily_factor/auction_amount_ratio.mat' ,'auction_amount_ratio');
% end
% %% 计算因子
% load  './data/daily_factor/auction_amount_ratio.mat';
% FileStr = './data/daily_factor/auction_amount_ratio_5d.mat';
% load  './data/tdays_data.mat';
% load  './data/stock.mat';
% 
% if exist(FileStr, 'file') == 2
%     load  './data/daily_factor/auction_amount_ratio_5d.mat';
%     load  './data/daily_factor/auction_amount_ratio_20d.mat';
%     start = size(auction_amount_ratio_5d,1);
% else
%     start = 0;
%     auction_amount_ratio_5d=[];
%     auction_amount_ratio_20d=[];
% end
% l1=length(tdays_data);
% l2=length(stock);
% len1=5;
% len2=20;
% if l1 > start
%     for  i=max(len2+1,start+1):l1
%         i
%         auction_amount_ratio_5d(i,:) = NaN(1,l2);
%         auction_amount_ratio_20d(i,:) = NaN(1,l2);
%         if i >= len1
%             auction_amount_ratio_5d(i,:) = mean(auction_amount_ratio(i-len1+1:i,:));
%             auction_amount_ratio_20d(i,:) = mean(auction_amount_ratio(i-len2+1:i,:));
%         end
%     end
%     save ('./data/daily_factor/auction_amount_ratio_5d.mat' ,'auction_amount_ratio_5d');
%     save ('./data/daily_factor/auction_amount_ratio_20d.mat' ,'auction_amount_ratio_20d');
% end

%% 指数成分股权重下载(从2008年有数据，下标726)
display('weight:');
load  './data/tdays_data.mat';
load  './data/stock.mat';
l1=length(tdays_data);
l2=length(stock);

FileStr = './data/ZZ500_weight.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    load  './data/HS300_weight.mat';
    start = size(ZZ500_weight,1);
else
    start = 0;
    HS300_weight = zeros(1,l2);
    ZZ500_weight= zeros(1,l2);
end


if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data{i,1},'yyyymmdd');
        eval(['curs=exec(conn, ''select s_con_windcode,i_weight FROM AINDEXHS300CloseWeight WHERE  TRADE_DT = ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if  ~(strcmp(data,'No Data'))
            len=size(data,1);
            for  j=1:len
                k=find(strcmp(stklist,data{j,1}));
                HS300_weight(i,k)=data{j,2}/100;%万德默认的百分数
            end
        else
            HS300_weight(i,:)=NaN;
        end
        %
        eval(['curs=exec(conn, ''select s_con_windcode,weight FROM AINDEXCSI500Weight WHERE  TRADE_DT = ''''', date, '''''  '');']);
        curs=fetch(curs);
        data=curs.Data;
        if  ~(strcmp(data,'No Data'))
            len=size(data,1);
            for  j=1:len
                k=find(strcmp(stklist,data{j,1}));
                ZZ500_weight(i,k)=data{j,2}/100;%万德默认的百分数
            end
        else
            ZZ500_weight(i,:)=NaN;
        end
    end
    save ('./data/HS300_weight.mat' ,'HS300_weight');
    save ('./data/ZZ500_weight.mat' ,'ZZ500_weight');
    clear 'HS300_weight'  'ZZ500_weight';
end
%% 上证50权重

load  './data/tdays_data.mat';
load  './data/stock.mat';
l1=length(tdays_data);
l2=length(stock);

FileStr = './data/SZ50_weight.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(SZ50_weight,1);
else
    start = 0;
    SZ50_weight = zeros(1,l2);
end


if l1 > start
    for  i=start+1:l1
        i
        date=datestr(tdays_data{i,1},'yyyymmdd');
        eval(['curs=exec(conn, ''select s_con_windcode,weight FROM AIndexSSE50Weight WHERE  TRADE_DT = ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        if  ~(strcmp(data,'No Data'))
            len=size(data,1);
            for  j=1:len
                k=find(strcmp(stklist,data{j,1}));
                SZ50_weight(i,k)=data{j,2}/100;%万德默认的百分数
            end
        else
            SZ50_weight(i,:)=NaN;
        end
        save ('./data/SZ50_weight.mat' ,'SZ50_weight');
    end
    clear 'SZ50_weight'  ;
end
% %% 全A非ST股票除去沪深300、中证500，等权指数
% load  './data/tdays_data.mat';
% load  './data/stock.mat';
% load './data/ZZ500_weight.mat';
% load './data/HS300_weight.mat';
% load './data/price_forward_adjusted.mat';
% load './data/A_ST_stock_d.mat';
%
% small_stock_weight=zeros(size(HS300_weight));
% l1=length(tdays_data);
% l2=length(stock);
% small_stock_daily_ret=zeros(l1,1);
% for  i=726:l1
%     ind=find(A_ST_stock_d(i,:)==1&HS300_weight(i,:)==0&ZZ500_weight(i,:)==0);
%     small_stock_weight(i,ind)=1/length(ind);
%     temp=price_forward_adjusted(i,ind)./price_forward_adjusted(i-1,ind)-1;
%     small_stock_daily_ret(i,1)=mean(temp(find(isnan(temp)==0)));
% end
% save('./data/small_stock_weight.mat','small_stock_weight');
% save('./data/small_stock_daily_ret.mat','small_stock_daily_ret');
% %%
%
%
%
%
% %% 2. 更新股票代码
% display(' 最新股票下载')
% stock_new = w.wset('SectorConstituent',['date=' end_date ';sector=全部A股']);
% stock_new =[stock_new; w.wset('SectorConstituent',['date=' end_date ';sector=已摘牌股票'])];
% stklist_new = stock_new(:,2);
% stklist_str = char(stklist_new);
% stklist_fir = cellstr(stklist_str(:,1));%首个字符，为了数组使用strcmp，转化为cell
% flag = strcmp(stklist_fir,'T') | strcmp(stklist_fir,'2') | ...
%     strcmp(stklist_fir,'9');
% stock_new = stock_new(~flag,:);
% stklist_new = stklist_new(~flag);
% %%
% FileStr = './新股加入/stock.mat';
% load stock.mat;
% index = setdiff(stklist_new,stklist);
% if ~ieempty(index)
%     stklist_new_add = stklist_new(index);%新添加的股票列表
%     stock_new_add = stock_new(index,:);
%     % 执行“新股加入”目录下的代码，更新相关因子数据
%     stock=[stock;stock_new_add];
%     stklist=[stklist;stklist_new_add];
%     save('stock.mat','stock','stklist');
% end
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 11.上市时间
load  './data/stock.mat';
load  './data/tdays_data.mat';
FileStr = './data/listdate.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(listdate,2);
else
    start = 0;
    listdate = [];
end

l2=length(stock);
if l2 > start
    %         tic
    eval(['curs=exec(conn, ''select s_info_windcode,s_info_listdate from  AShareDescription '');']);
    curs=fetch(curs);
    data=curs.Data;
    %         toc
        
    data_temp=cell(1,l2);
    len=size(data,1);
    if len>1
        for  j=1:len
            j
            k=find(strcmp(stklist,data{j,1}));
            if length(k)==1
                data_temp{1,k}=data{j,2};
            end
        end
    end

    listdate = data_temp;
    save(FileStr,'listdate');
    clear start l1 l2 i date data data_temp len j listdate;   
end
%% 12.是否次新股（0为是，1为不是）
load  './data/stock.mat';
load  './data/tdays_data.mat';
load  './data/listdate.mat';
FileStr = './data/new_stock.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(new_stock,1);
else
    start = 0;
    new_stock = [];
end

l1=length(tdays_data);
if l1 > start
    l2=length(stock);
    listyear=datenum(listdate,'yyyymmdd')+356;
    list=datenum(listdate,'yyyymmdd');
    for  i=start+1:l1
        i
        date=datenum(tdays_data(i),'yyyy/mm/dd');
        data_temp=NaN(1,l2);
        data_temp(listyear>date&list<=date)=0;
        data_temp(listyear<=date)=1;
        new_stock = [new_stock;data_temp];
    end
    save(FileStr,'new_stock');
    clear start l1 l2 i date data data_temp len j new_stock;
end


%% 提取中信一级行业指数(29个)
load  './data/tdays_data.mat';
load  './data/ind_code_name_CITIC_29.mat';
FileStr = './data/indIndex_CITIC_29.mat';
if exist(FileStr, 'file') == 2
    load(FileStr);
    start = size(indIndex_CITIC_29,1);
else
    start = 0;
    indIndex_CITIC_29 = [];
end

l1=length(tdays_data);
if l1 > start
    l2=size(ind_code_name_CITIC_29,1)
    
    for  i=start+1:l1
        i
        date=datestr(tdays_data(i),'yyyymmdd');
        %         tic
        eval(['curs=exec(conn, ''select s_info_windcode,S_DQ_CLOSE from AIndexIndustriesEODCITICS    WHERE trade_dt= ''''', date,''''' '');']);
        curs=fetch(curs);
        data=curs.Data;
        close(curs)
        %         toc
        
        data_temp=NaN(1,l2);
        len=size(data,1);
        if len>1
            [temp,tempindex]=ismember(ind_code_name_CITIC_29(:,1),data(:,1));
            data_temp(1,temp>0)=cell2mat(data(tempindex(temp>0),2)');
        end

        indIndex_CITIC_29 = [indIndex_CITIC_29;data_temp];
    end
    save(FileStr,'indIndex_CITIC_29');
    clear start l1 l2 i date data data_temp len j ind_code_name_CITIC_29 temp;
end
