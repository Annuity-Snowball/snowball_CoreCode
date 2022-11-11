from datetime import date, timedelta
import pymysql
import copy
import datetime as dt
from pandas.tseries.offsets import *
from pandas_datareader import data as pdr
import pandas as pd
import asyncio

from getDatainfo import getDailyDateInfo, getPayInDateInfo, getRebalanceDateInfo, getYearlyDateInfo

db = pymysql.connect(host='localhost', port=3306, user='snowball_test', passwd='909012', db='snowball_core', charset='utf8') 
snowball=db.cursor()

class Portfolio():
    def __init__(self,portfolio_id, portfolio_name, start_money, strategy_ratio, start_time, end_time, rebalance_cycle, input_type, input_money):
        self.portfolio_id=portfolio_id
        self.start_money = start_money
        self.portfolio_name = portfolio_name
        self.strategy_ratio= strategy_ratio
        self.start_time=start_time
        self.end_time=end_time
        self.rebalance_cycle = rebalance_cycle
        self.input_type = input_type
        self.input_money = input_money
        self.portfolio_account_without_tax_benefit = None
        self.portfolio_receive_without_tax_benefit = None
        self.portfolio_account_with_tax_benefit = None
        self.portfolio_receive_with_tax_benefit = None
        pass
    
    async def get_returns(self,df):
        df['return'] = (df['value'] - df['seed']) / df['seed'] 
        df['return'] = ((df['return'].round(4))*100).round(2)
    
    async def monthlyReturn(self,dict_realvalue):
        realValue = dict()
        rtDict = dict()
        realValue = dict_realvalue
        mfValue = realValue[next(iter(realValue))]
        mlValue = realValue[next(iter(realValue))]
        month = next(iter(realValue))
        firstmonth = month.split('-')[1]
        ymonth = month.split('-')[0] + '-' + month.split('-')[1]
        monthCount = 1

        keyList = []
        for key in realValue.keys():
            keyList.append(key)

        for i in range(len(keyList)):
            splitkey = keyList[i].split('-')
            mlValue = realValue[keyList[i - 1]]
            if (splitkey[1] != firstmonth):
                rtDict[ymonth] = round((mlValue - mfValue) / mfValue * 100, 2)
                mfValue = realValue[keyList[i]]
                ymonth = splitkey[0] + "-" + splitkey[1]
                firstmonth = splitkey[1]
                monthCount = monthCount + 1

            rtDict[ymonth] = round((realValue[key] - mfValue) / mfValue * 100, 2)

        sum_MonthlyReturn = 0
        for key in rtDict.keys():
            sum_MonthlyReturn = sum_MonthlyReturn + rtDict[key]

        rtDict['월 수익률 평균'] = round(sum_MonthlyReturn / monthCount, 2)

        return (rtDict)
    
    def get_winRate(self,df):
        win_count = 0

        for i in df.index:
            if df.loc[i, 'return'] >= 0:
                win_count = win_count + 1

        return round((win_count) / (len(df.index)) * 100, 2)

    
    async def get_portVariables(self,dict_realvalue, dict_inputmoney):
        portfolio_result = dict()
        values = dict()
        for key in dict_inputmoney.keys():
            values[key] = list()
            values[key].append(float(dict_inputmoney[key]))
            values[key].append(dict_realvalue[key])

        df = pd.DataFrame.from_dict(values, orient='index', columns=['seed', 'value'])

        await self.get_returns(df)
        win_rate = self.get_winRate(df)
        portfolio_result['투입한 금액'] = dict_inputmoney[list(dict_inputmoney.keys())[-1]]
        portfolio_result['포트폴리오 가치'] = df.iloc[-1, 1]
        portfolio_result['총 수익률'] = df.iloc[-1, 2]
        portfolio_result['월 수익률 추이'] = await self.monthlyReturn(dict_realvalue)
        portfolio_result['일별 수익률'] = dict(zip(df.index, df['return']))
        portfolio_result['승률'] = win_rate

        return portfolio_result
        
    async def cal_receiptValue(self,year, value):
        if year == 10:
            return value
        return value / (11 - year) * 1.2
    
    
    async def receipt_simul(self,portfolioResult, receiptYear):

        cum_value = int(portfolioResult['포트폴리오 가치'])
        rtDict1 = dict()
        rtDict1['총 수령액'] = 0
        can_receiptValue = 0

        for i in range(1, receiptYear + 1):
            can_receiptValue = int(cum_value / (receiptYear - (i - 1)))
            if i <= 10:
                if can_receiptValue > await self.cal_receiptValue(i, cum_value):
                    can_receiptValue = int(self.cal_receiptValue(i, cum_value))
            if can_receiptValue > 12000000:
                can_receiptValue = 12000000
            cum_value = int(cum_value - can_receiptValue) 
            rtDict1[str(i) + '년차 수령금액'] = can_receiptValue
            rtDict1['총 수령액'] = rtDict1['총 수령액'] + can_receiptValue

        rtDict1['잔액'] = cum_value

        cum_value2 = int(portfolioResult['포트폴리오 가치']) 
        rtDict2 = dict()
        rtDict2['총 수령액'] = 0
        can_receiptValue2 = 0
        year = 1

        while cum_value2 > 0:
            if year <= 10:
                can_receiptValue2 = int(await self.cal_receiptValue(year, cum_value2)) 
            else:
                can_receiptValue2 = int(cum_value2)

            if can_receiptValue2 > 12000000:
                can_receiptValue2 = 12000000

            rtDict2[str(year) + '년차 수령금액'] = can_receiptValue2
            rtDict2['총 수령액'] = rtDict2['총 수령액'] + can_receiptValue2
            cum_value2 = int(cum_value2 - can_receiptValue2)
            year = year + 1

        rtDict2['잔액'] = cum_value2
        
        if rtDict1['잔액'] > 0:
            pass
        rtList = [rtDict1, rtDict2]
        return rtList
    
class Strategy():
    def __init__(self,strategy_kind, product_count_per_strategy,min_value,max_value):
        
        self.strategy_kind=strategy_kind
        self.product_count_per_strategy = product_count_per_strategy
        if self.strategy_kind == 'PER':
            self.min_value = min_value
            self.max_value = max_value
        else:
            self.min_value = 0
            self.max_value = 0
        
    def getProductListQuery(self):
        
        if self.strategy_kind == 'PER 저':
            self.sql_query='select product_date,product_ticker from product_evaluate order by per asc limit '+str(self.product_count_per_strategy)
        
        elif self.strategy_kind == 'PER 고':
            self.sql_query='select product_date,product_ticker from product_evaluate order by per desc limit '+str(self.product_count_per_strategy)
         
        elif self.strategy_kind == 'PER':
            self.sql_query='select product_date,product_ticker from product_evaluate where per >= '+str(self.min_value)+' and per <='+str(self.max_value)+' order by per asc limit '+str(self.product_count_per_strategy)
        
        elif self.strategy_kind == 'PBR 저':
            self.sql_query='select product_date,product_ticker from product_evaluate order by pbr asc limit '+str(self.product_count_per_strategy)
            
        elif self.strategy_kind == 'PBR 고':
            self.sql_query='select product_date,product_ticker from product_evaluate order by pbr desc limit '+str(self.product_count_per_strategy)
            
        pass
        
        return self.strategy_kind, self.sql_query

class Backtest():
    def __init__(self,portfolio_id, portfolio_name, portfolio_start_money, strategy_ratio, portfolio_start_time, 
                portfolio_end_time, rebalance_cycle, input_type, input_money,stratgy_input_info_list):
        
        self.portfolio_id = portfolio_id
        self.portfolio_name = portfolio_name
        self.portfolio_start_money = portfolio_start_money
        self.strategy_ratio = strategy_ratio
        self.portfolio_start_time = portfolio_start_time
        self.portfolio_end_time = portfolio_end_time
        self.rebalance_cycle = rebalance_cycle
        self.input_type = input_type
        self.input_money = input_money
        self.strategy_list = list()
        self.stratgy_input_info_list = stratgy_input_info_list
        
        self.portfolio_object = Portfolio( self.portfolio_id,self.portfolio_name, self.portfolio_start_money, self.strategy_ratio, self.portfolio_start_time, self.portfolio_end_time, self.rebalance_cycle, self.input_type, self.input_money)
        
        for stratgy_input_info in self.stratgy_input_info_list:
            self.strategy_list.append(Strategy(*stratgy_input_info))
            
    async def getProductTicker(self, sql_query, interval_dates):
        product_ticker_infos=list()
        
        for interval_date in interval_dates:        
            get_product_ticker_query=sql_query.split(' ')
            if get_product_ticker_query[4] == 'where':
                get_product_ticker_query.insert(5,"product_date = '"+str(interval_date)+"' and")
            else:
                get_product_ticker_query.insert(4,"where product_date = '"+str(interval_date)+"'") 
            get_product_ticker_query=" ".join(get_product_ticker_query)

            snowball.execute(get_product_ticker_query) 
            
            results=list(snowball.fetchall())
            
            for i in range(len(results)):
                results[i] = list(results[i]) 
                results[i][0] = str(results[i][0])
                
            product_ticker_infos.append(results)
            
        return product_ticker_infos
    
    async def getProductPrice(self, product_date, product_ticker):
        sql_query = "select high_price from price_"+product_ticker+" where product_date='"+product_date+"'"

        snowball.execute(sql_query) 

        result=snowball.fetchone()
        result=list(result)
        result.insert(0,product_ticker) 
        
        return result
      
    async def getPortfolioToRebalanceProductPrice(self, stratgy_sql_query_list, strategy_list, rebalance_date):
        
        portfolio_rebalance_product_price = list()
        
        for i,sql_query in enumerate(stratgy_sql_query_list):
            
            product_ticker_infos = await self.getProductTicker(sql_query,[rebalance_date])
            strategy_dict=dict()
            product_dict=dict() 
            
            for product_ticker_info in product_ticker_infos:
                
                for product_date,product_ticker in product_ticker_info:
                    product_price_info= await self.getProductPrice(product_date,product_ticker)
                    if product_date in product_dict:
                        product_dict[product_date].append(product_price_info)
                    else: 
                        product_dict[product_date] = [product_price_info]
            
            strategy_dict[strategy_list[i]]=product_dict 
            portfolio_rebalance_product_price.append(strategy_dict)
        
        return portfolio_rebalance_product_price      
    
    async def getPortfolioRabalanceInfo(self, portfolio_rebalance_product_price, rebalance_input_money, strategy_ratio, test_date):
        
        portfolio_rebalance_product_count=copy.deepcopy(portfolio_rebalance_product_price)
        
        rebalance_balance_account=dict()
        rebalance_balance_account[test_date] =0 
        input_money_ratio=list()
        for amount in strategy_ratio:
            input_money_ratio.append(amount*rebalance_input_money//100)

        for j,strategy_kind_money in enumerate(input_money_ratio):
            product_price_dict = list(portfolio_rebalance_product_count[j].values())[0]
            
            if len(product_price_dict) <= 0: 
                rebalance_balance_account[test_date] +=strategy_kind_money 
                continue
            price_lists=product_price_dict[test_date]
                
            strategy_product_money = int(strategy_kind_money // len(price_lists)) 
            for price_list in price_lists:
                rebalance_balance_account[test_date] += strategy_product_money%price_list[1]

                price_list[1] = int(strategy_product_money//price_list[1])

        return rebalance_balance_account, portfolio_rebalance_product_count
    
    async def getPortfolioProductValue(self, portfolio_rebalance_product_price, portfolio_rebalance_product_count):
        product_price = copy.deepcopy(portfolio_rebalance_product_price)
        product_count = copy.deepcopy(portfolio_rebalance_product_count)
        for i in range(len(product_price)):
            price_strategy_key=list(product_price[i].keys())[0]
            price_strategy_value=product_price[i][price_strategy_key]
            strategy_value_keys=list(price_strategy_value.keys())
            
            count_strategy_value=product_count[i][price_strategy_key]
            
            for strategy_value_key in strategy_value_keys:
                price_lists=price_strategy_value[strategy_value_key]
                count_lists=count_strategy_value[strategy_value_key]
                
                for i in range(len(price_lists)):
                    price_lists[i][1] = price_lists[i][1] * count_lists[i][1]
        return(product_price)
    
    async def getPortfolioStrategyValue(self,portfolio_rebalance_product_value):
        product_value = copy.deepcopy(portfolio_rebalance_product_value)
        for i in range(len(product_value)):
            price_strategy_key=list(product_value[i].keys())[0]
            price_strategy_value=product_value[i][price_strategy_key]
            strategy_value_keys=list(price_strategy_value.keys())
            
            for strategy_value_key in strategy_value_keys:
                sum=0
                price_lists=price_strategy_value[strategy_value_key]
                for price_list in price_lists:
                    sum+=price_list[1]
                price_strategy_value[strategy_value_key] = sum
        return(product_value)
            
    def getPortfolioValueWithoutBalance(self,portfolio_rebalance_strategy_value):
        portfolio_strategy_value = copy.deepcopy(portfolio_rebalance_strategy_value)
        portfolio_value=dict()

        for i in range(len(portfolio_strategy_value)):
            price_strategy_key=list(portfolio_strategy_value[i].keys())[0]
            price_strategy_value=portfolio_strategy_value[i][price_strategy_key]
            strategy_value_keys=list(price_strategy_value.keys())
            
            
            for strategy_value_key in strategy_value_keys:
                if strategy_value_key in portfolio_value:
                    portfolio_value[strategy_value_key]+=price_strategy_value[strategy_value_key]
                else:
                    portfolio_value[strategy_value_key]=price_strategy_value[strategy_value_key]
        return(portfolio_value)
            
    async def getPortfolioProductPrice(self,portfolio_rebalance_product_price, test_date):
        product_price = copy.deepcopy(portfolio_rebalance_product_price)
        for strategy_kind_dict in product_price:
            for strategy_kind_key in strategy_kind_dict.keys():
                temp = strategy_kind_dict[strategy_kind_key]
                if len(list(temp.keys())) ==0:
                    continue
                temp[test_date] = temp.pop(list(temp.keys())[0])
                for i,product_list in enumerate(temp[test_date]):
                    temp[test_date][i] = await self.getProductPrice(test_date,product_list[0])
        return product_price  
    
    async def getPortfolioProductInfo(self,portfolio_product_price,input_money,strategy_ratio, test_date):
        portfolio_product_count=copy.deepcopy(portfolio_product_price)
        
        input_balance_account=dict()
        input_balance_account[test_date] = 0
        input_money_ratio=list()

        for i in strategy_ratio:
            input_money_ratio.append(i*input_money//100)

        for i,money in enumerate(input_money_ratio):
            product_price_dict = list(portfolio_product_count[i].values())[0]
            if len(list(product_price_dict)) <=0: 
                input_balance_account[test_date] += money 
            price_lists=product_price_dict[test_date]

            money_to_price_list = money//len(price_lists)
    
            for price_list in price_lists:
                input_balance_account[test_date] += money_to_price_list % price_list[1]
                price_list[1] = int(money_to_price_list // price_list[1])
     
        return input_balance_account,portfolio_product_count

    async def getPortfolioProductAccumulateCount(self,Portfolio_rebalance_product_count,Portfolio_product_count):
        
        portfolio_rebalance_product_count = copy.deepcopy(Portfolio_rebalance_product_count)
        portfolio_product_count = copy.deepcopy(Portfolio_product_count)
        
        for i in range(len(portfolio_product_count)):
            product_strategy_key=list(portfolio_product_count[i].keys())[0]
            product_strategy_value=portfolio_product_count[i][product_strategy_key]
            if len(list(product_strategy_value))<=0:
                continue 
            product_strategy_value_key=list(product_strategy_value.keys())[0] 
            
            rebalance_product_strategy_key=list(portfolio_rebalance_product_count[i].keys())[0]
            rebalance_product_strategy_value=portfolio_rebalance_product_count[i][rebalance_product_strategy_key]
            rebalance_product_strategy_value_key=list(rebalance_product_strategy_value.keys())[0]
            
            for i,product_list in enumerate(product_strategy_value[product_strategy_value_key]):
                product_list[1]+=rebalance_product_strategy_value[rebalance_product_strategy_value_key][i][1]
        return portfolio_product_count

    async def getRealPortfolioValue(self,total_portfolio_account,total_balance_account):
        real_portfolio_account = dict()

        for portfolio_key in total_portfolio_account.keys():
            real_portfolio_account[portfolio_key]=total_portfolio_account[portfolio_key]+total_balance_account[portfolio_key]
        return real_portfolio_account
    
    async def updateToRecentDate(self,portfolio_product_count_account,new_date):
        for i in range(len(portfolio_product_count_account)):
            price_strategy_key=list(portfolio_product_count_account[i].keys())[0]
            price_strategy_value=portfolio_product_count_account[i][price_strategy_key]
            if len(list(price_strategy_value.keys())) <=0: 
                continue
            price_strategy_value[new_date]=price_strategy_value.pop(list(price_strategy_value.keys())[0])
        return(portfolio_product_count_account)
          
    async def doBacktest(self):
        
        self.stratgy_kind_list = list() 
        self.stratgy_sql_query_list = list() 
        
        for strategy_object in self.strategy_list: 
            self.stratgy_kind_list.append(strategy_object.getProductListQuery()[0])
            self.stratgy_sql_query_list.append(strategy_object.getProductListQuery()[1]) 
            
        self.backtesting_date_list = await getDailyDateInfo(self.portfolio_start_time, self.portfolio_end_time)

        self.rebalance_date_list = await getRebalanceDateInfo(self.portfolio_start_time, self.portfolio_end_time, self.input_type, self.rebalance_cycle)
        
        self.input_date_list = await getPayInDateInfo(self.portfolio_start_time, self.portfolio_end_time, self.input_type)
    
        self.tax_benfit_date_list = await getYearlyDateInfo(self.portfolio_start_time, self.portfolio_end_time)
        
        input_money_count = 0
        self.input_money_to_portfolio=dict()
        for backtesting_date in self.backtesting_date_list:
            if backtesting_date in self.input_date_list[1:]:
                input_money_count+=1
            self.input_money_to_portfolio[backtesting_date] = self.input_money *input_money_count + self.portfolio_start_money
            
        for tax in range(2):
        
            tax_benefit_money = self.portfolio_start_money

            current_portfolio_amount_without_balance = 0

            
            total_portfolio_without_balance_account = dict()
            
            current_balance_amount = self.portfolio_start_money
            
            recent_rebalance_date = None
            
            self.portfolio_balance_account = dict() 
            self.portfolio_product_count_account = None
    
            for backtesting_date in self.backtesting_date_list:
                if (backtesting_date in self.tax_benfit_date_list) and tax ==1:
                    if tax_benefit_money >= 7000000:
                        tax_benefit_money = 7000000 * 0.165
                    else:
                        tax_benefit_money *= 0.165
                    current_balance_amount += tax_benefit_money
                    tax_benefit_money =0
                    
                if backtesting_date in self.rebalance_date_list:
                    if backtesting_date in self.input_date_list and backtesting_date != self.rebalance_date_list[0]:
                        current_portfolio_amount_without_balance+=self.input_money
                        tax_benefit_money += self.input_money
                        
                    recent_rebalance_date = backtesting_date 
                    
                    portfolio_product_price = await self.getPortfolioToRebalanceProductPrice(self.stratgy_sql_query_list, self.stratgy_kind_list, recent_rebalance_date)
                    
                    rebalance_balance_account, self.portfolio_product_count_account = await self.getPortfolioRabalanceInfo(portfolio_product_price,current_portfolio_amount_without_balance+current_balance_amount, self.strategy_ratio, recent_rebalance_date)
                    
                    portfolio_rebalance_product_value= await self.getPortfolioProductValue(portfolio_product_price, self.portfolio_product_count_account)
                    
                    portfolio_rebalance_strategy_value= await self.getPortfolioStrategyValue(portfolio_rebalance_product_value)
                    
                    total_portfolio_without_balance_account[backtesting_date] = self.getPortfolioValueWithoutBalance(portfolio_rebalance_strategy_value).get(recent_rebalance_date)
                    current_portfolio_amount_without_balance = total_portfolio_without_balance_account[backtesting_date]
                    
                    self.portfolio_balance_account[backtesting_date] = rebalance_balance_account[backtesting_date]
                    
                    current_balance_amount =self.portfolio_balance_account[recent_rebalance_date]
                    
                elif backtesting_date in self.input_date_list:
                    portfolio_product_price = await self.getPortfolioProductPrice(portfolio_product_price, backtesting_date)
                    
                    tax_benefit_money += self.input_money 
                    
                    input_balance_account,new_portfolio_product_count= await self.getPortfolioProductInfo(portfolio_product_price, self.input_money+current_balance_amount, self.strategy_ratio, backtesting_date)
                    current_balance_amount = input_balance_account[list(input_balance_account.keys())[-1]]
                    
                    self.portfolio_product_count_account = await self.getPortfolioProductAccumulateCount(self.portfolio_product_count_account, new_portfolio_product_count)
                    
                    portfolio_product_value=await self.getPortfolioProductValue(portfolio_product_price, self.portfolio_product_count_account)
                    
                    portfolio_strategy_value=await self.getPortfolioStrategyValue(portfolio_product_value)
                    
                    total_portfolio_without_balance_account[backtesting_date] = self.getPortfolioValueWithoutBalance(portfolio_strategy_value)[backtesting_date]
                    current_portfolio_amount_without_balance = total_portfolio_without_balance_account[backtesting_date]
                    
                    self.portfolio_balance_account[backtesting_date] = current_balance_amount
                    
                else:
                    portfolio_product_price= await self.getPortfolioProductPrice(portfolio_product_price, backtesting_date)
                
                    self.portfolio_product_count_account= await self.updateToRecentDate(self.portfolio_product_count_account,backtesting_date)
                    
                    portfolio_product_value = await self.getPortfolioProductValue(portfolio_product_price, self.portfolio_product_count_account)
                    
                    portfolio_strategy_value = await self.getPortfolioStrategyValue(portfolio_product_value)
                    
                    total_portfolio_without_balance_account[backtesting_date]= self.getPortfolioValueWithoutBalance(portfolio_strategy_value)[backtesting_date]
                    current_portfolio_amount_without_balance = total_portfolio_without_balance_account[backtesting_date]
                    
                    
                    self.portfolio_balance_account[backtesting_date] = current_balance_amount
                    
            if tax == 0:
                real_portfolio_account= await self.getRealPortfolioValue(total_portfolio_without_balance_account,self.portfolio_balance_account) # 포트폴리오 가치 추이
                self.portfolio_object.portfolio_account_without_tax_benefit = await self.portfolio_object.get_portVariables(real_portfolio_account, self.input_money_to_portfolio) # 포트폴리오 출력결과 변수
                self.portfolio_object.portfolio_receive_without_tax_benefit = await self.portfolio_object.receipt_simul(self.portfolio_object.portfolio_account_without_tax_benefit,10) # 포트폴리오 수령방법, 몇년 수령할지 입력(10년 디폴트이고 나중에 사용자 맞게 수정 가능)

            elif tax == 1:
                real_portfolio_account_tax_benefit=await self.getRealPortfolioValue(total_portfolio_without_balance_account,self.portfolio_balance_account)
                self.portfolio_object.portfolio_account_with_tax_benefit = await self.portfolio_object.get_portVariables(real_portfolio_account_tax_benefit, self.input_money_to_portfolio)
                self.portfolio_object.portfolio_receive_with_tax_benefit = await self.portfolio_object.receipt_simul(self.portfolio_object.portfolio_account_with_tax_benefit,10) # 몇년 수령할지 입력(10년 디폴트이고 나중에 사용자 맞게 수정 가능)
                    
        # print('포트폴리오 결과 :',self.portfolio_object.portfolio_account_without_tax_benefit)
        # print()
        # print('포트폴리오 결과 :',self.portfolio_object.portfolio_account_with_tax_benefit)
    
 
if __name__ == "__main__":

    loop = asyncio.get_event_loop()     

    backtest_object = Backtest('123456', 'teststrategy', 3000000, [30,30,40], '2017-10-01', '2018-05-01', 3, '0', 700000, [['PER 저', 2, 0, 0], ['PER 고', 3, 0, 0], ['PER', 3, 10, 15]])

    loop.run_until_complete(backtest_object.doBacktest()) 
    loop.close()
    db.close()  