from datetime import date, timedelta, datetime
import copy
import datetime as dt
from pandas.tseries.offsets import *
from pandas_datareader import data as pdr
import pandas as pd
from pymongo import MongoClient


from src.core.getDatainfo import getDailyDateInfo, getPayInDateInfo, getRebalanceDateInfo, getYearlyDateInfo

import time

host = 'host.docker.internal'
port = 27017
client = MongoClient(host, port)
db = client['snowball_data_engineer']
collection = db['etf_evaluate']
collection2 = db['etf_price']

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
    
    def getReturns(self,df):
        df['return'] = (df['value'] - df['seed']) / df['seed']
        df['return'] = ((df['return'].round(4))*100).round(2)
    
    def monthlyReturn(self,dict_realvalue):
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
    
    def getWinRate(self,df):
        win_count = 0

        for i in df.index:
            if df.loc[i, 'return'] >= 0:
                win_count = win_count + 1

        return round((win_count) / (len(df.index)) * 100, 2)
    
    def calculateMdd(self, portfolio_with_tax_benefit_account):
            
        portfolio_values = list(portfolio_with_tax_benefit_account.values())
        max_value = portfolio_values[0]
        mdd = 0

        for value in portfolio_values:
            if value > max_value:
                max_value = value
            else:
                drawdown = (value - max_value) / max_value
                if drawdown < mdd:
                    mdd = drawdown
        mdd = mdd*100
        return mdd
    
    def calculateLowestReturn(self, dailyReturn):
        dailyReturn = list(dailyReturn.values())
        
        min_return = dailyReturn[0]
        
        for value in dailyReturn:
            if value < min_return:
                min_return = value
        return min_return

    
    def getPortVariables(self,dict_realvalue, dict_inputmoney):
        portfolio_result = dict()
        values = dict()
        for key in dict_inputmoney.keys():
            values[key] = list()
            values[key].append(float(dict_inputmoney[key]))
            values[key].append(dict_realvalue[key])

        df = pd.DataFrame.from_dict(values, orient='index', columns=['seed', 'value'])

        self.getReturns(df)
        win_rate = self.getWinRate(df)
        portfolio_result['투입한 금액'] = dict_inputmoney[list(dict_inputmoney.keys())[-1]]
        portfolio_result['포트폴리오 가치'] = df.iloc[-1, 1]
        
        portfolio_result['총 수익률'] = df.iloc[-1, 2]
        portfolio_result['월 수익률 추이'] = self.monthlyReturn(dict_realvalue)
        portfolio_result['일별 수익률'] = dict(zip(df.index, df['return']))
        portfolio_result['일별 최저수익률'] = self.calculateLowestReturn(portfolio_result['일별 수익률'])
        portfolio_result['승률'] = win_rate
        portfolio_result['mdd'] = self.calculateMdd(dict_realvalue)
        return portfolio_result
        
    def calReceiptValue(self,year, value):
        if year == 10:
            return value
        return value / (11 - year) * 1.2
    
    
    def receiptSimul(self,portfolioResult, receiptYear):
        cum_value = int(portfolioResult['포트폴리오 가치'])

        rtDict1 = dict()
        rtDict1['총 수령액'] = 0
        can_receiptValue = 0

        for i in range(1, receiptYear + 1):
            can_receiptValue = int(cum_value / (receiptYear - (i - 1)))
            if i <= 10:
                if can_receiptValue > self.calReceiptValue(i, cum_value):
                    can_receiptValue = int(self.calReceiptValue(i, cum_value))
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
                can_receiptValue2 = int(self.calReceiptValue(year, cum_value2))
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
            print("첫 번째 방식의 경우 잔액이 0보다 큽니다, 수령 기간을 늘리시길 권장드립니다.")
        rtList = [rtDict1, rtDict2]
        return rtList
    
class Strategy():
    def __init__(self,strategy_kind, product_count_per_strategy,min_value,max_value):
        self.strategy_kind=strategy_kind
        self.product_count_per_strategy = product_count_per_strategy
        self.min_value = min_value
        self.max_value = max_value

    def getProductListQuery(self):        
        self.mongodb_query = (self.strategy_kind, self.product_count_per_strategy,self.min_value,self.max_value)
        return self.mongodb_query



class Backtest(Portfolio):
    def __init__(self,portfolio_id, portfolio_name, portfolio_start_money, strategy_ratio_list, portfolio_start_time, 
                portfolio_end_time, rebalance_cycle, input_type, input_money,stratgy_input_info_list):
        
        
        self.portfolio_id = portfolio_id
        self.portfolio_name = portfolio_name
        self.portfolio_start_money = portfolio_start_money
        self.strategy_ratio_list = strategy_ratio_list
        self.portfolio_start_time = portfolio_start_time
        self.portfolio_end_time = portfolio_end_time
        self.rebalance_cycle = rebalance_cycle
        self.input_type = input_type
        self.input_money = input_money
        self.strategy_list = list()
        self.stratgy_input_info_list = stratgy_input_info_list
        self.portfolio_object = Portfolio( self.portfolio_id,self.portfolio_name, self.portfolio_start_money, self.strategy_ratio_list, self.portfolio_start_time, self.portfolio_end_time, self.rebalance_cycle, self.input_type, self.input_money)
        self.backtesting_date_list = getDailyDateInfo(self.portfolio_start_time, self.portfolio_end_time)
        self.rebalance_date_list = getRebalanceDateInfo(self.portfolio_start_time, self.portfolio_end_time, self.input_type, self.rebalance_cycle)
        self.input_date_list = getPayInDateInfo(self.portfolio_start_time, self.portfolio_end_time, self.input_type)
        self.tax_benfit_date_list = getYearlyDateInfo(self.portfolio_start_time, self.portfolio_end_time)
        self.strategy_name_list = list()
        self.strategy_product_number = list()
        self.strategy_count = len(self.stratgy_input_info_list)
        
        for stratgy_input_info in self.stratgy_input_info_list:
            self.strategy_list.append(Strategy(*stratgy_input_info))
            self.strategy_name_list.append(stratgy_input_info[0])
            
        self.strategy_mongodb_query_list = list()
        for strategy_object in self.strategy_list:
            self.strategy_mongodb_query_list.append(strategy_object.getProductListQuery())
            
        self.purchase_assignment_amount_list =  list()
        self.strategy_product_code = list()
        self.strategy_product_price = list()
        self.strategy_add_product_count = list()
        self.strategy_accumulate_product_count = list()
        self.strategy_product_value = list()
        self.strategy_value = list()
        
        self.current_portfolio_value_without_balance = 0
        self.tax_benefit_money = 0
        self.current_balance = 0
        self.input_money_sum = 0
        
        self.input_money_to_portfolio_account=dict()
        self.balance_account = dict()
        self.portfolio_value_without_balance_account = dict()
        self.portfolio_without_tax_benefit_account = None
        self.portfolio_with_tax_benefit_account = None
        
        self.recent_strategy_product_code = list()
        
    def getProductTicker(self, mongodb_query, product_date):
        product_ticker_list=list()
        strategy_kind = mongodb_query[0]
        product_count_per_strategy = mongodb_query[1]
        min_value = mongodb_query[2]
        max_value = mongodb_query[3]
        product_date = datetime.strptime(product_date, "%Y-%m-%d")
        
        if strategy_kind == 'PER 저':
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_per", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'PER 고':
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_per", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PER':
            query = {"etf_date": product_date, "etf_per":{"$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_per", 1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PBR 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_pbr", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'PBR 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_pbr", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PBR':
            query = {"etf_date": product_date, "etf_pbr":{"$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_pbr", 1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PSR 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_pcr", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'PSR 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_pcr", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PSR':
            query = {"etf_date": product_date, "etf_pcr":{"$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_pcr", 1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'OperatingRatio 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_operating_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'OperatingRatio 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_operating_ratio", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'OperatingRatio': 
            query = {"etf_date": product_date, "etf_operating_ratio":{"$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_operating_ratio", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'ProfitRatio 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_profit_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'ProfitRatio 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_profit_ratio", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'ProfitRatio':
            query = {"etf_date": product_date, "etf_profit_ratio":{ "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_profit_ratio", -1).limit(product_count_per_strategy)

        elif strategy_kind == 'DebtRatio 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_debt_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'DebtRatio 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_debt_ratio", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'DebtRatio':
            query = {"etf_date": product_date, "etf_debt_ratio":{"$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_debt_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'ROE 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_roe", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'ROE 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_roe", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'ROE':
            query = {"etf_date": product_date, "etf_roe":{"$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_roe", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == '가격모멘텀 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_price_change", 1).limit(product_count_per_strategy)

        elif strategy_kind == '가격모멘텀 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_price_change", -1).limit(product_count_per_strategy)
        
        elif strategy_kind == '매출액모멘텀 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_revenue_change", 1).limit(product_count_per_strategy)

        elif strategy_kind == '매출액모멘텀 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_revenue_change", -1).limit(product_count_per_strategy)
                                
        elif strategy_kind == '영업이익모멘텀 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_operatingincomeloss_change", 1).limit(product_count_per_strategy)

        elif strategy_kind == '영업이익모멘텀 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_operatingincomeloss_change", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == '순이익모멘텀 저': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_profitloss_change", 1).limit(product_count_per_strategy)

        elif strategy_kind == '순이익모멘텀 고': 
            query = {"etf_date": product_date}
            result = collection.find(query).sort("etf_profitloss_change", -1).limit(product_count_per_strategy)

        else:
            query = {"etf_date": product_date, "etf_code":strategy_kind}
            result = collection.find(query)

        for document in result:
            product_ticker_list.append(document['etf_code'])
        return product_ticker_list


    def getProductPrice(self, product_date, product_ticker):
        
        product_date = datetime.strptime(product_date, "%Y-%m-%d")
        
        query = {"etf_date": product_date, "etf_code":product_ticker}

        documents = collection.find(query)
        for document in documents:
            result = document['etf_price']
        return result
    
    
    def getRebalanceStrategyProductCode(self,backtesting_date):
        self.strategy_product_code = [[] for _ in range(self.strategy_count)]
        for i,strategy_query in enumerate(self.strategy_mongodb_query_list):
            product_ticker_list = self.getProductTicker(strategy_query, backtesting_date)
            for product_ticker in product_ticker_list:
                self.strategy_product_code[i].append(product_ticker)
                
    # def getRecentStrategyProductCode(self,recent_date):
    #     self.recent_strategy_product_code = [[] for _ in range(self.strategy_count)]
    #     for i,strategy_query in enumerate(self.strategy_mongodb_query_list):
    #         product_ticker_list = self.getProductTicker(strategy_query, recent_date)
    #         for product_ticker in product_ticker_list:
    #             self.recent_strategy_product_code[i].append(product_ticker)
                
    def getRecentStrategyProductCode(self,recent_date):
        # collection2 = db['etf_price']
        self.recent_strategy_product_code = [[] for _ in range(self.strategy_count)]
        for i,strategy_query in enumerate(self.strategy_mongodb_query_list):
            product_ticker_list = self.getProductTicker(strategy_query, recent_date)
            for product_ticker in product_ticker_list:
                query = {"etf_code":product_ticker}
                documents = collection2.find(query).sort("etf_date", -1).limit(1)
                for document in documents:
                    etf_name = document['etf_name']
                product_dict = {product_ticker : etf_name}
                self.recent_strategy_product_code[i].append(product_dict)
                
        
    def getStrategyProductPrice(self,backtesting_date):
        self.strategy_product_price = [[] for _ in range(self.strategy_count)]
        for i,product_code_list in enumerate(self.strategy_product_code):
            for product_ticker in product_code_list:
                self.strategy_product_price[i].append(self.getProductPrice(backtesting_date,product_ticker))
    
    
    def getRebalanceStrategyProductCountandBalance(self,to_calculate_money):
        self.purchase_assignment_amount_list= [[] for _ in range(self.strategy_count)]
        self.strategy_accumulate_product_count = [[] for _ in range(self.strategy_count)]
        self.current_balance = 0
        
        for i,strategy_ratio in enumerate(self.strategy_ratio_list):
            strategy_product_number = len(self.strategy_product_code[i])  
            if strategy_product_number == 0:
                self.current_balance+=(strategy_ratio*to_calculate_money//100)
            else:
                for _ in range(strategy_product_number):
                    self.purchase_assignment_amount_list[i].append((strategy_ratio*to_calculate_money//100)//strategy_product_number) 
        current_buy_value =0
        
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j,product_price in enumerate(product_price_list):
                self.strategy_accumulate_product_count[i].append(int(self.purchase_assignment_amount_list[i][j]//product_price))
                current_buy_value += self.strategy_accumulate_product_count[i][j] * self.strategy_product_price[i][j]
        
        self.current_balance = to_calculate_money-current_buy_value


    def getStrategyProductValue(self):
        self.strategy_product_value = [[] for _ in range(self.strategy_count)]
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j, product_price in enumerate(product_price_list):
                self.strategy_product_value[i].append(product_price * self.strategy_accumulate_product_count[i][j])
        
        
    def getStrategyValue(self):
        self.strategy_value = list()
        for product_value in self.strategy_product_value:
            self.strategy_value.append(sum(product_value))
    
    def getPortfolioValueWithoutBalanceAndBalance(self,backtesting_date):
        self.portfolio_value_without_balance_account[backtesting_date] = sum(self.strategy_value)
        self.current_portfolio_value_without_balance = sum(self.strategy_value)
        self.balance_account[backtesting_date]=self.current_balance
        
    
    def getInputwork(self,backtesting_date):
        self.tax_benefit_money+=self.input_money
        self.current_balance+=self.input_money
        self.input_money_sum +=self.input_money
        self.input_money_to_portfolio_account[backtesting_date] = self.input_money_sum
        
        
    def updateProductcount(self, to_calculate_money):
        self.purchase_assignment_amount_list= [[] for _ in range(self.strategy_count)]
        self.strategy_add_product_count = [[] for _ in range(self.strategy_count)]
        self.current_balance = 0
        
        for i,strategy_ratio in enumerate(self.strategy_ratio_list):
            strategy_product_number = len(self.strategy_product_code[i])  
            if strategy_product_number == 0:
                self.current_balance+=(strategy_ratio*to_calculate_money//100)
            else:
                for _ in range(strategy_product_number):
                    self.purchase_assignment_amount_list[i].append((strategy_ratio*to_calculate_money//100)//strategy_product_number) 
        
        current_buy_value =0
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j,product_price in enumerate(product_price_list):
                self.strategy_add_product_count[i].append(int(self.purchase_assignment_amount_list[i][j]//product_price))
        for i,add_product_count_list in enumerate(self.strategy_add_product_count):
            for j, add_product_count in enumerate(add_product_count_list):
                self.strategy_accumulate_product_count[i][j]+=add_product_count
                current_buy_value += self.strategy_add_product_count[i][j] * self.strategy_product_price[i][j]
        
        
        self.current_balance = to_calculate_money - current_buy_value
    
    def getRealPortfolioValue(self):
        portfolio_value_account=dict()
        for backtesting_date in self.backtesting_date_list:
            portfolio_value_account[backtesting_date] = self.balance_account[backtesting_date]+self.portfolio_value_without_balance_account[backtesting_date]
        return portfolio_value_account
            
    
    def doBackTest(self, date):  
        for tax in range(2):
            self.tax_benefit_money = self.portfolio_start_money
            self.current_balance = self.portfolio_start_money
            self.current_portfolio_value_without_balance = 0
            self.input_money_sum = self.portfolio_start_money
            
            self.input_money_to_portfolio_account=dict()
            self.balance_account = dict()
            self.portfolio_value_without_balance_account = dict()
            
            for backtesting_date in self.backtesting_date_list:
                if (backtesting_date in self.tax_benfit_date_list) and tax ==1:
                    if self.tax_benefit_money >= 9000000:
                        self.tax_benefit_money = 9000000 * 0.165
                    else:
                        self.tax_benefit_money *= 0.165
                    
                    self.current_balance += self.tax_benefit_money
                    self.tax_benefit_money =0
        
                if backtesting_date in self.rebalance_date_list:
                    if backtesting_date in self.input_date_list and backtesting_date != self.rebalance_date_list[0]:
                        self.getInputwork(backtesting_date)
                    self.getRebalanceStrategyProductCode(backtesting_date)
                    self.getStrategyProductPrice(backtesting_date)
                    self.getRebalanceStrategyProductCountandBalance(self.current_balance + self.current_portfolio_value_without_balance)
                    self.getStrategyProductValue()
                    self.getStrategyValue()
                    self.getPortfolioValueWithoutBalanceAndBalance(backtesting_date)
                    # print()
                
                elif backtesting_date in self.input_date_list:
                    self.getStrategyProductPrice(backtesting_date)
                    self.getInputwork(backtesting_date)
                    self.updateProductcount(self.current_balance)
                    self.getStrategyProductValue()
                    self.getStrategyValue()
                    self.getPortfolioValueWithoutBalanceAndBalance(backtesting_date)
                
                else:
                    self.input_money_to_portfolio_account[backtesting_date] = self.input_money_sum
                    self.getStrategyProductPrice(backtesting_date)
                    self.getStrategyProductValue()
                    self.getStrategyValue()
                    self.getPortfolioValueWithoutBalanceAndBalance(backtesting_date)
                    
            if tax == 0:
                self.portfolio_without_tax_benefit_account = self.getRealPortfolioValue()
                self.portfolio_object.portfolio_account_without_tax_benefit = self.portfolio_object.getPortVariables(self.portfolio_without_tax_benefit_account, self.input_money_to_portfolio_account)
                self.portfolio_object.portfolio_receive_without_tax_benefit = self.portfolio_object.receiptSimul(self.portfolio_object.portfolio_account_without_tax_benefit,10)

            elif tax == 1:
                self.portfolio_with_tax_benefit_account = self.getRealPortfolioValue()
                self.portfolio_object.portfolio_account_with_tax_benefit = self.portfolio_object.getPortVariables(self.portfolio_with_tax_benefit_account, self.input_money_to_portfolio_account)
                self.portfolio_object.portfolio_receive_with_tax_benefit = self.portfolio_object.receiptSimul(self.portfolio_object.portfolio_account_with_tax_benefit,10)
        
        self.getRecentStrategyProductCode(date) # TODO: 가장 최근날짜로 바꾸기