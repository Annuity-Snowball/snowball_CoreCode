from datetime import date, timedelta
import copy
import datetime as dt
from pandas.tseries.offsets import *
from pandas_datareader import data as pdr
import pandas as pd
from pymongo import MongoClient

from src.core.getDatainfo import getDailyDateInfo, getPayInDateInfo, getRebalanceDateInfo, getYearlyDateInfo

import time

host = 'localhost'  # 호스트 주소
port = 27017  # 포트 번호

#     # MongoDB에 연결
client = MongoClient(host, port)

#     # 데이터베이스 선택
db = client['snowball_data_engineer']  # 여기서 'mydatabase'는 사용하려는 데이터베이스 이름입니다.

#     # 컬렉션 선택 (테이블과 유사한 개념)
collection = db['etf_evaluate']  # 여기서 'mycollection'은 사용하려는 컬렉션 이름입니다.

# 포트폴리오 클래스 생성
class Portfolio():
    
    # 객체 생성할 때 초기화하는 매소드 입니다
    def __init__(self,portfolio_id, portfolio_name, start_money, strategy_ratio, start_time, end_time, rebalance_cycle, input_type, input_money):
        # 포트폴리오아이디 생성
        self.portfolio_id=portfolio_id # 추후 수정 필요
        
        
        # 포트폴리오 시작금액 입력받음
        self.start_money = start_money
        
        # 포트폴리오명 입력받음
        self.portfolio_name = portfolio_name
        
            
        # 구성 전략 개수 별 비율 입력 받기
        self.strategy_ratio= strategy_ratio
            
        # 포트폴리오 시작날짜, 끝날짜 입력받음(둘다 str 형태)
        self.start_time=start_time
        self.end_time=end_time
        
        # 리밸런싱 주기 입력받음
        self.rebalance_cycle = rebalance_cycle
        
        # 납입방법 입력 받음
        self.input_type = input_type
        
        # 주기적납입금액 입력받음
        self.input_money = input_money
        
        self.portfolio_account_without_tax_benefit = None
        self.portfolio_receive_without_tax_benefit = None
        
        self.portfolio_account_with_tax_benefit = None
        self.portfolio_receive_with_tax_benefit = None
        # 데이터베이스에 업데이트하는 부분 - sql 쿼리문
        pass # 추후 수정 필요
    
    def getReturns(self,df):
        df['return'] = (df['value'] - df['seed']) / df['seed']  # +df['cash'] #일별수익률 계산해서 열 추가 (월간수익률(납입일 기준),추가 가능)
        df['return'] = ((df['return'].round(4))*100).round(2)
    
    def monthlyReturn(self,dict_realvalue):
        # 입력: 포트폴리오 가치
        realValue = dict()
        rtDict = dict()
        realValue = dict_realvalue
        mfValue = realValue[next(iter(realValue))]  # 월 초 가치, 초기값: 시작 날 가치
        mlValue = realValue[next(iter(realValue))]  # 월 말 가치
        month = next(iter(realValue))
        firstmonth = month.split('-')[1]
        ymonth = month.split('-')[0] + '-' + month.split('-')[1]
        monthCount = 1

        keyList = []
        for key in realValue.keys():
            keyList.append(key)

        for i in range(len(keyList)):
            splitkey = keyList[i].split('-')
            mlValue = realValue[keyList[i - 1]]  # 월 말 value 보존
            if (splitkey[1] != firstmonth):  # 월 변경될 경우
                rtDict[ymonth] = round((mlValue - mfValue) / mfValue * 100, 2)  # 월 수익률 계산
                mfValue = realValue[keyList[i]]  # 월 초 값 갱신
                ymonth = splitkey[0] + "-" + splitkey[1]  # 연-월 갱신 (딕셔너리 생성에 사용)
                firstmonth = splitkey[1]  # 월 갱신 (월 비교에 사용)
                monthCount = monthCount + 1  # 운용 개월 수 체크

            rtDict[ymonth] = round((realValue[key] - mfValue) / mfValue * 100, 2)  # realValue[key]: 마지막 날 수익률 마지막 달 수익률 계산

        sum_MonthlyReturn = 0
        for key in rtDict.keys():
            sum_MonthlyReturn = sum_MonthlyReturn + rtDict[key]

        rtDict['월 수익률 평균'] = round(sum_MonthlyReturn / monthCount, 2)

        return (rtDict)
    
    def getWinRate(self,df):
        # 리밸런싱 날짜 리스트받아서, 해당하는 날짜들의 승률 계산해서 반환
        win_count = 0

        for i in df.index:
            if df.loc[i, 'return'] >= 0:
                win_count = win_count + 1

        return round((win_count) / (len(df.index)) * 100, 2)

    
    def getPortVariables(self,dict_realvalue, dict_inputmoney):
        portfolio_result = dict()
        values = dict()
        for key in dict_inputmoney.keys():
            values[key] = list()
            values[key].append(float(dict_inputmoney[key]))
            values[key].append(dict_realvalue[key])

        df = pd.DataFrame.from_dict(values, orient='index', columns=['seed', 'value'])  # 넘겨받은 사전 데이터 데이터프레임으로 변환

        self.getReturns(df)  # 수익률 계산
        win_rate = self.getWinRate(df)  # 승률 계산해서 저장 (리밸런싱 날짜 기준)
        portfolio_result['투입한 금액'] = dict_inputmoney[list(dict_inputmoney.keys())[-1]]
        portfolio_result['포트폴리오 가치'] = df.iloc[-1, 1]  # 포트폴리오 가치 저장, 초기값: 수령 직전 가치
        
        portfolio_result['총 수익률'] = df.iloc[-1, 2]
        portfolio_result['월 수익률 추이'] = self.monthlyReturn(dict_realvalue)
        portfolio_result['일별 수익률'] = dict(zip(df.index, df['return']))
        portfolio_result['승률'] = win_rate

        return portfolio_result
        
    def calReceiptValue(self,year, value):
        if year == 10:
            return value
        return value / (11 - year) * 1.2
    
    
    def receiptSimul(self,portfolioResult, receiptYear):

        cum_value = int(portfolioResult['포트폴리오 가치'])  # 포트폴리오 가치 저장, 초기값: 수령 직전 가치

        rtDict1 = dict()
        rtDict1['총 수령액'] = 0
        can_receiptValue = 0

        for i in range(1, receiptYear + 1):
            can_receiptValue = int(cum_value / (receiptYear - (i - 1)))  # 남은 금액을 남은 연차로 엔빵
            if i <= 10:  # 1~10년차의 경우
                if can_receiptValue > self.calReceiptValue(i, cum_value):
                    can_receiptValue = int(self.calReceiptValue(i, cum_value))  # 10년이내의 최대를 넘을 시 최대금액으로 제한
            if can_receiptValue > 12000000:  # 세액공제 한도 초과 시
                can_receiptValue = 12000000  # 한도 범위인 1200만원으로 제한
            cum_value = int(cum_value - can_receiptValue)  # 잔액 정리
            rtDict1[str(i) + '년차 수령금액'] = can_receiptValue
            rtDict1['총 수령액'] = rtDict1['총 수령액'] + can_receiptValue

        rtDict1['잔액'] = cum_value

        cum_value2 = int(portfolioResult['포트폴리오 가치'])  # 포트폴리오 가치 저장, 초기값: 수령 직전 가치
        rtDict2 = dict()
        rtDict2['총 수령액'] = 0
        can_receiptValue2 = 0
        year = 1

        while cum_value2 > 0:
            if year <= 10:
                can_receiptValue2 = int(self.calReceiptValue(year, cum_value2))  # 당 해 수령가능 금액
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
        
        self.sql_query = (self.strategy_kind, self.product_count_per_strategy,self.min_value,self.max_value)
        return self.sql_query



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
        self.stratgy_input_info_list = stratgy_input_info_list # 전략생성을 위해서 기입할 정보드을 담을 리스트 ex) [['전략명1','전략으로담을금융상품개수1','수치입력일 경우 시작값', '수치입력을 경우 마지막값']]
                                                                # 백엔드 구현 시 함수 입력이 아닌 직접구현으로 교체
                                                                # 추후 이런식으로 직접 대입 [['PER 저', 2, 0, 0], ['PER 고', 3, 0, 0]]
        
        self.portfolio_object = Portfolio( self.portfolio_id,self.portfolio_name, self.portfolio_start_money, self.strategy_ratio_list, self.portfolio_start_time, self.portfolio_end_time, self.rebalance_cycle, self.input_type, self.input_money)
    
        
        self.backtesting_date_list = getDailyDateInfo(self.portfolio_start_time, self.portfolio_end_time) # 백테스트 기간 리스트
        self.rebalance_date_list = getRebalanceDateInfo(self.portfolio_start_time, self.portfolio_end_time, self.input_type, self.rebalance_cycle) #리밸런싱 날짜 리스트
        self.input_date_list = getPayInDateInfo(self.portfolio_start_time, self.portfolio_end_time, self.input_type) # 납입날짜 리스트
        self.tax_benfit_date_list = getYearlyDateInfo(self.portfolio_start_time, self.portfolio_end_time) # 세제혜택 날짜 리스트

        self.strategy_name_list = list() # 전략별전략명
        self.strategy_product_number = list() # 전략별금융상품개수
        self.strategy_count = len(self.stratgy_input_info_list) # 포트폴리오를 구성하는 전략 개수
        
        for stratgy_input_info in self.stratgy_input_info_list: # '포트폴리오'를 구성하는 '전략의 개수'만큼 반복
            self.strategy_list.append(Strategy(*stratgy_input_info)) # 'Strategy() 클래스'를 이용해서 생성한 '전략'들을 '전략 리스트'에 추가
            self.strategy_name_list.append(stratgy_input_info[0])
            self.strategy_product_number.append(stratgy_input_info[1])
            
        self.strategy_sql_query_list = list() # 전략을 조회하는 쿼리문 들 리스트
        for strategy_object in self.strategy_list: # '전략리스트' 에 있는 모든 '전략'들에 대해서 반복
            self.strategy_sql_query_list.append(strategy_object.getProductListQuery())
            
        self.purchase_assignment_amount_list =  [[] for _ in range(self.strategy_count)] # 구매할 때 할당금액
        self.strategy_product_code = [[] for _ in range(self.strategy_count)] # 전략별금융상품코드
        self.strategy_product_price = [[] for _ in range(self.strategy_count)] # 전략별금융상품가격
        self.strategy_add_product_count = [[] for _ in range(self.strategy_count)] # 전략별추가할금융상품개수
        self.strategy_accumulate_product_count = [[] for _ in range(self.strategy_count)] # 전략별누적금융상품개수
        self.strategy_product_value = [[] for _ in range(self.strategy_count)] # 조회금융상품가치
        self.strategy_value = list() # 조회금융상품전략별가치
        
        self.current_portfolio_value_without_balance = 0 # 포트폴리오가치
        self.tax_benefit_money = 0 # 1년간 납입금액
        self.current_balance = 0 # 현재 잔액
        self.input_money_sum = 0 # 납입금액 합계
        
        self.input_money_to_portfolio_account=dict()
        self.balance_account = dict()
        self.portfolio_value_without_balance_account = dict()
        self.portfolio_without_tax_benefit_account = None
        self.portfolio_with_tax_benefit_account = None 
        
        
    def getProductTicker(self, sql_query, product_date):
        product_ticker_list=list()

        strategy_kind = sql_query[0]
        product_count_per_strategy = sql_query[1]
        min_value = sql_query[2]
        max_value = sql_query[3]
        
        if strategy_kind == 'PER 저':
            query = {"etf_date": product_date, "etf_per":{"$gt":0}}
            result = collection.find(query).sort("etf_per", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'PER 고':
            query = {"etf_date": product_date, "etf_per":{"$gt":0}}
            result = collection.find(query).sort("etf_per", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PER':
            query = {"etf_date": product_date, "etf_per":{"$gt":0, "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_per", 1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PBR 저': 
            query = {"etf_date": product_date, "etf_pbr":{"$gt":0}}
            result = collection.find(query).sort("etf_pbr", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'PBR 고': 
            query = {"etf_date": product_date, "etf_pbr":{"$gt":0}}
            result = collection.find(query).sort("etf_pbr", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PBR':
            query = {"etf_date": product_date, "etf_pbr":{"$gt":0, "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_pbr", 1).limit(product_count_per_strategy)
            
            
        elif strategy_kind == 'PSR 저': 
            query = {"etf_date": product_date, "etf_pcr":{"$gt":0}}
            result = collection.find(query).sort("etf_pcr", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'PSR 고': 
            query = {"etf_date": product_date, "etf_pcr":{"$gt":0}}
            result = collection.find(query).sort("etf_pcr", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'PSR':
            query = {"etf_date": product_date, "etf_pcr":{"$gt":0, "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_pcr", 1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'OperatingRatio 저': 
            query = {"etf_date": product_date, "etf_operating_ratio":{"$gt":0}}
            result = collection.find(query).sort("etf_operating_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'OperatingRatio 고': 
            query = {"etf_date": product_date, "etf_operating_ratio":{"$gt":0}}
            result = collection.find(query).sort("etf_operating_ratio", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'OperatingRatio':
            query = {"etf_date": product_date, "etf_operating_ratio":{"$gt":0, "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_operating_ratio", 1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'ProfitRatio 저': 
            query = {"etf_date": product_date, "etf_profit_ratio":{"$gt":0}}
            result = collection.find(query).sort("etf_profit_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'ProfitRatio 고': 
            query = {"etf_date": product_date, "etf_profit_ratio":{"$gt":0}}
            result = collection.find(query).sort("etf_profit_ratio", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'ProfitRatio':
            query = {"etf_date": product_date, "etf_profit_ratio":{"$gt":0, "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_profit_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'DebtRatio 저': 
            query = {"etf_date": product_date, "etf_debt_ratio":{"$gt":0}}
            result = collection.find(query).sort("etf_debt_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'DebtRatio 고': 
            query = {"etf_date": product_date, "etf_debt_ratio":{"$gt":0}}
            result = collection.find(query).sort("etf_debt_ratio", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'DebtRatio':
            query = {"etf_date": product_date, "etf_debt_ratio":{"$gt":0, "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_debt_ratio", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'ROE 저': 
            query = {"etf_date": product_date, "etf_roe":{"$gt":0}}
            result = collection.find(query).sort("etf_roe", 1).limit(product_count_per_strategy)

        elif strategy_kind == 'ROE 고': 
            query = {"etf_date": product_date, "etf_roe":{"$gt":0}}
            result = collection.find(query).sort("etf_roe", -1).limit(product_count_per_strategy)
            
        elif strategy_kind == 'ROE':
            query = {"etf_date": product_date, "etf_roe":{"$gt":0, "$gte":min_value, "$lte":max_value}}
            result = collection.find(query).sort("etf_roe", 1).limit(product_count_per_strategy)

        else:
            query = {"etf_date": product_date, "etf_code":strategy_kind}
            result = collection.find(query)

        for document in result:
            product_ticker_list.append(document['etf_code'])
        return product_ticker_list

    def getProductPrice(self, product_date, product_ticker):
        query = {"etf_date": product_date, "etf_code":product_ticker}

        documents = collection.find(query)
        for document in documents:
            result = document['etf_price']
        return result
    
    
    def getRebalanceStrategyProductCode(self,backtesting_date):
        self.strategy_product_code = [[] for _ in range(self.strategy_count)]
        for i,strategy_query in enumerate(self.strategy_sql_query_list):
            product_ticker_list = self.getProductTicker(strategy_query, backtesting_date)
            for product_ticker in product_ticker_list:
                self.strategy_product_code[i].append(product_ticker)
        
        
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
            for j in range(self.strategy_product_number[i]):
                self.purchase_assignment_amount_list[i].append((strategy_ratio*to_calculate_money//100)//self.strategy_product_number[i])
                if j == 0:
                    self.current_balance+=(strategy_ratio*to_calculate_money//100)%self.strategy_product_number[i]
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j,product_price in enumerate(product_price_list):
                self.strategy_accumulate_product_count[i].append(int(self.purchase_assignment_amount_list[i][j]//product_price))
                self.current_balance+=self.purchase_assignment_amount_list[i][j]%product_price


    def getStrategyProductValue(self):
        self.strategy_product_value = [[] for _ in range(self.strategy_count)]
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j, product_price in enumerate(product_price_list):
                self.strategy_product_value[i].append(product_price * self.strategy_accumulate_product_count[i][j])
        # print('금융상품들 가치 :', self.strategy_product_value)
        
        
    def getStrategyValue(self):
        self.strategy_value = list()
        for product_value in self.strategy_product_value:
            self.strategy_value.append(sum(product_value))
        # print('전략별 가치 :', self.strategy_value)
    
    
    def getPortfolioValueWithoutBalanceAndBalance(self,backtesting_date):
        self.portfolio_value_without_balance_account[backtesting_date] = sum(self.strategy_value)
        self.current_portfolio_value_without_balance = sum(self.strategy_value)
        # print('포트폴리오 가치 기록(잔액X) :', self.portfolio_value_without_balance_account)
        self.balance_account[backtesting_date]=self.current_balance
        # print('잔액기록 :', self.balance_account)
        
    
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
            for j in range(self.strategy_product_number[i]):
                self.purchase_assignment_amount_list[i].append((strategy_ratio*to_calculate_money//100)//self.strategy_product_number[i])
                if j == 0:
                    self.current_balance+=(strategy_ratio*to_calculate_money//100)%self.strategy_product_number[i]
        
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j,product_price in enumerate(product_price_list):
                self.strategy_add_product_count[i].append(int(self.purchase_assignment_amount_list[i][j]//product_price))
                self.current_balance+=self.purchase_assignment_amount_list[i][j]%product_price
        for i,add_product_count_list in enumerate(self.strategy_add_product_count):
            for j, add_product_count in enumerate(add_product_count_list):
                self.strategy_accumulate_product_count[i][j]+=add_product_count

    
    def getRealPortfolioValue(self):
        portfolio_value_account=dict()
        for backtesting_date in self.backtesting_date_list:
            portfolio_value_account[backtesting_date] = self.balance_account[backtesting_date]+self.portfolio_value_without_balance_account[backtesting_date]
        return portfolio_value_account
            
    
    def doBackTest(self):  
        for tax in range(2):
            self.tax_benefit_money = self.portfolio_start_money
            self.current_balance = self.portfolio_start_money
            self.current_portfolio_value_without_balance = 0
            self.input_money_sum = self.portfolio_start_money
            
            for backtesting_date in self.backtesting_date_list:
                if (backtesting_date in self.tax_benfit_date_list) and tax ==1:
                    if self.tax_benefit_money >= 7000000:
                        self.tax_benefit_money = 7000000 * 0.165 
                    else:
                        self.tax_benefit_money *= 0.165
                    
                    self.current_balance += self.tax_benefit_money
                    self.tax_benefit_money =0
        
                if backtesting_date in self.rebalance_date_list:
                    if backtesting_date in self.input_date_list and backtesting_date != self.rebalance_date_list[0]:  # '조회날짜'가 '납입날짜 리스트'에 있고, '조회날짜'가 '리밸런싱 날짜리스트'의 첫번째 날짜가 아니면(리밸런싱하는 첫번째 날이면 납입금액을 더하지 않아야 하므로)
                        self.getInputwork(backtesting_date)
                    self.getRebalanceStrategyProductCode(backtesting_date)
                    self.getStrategyProductPrice(backtesting_date)
                    self.getRebalanceStrategyProductCountandBalance(self.current_balance + self.current_portfolio_value_without_balance)
                    self.getStrategyProductValue()
                    self.getStrategyValue()
                    self.getPortfolioValueWithoutBalanceAndBalance(backtesting_date)
                
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
                self.portfolio_without_tax_benefit_account = self.getRealPortfolioValue() # 포트폴리오 가치 추이
                self.portfolio_object.portfolio_account_without_tax_benefit = self.portfolio_object.getPortVariables(self.portfolio_without_tax_benefit_account, self.input_money_to_portfolio_account) # 포트폴리오 출력결과 변수
                self.portfolio_object.portfolio_receive_without_tax_benefit = self.portfolio_object.receiptSimul(self.portfolio_object.portfolio_account_without_tax_benefit,10) # 포트폴리오 수령방법, 몇년 수령할지 입력(10년 디폴트이고 나중에 사용자 맞게 수정 가능)

            elif tax == 1:# 세제혜택 0 인 경우 결과값들 입력
                self.portfolio_with_tax_benefit_account = self.getRealPortfolioValue()
                self.portfolio_object.portfolio_account_with_tax_benefit = self.portfolio_object.getPortVariables(self.portfolio_with_tax_benefit_account, self.input_money_to_portfolio_account)
                self.portfolio_object.portfolio_receive_with_tax_benefit = self.portfolio_object.receiptSimul(self.portfolio_object.portfolio_account_with_tax_benefit,10) # 몇년 수령할지 입력(10년 디폴트이고 나중에 사용자 맞게 수정 가능)


# 실행하는 부분이 메인함수이면 실행 
if __name__ == "__main__":

    
    host = 'localhost'  # 호스트 주소
    port = 27017  # 포트 번호

    # MongoDB에 연결
    client = MongoClient(host, port)

    # 데이터베이스 선택
    db = client['snowball_data_engineer']  # 여기서 'mydatabase'는 사용하려는 데이터베이스 이름입니다.

    # 컬렉션 선택 (테이블과 유사한 개념)
    collection = db['etf_evaluate']  # 여기서 'mycollection'은 사용하려는 컬렉션 이름입니다.

    # 코드 실행 시간 측정 시작
    start_time = time.time()    
    print("start!!")
    print()
    backtest_object = Backtest('189', 'test', 1000000, [60,40], '2018-10-01', '2019-05-01', 3, '0', 600000, [['PER 저', 2, 0, 0], ['OperatingRatio', 2, 8, 15]])
    backtest_object.doBackTest()
    
    # 코드 실행 시간 측정 종료
    end_time = time.time()
    
    # 실행 시간 계산
    execution_time = end_time - start_time
    print()
    print("end!!")
    print("Execution Time:", execution_time, "seconds")
    
    client.close()  