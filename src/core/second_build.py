from datetime import date, timedelta
import pymysql
import copy
import datetime as dt
from pandas.tseries.offsets import *
from pandas_datareader import data as pdr
import pandas as pd
import configparser

from src.core.getDatainfo import getDailyDateInfo, getPayInDateInfo, getRebalanceDateInfo, getYearlyDateInfo

config = configparser.ConfigParser()
config.read('src/core/config.ini')
db_host=config['DB']['HOST']
db_user=config['DB']['USER']
db_port=int(config['DB']['PORT'])
db_pass=config['DB']['PASSWORD']
db_name=config['DB']['NAME']

db = pymysql.connect(
        host=db_host, 
        port=db_port, 
        user=db_user, 
        passwd=db_pass,
        db=db_name,
        charset="utf8"
        )
snowball=db.cursor()

# 포트폴리오 클래스 생성
class Portfolio():
    """
    포트폴리오 클래스 입니다
    추가적으로 백테스트 결과 반환하는 메소드들 개별로 추가해서 작성해야 합니다!
    """
    
    # 객체 생성할 때 초기화하는 매소드 입니다
    def __init__(self,portfolio_id, portfolio_name, start_money, strategy_ratio, start_time, end_time, rebalance_cycle, input_type, input_money):
        """
        포트폴리오 아이디를 데이터베이스에서 받는 부분은 구현 필요 합니다
        오류를 출력하는 부분(print(error))들은 예외 처리로 수정 필요합니다.
        입력받은 값들을 데이터베이스에 업데이트 하는 부분 구현이 필요합니다
        
        Args:
            portfolio_name (str): 포트폴리오명
            strategy_count (int: 포트폴리오를 구성하는 전략의 개수
            start_time (str): 조회 시작날짜
            end_time (str): 조회 끝날짜
            rebalance_cycle (int): 리밸런싱하는 주기(달별로)
            input_type (str): MF,ML,YF,YL 식으로 매달,매초,연초,연말 등의 타입을 입력받을 수 있습니다
            input_money (int): 납입금액, 한번 납입할 때 얼마씩 납입하는 지 입력
        """
        
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
    
    def get_returns(self,df):
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
    
    def get_winRate(self,df):
        # 리밸런싱 날짜 리스트받아서, 해당하는 날짜들의 승률 계산해서 반환
        win_count = 0

        for i in df.index:
            if df.loc[i, 'return'] >= 0:
                win_count = win_count + 1

        return round((win_count) / (len(df.index)) * 100, 2)

    
    def get_portVariables(self,dict_realvalue, dict_inputmoney):
        portfolio_result = dict()
        values = dict()
        for key in dict_inputmoney.keys():
            values[key] = list()
            values[key].append(float(dict_inputmoney[key]))
            values[key].append(dict_realvalue[key])

        df = pd.DataFrame.from_dict(values, orient='index', columns=['seed', 'value'])  # 넘겨받은 사전 데이터 데이터프레임으로 변환

        self.get_returns(df)  # 수익률 계산
        win_rate = self.get_winRate(df)  # 승률 계산해서 저장 (리밸런싱 날짜 기준)
        # print(df)
        # print("승률: ", win_rate, "%")
        # print(mdd)
        # print(df['return'].to_dict())
        portfolio_result['투입한 금액'] = dict_inputmoney[list(dict_inputmoney.keys())[-1]]
        portfolio_result['포트폴리오 가치'] = df.iloc[-1, 1]  # 포트폴리오 가치 저장, 초기값: 수령 직전 가치
        
        portfolio_result['총 수익률'] = df.iloc[-1, 2]
        portfolio_result['월 수익률 추이'] = self.monthlyReturn(dict_realvalue)
        portfolio_result['일별 수익률'] = dict(zip(df.index, df['return']))
        portfolio_result['승률'] = win_rate

        return portfolio_result
        
    def cal_receiptValue(self,year, value):
        if year == 10:
            return value
        return value / (11 - year) * 1.2
    
    
    def receipt_simul(self,portfolioResult, receiptYear):
        # 수령 시뮬레이션
        # 연간 수령한도: 계좌평가액 / (11 - 연금수령연차) * 1.2 -> 1년간 자유롭게 나누어 수령 가능 (수령하지 않는 것도 가능) - 매년 1월 1일 기준으로 평가
        # 연 1,200만원 이상 수령 시, 종합소득세 부과 (16.5%)
        # 수령 가능금액 확인
        # 0: 수령 시 운용 중지
        # 1: 수령 시 운용 유지
        # (수령 후 다음 해 잔액) = (잔액 x 포트폴리오 기간 내 평균수익률)

        cum_value = int(portfolioResult['포트폴리오 가치'])  # 포트폴리오 가치 저장, 초기값: 수령 직전 가치
        # print("수령 직전가치: ", cum_value)
        # print("평균수익률: ", mean_return)

        rtDict1 = dict()
        rtDict1['총 수령액'] = 0
        can_receiptValue = 0

        for i in range(1, receiptYear + 1):
            can_receiptValue = int(cum_value / (receiptYear - (i - 1)))  # 남은 금액을 남은 연차로 엔빵
            if i <= 10:  # 1~10년차의 경우
                if can_receiptValue > self.cal_receiptValue(i, cum_value):
                    can_receiptValue = int(self.cal_receiptValue(i, cum_value))  # 10년이내의 최대를 넘을 시 최대금액으로 제한
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
                can_receiptValue2 = int(self.cal_receiptValue(year, cum_value2))  # 당 해 수령가능 금액
            else:
                can_receiptValue2 = int(cum_value2)

            if can_receiptValue2 > 12000000:
                can_receiptValue2 = 12000000

            rtDict2[str(year) + '년차 수령금액'] = can_receiptValue2
            rtDict2['총 수령액'] = rtDict2['총 수령액'] + can_receiptValue2
            cum_value2 = int(cum_value2 - can_receiptValue2)
            year = year + 1

        rtDict2['잔액'] = cum_value2

        # print("수령방식: ", receiptWay)
        # print()
        # print("포트폴리오 누적 가치: ", cum_value)
        # print()
        # print("수익률 평균: ", mean_return)

        if rtDict1['잔액'] > 0:
            print("첫 번째 방식의 경우 잔액이 0보다 큽니다, 수령 기간을 늘리시길 권장드립니다.")
        rtList = [rtDict1, rtDict2]
        return rtList
    
    
              
# 전략 클래스 생성
class Strategy():
    """
    전략 클래스 입니다
    """
    
    # 객체 생성할 때 초기화하는 매소드 입니다
    def __init__(self,strategy_kind, product_count_per_strategy,min_value,max_value):
        """
        Args:
            strategy_kind (str): 전략종류(ex-'PER 저')를 입력 받음
            product_count_per_strategy (int): 한 전략에 해당하는 금융상품들 개수
        """
        
        self.strategy_kind=strategy_kind
        self.product_count_per_strategy = product_count_per_strategy
        if self.strategy_kind == 'PER':
            self.min_value = min_value
            self.max_value = max_value
        else:
            self.min_value = 0
            self.max_value = 0
        
    # 전략에 해당하는 금융상품티커를 조회하는데 사용하는 쿼리문  반환하는 매소드
    def getProductListQuery(self):
        """
        1. 평가지표(전략종류, 전략구성금융상품개수)들에 따라서 쿼리문들을 추가해야 한다 -> 추가 및 수정필요!!
        2. 평가지표
        날짜는 지정이 안되어 있는 쿼리문을 반환 합니다!
        """
        
        if self.strategy_kind == 'PER 저':
            # product_ticker, product_evaluate, estimated_per 명칭은 아직 미정 - 전략을 통해 선택할 금융상품개수까지 포함한 쿼리문
            self.sql_query='select product_ticker from product_evaluate order by per asc limit '+str(self.product_count_per_strategy)
        
        elif self.strategy_kind == 'PER 고':
            # product_ticker, product_evaluate, estimated_per 명칭은 아직 미정 - 전략을 통해 선택할 금융상품개수까지 포함한 쿼리문
            self.sql_query='select product_ticker from product_evaluate order by per desc limit '+str(self.product_count_per_strategy)
            
        # 평가지표의 범위를 입력받을 경우    
        elif self.strategy_kind == 'PER':
            # product_ticker, product_evaluate, estimated_per 명칭은 아직 미정 - 전략을 통해 선택할 금융상품개수까지 포함한 쿼리문
            self.sql_query='select product_ticker from product_evaluate where per >= '+str(self.min_value)+' and per <='+str(self.max_value)+' order by per asc limit '+str(self.product_count_per_strategy)
        
        elif self.strategy_kind == 'PBR 저':
            # product_ticker, product_evaluate, estimated_per 명칭은 아직 미정 - 전략을 통해 선택할 금융상품개수까지 포함한 쿼리문
            self.sql_query='select product_ticker from product_evaluate order by pbr asc limit '+str(self.product_count_per_strategy)
            
        elif self.strategy_kind == 'PBR 고':
        # product_ticker, product_evaluate, estimated_per 명칭은 아직 미정 - 전략을 통해 선택할 금융상품개수까지 포함한 쿼리문
            self.sql_query='select product_ticker from product_evaluate order by pbr desc limit '+str(self.product_count_per_strategy)
            
        # 위에서의 'PER 저', 'PER 고' 같이 모든 평가 지표들 마다 쿼리문을 작성할 것
        pass
        
        return self.sql_query



class Backtest(Portfolio):
    def __init__(self,portfolio_id, portfolio_name, portfolio_start_money, strategy_ratio_list, portfolio_start_time, 
                portfolio_end_time, rebalance_cycle, input_type, input_money, stratgy_input_info_list):
        
        
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
            self.strategy_sql_query_list.append(strategy_object.getProductListQuery()) # '전략'의 '전략종류를 통해 데이터베이스에서 정보를 가져올 쿼리문'들을 해당 리스트에 추가  ex) ['select product_date,product_ticker from product_evaluate order by per asc limit 2', 'select product_date,product_ticker from product_evaluate order by per desc limit 3']
          
            
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
        
        self.input_money_to_portfolio_account=dict() # 일별 납입 금액 기록
        self.balance_account = dict() # 잔액 기록
        self.portfolio_value_without_balance_account = dict() # 포트폴리오 가치(잔액X) 기록
        self.portfolio_without_tax_benefit_account = None # (세제혜택X)포트폴리오 가치 기록
        self.portfolio_with_tax_benefit_account = None # (세제혜택0)포트폴리오 가치 기록
        
        
    # 날짜지정이 안되어 있는 쿼리문에서 날짜를 지정하는 부분을 추가해서 반환하는 함수 
    def getProductTicker(self, sql_query, product_date):
        product_ticker_list=list()
        get_product_ticker_query=sql_query.split(' ')
        if get_product_ticker_query[4] == 'where':
            get_product_ticker_query.insert(5,"product_date = '"+str(product_date)+"' and")
        else:
            get_product_ticker_query.insert(4,"where product_date = '"+str(product_date)+"'") 
        get_product_ticker_query=" ".join(get_product_ticker_query)
        snowball.execute(get_product_ticker_query) 
        results=list(snowball.fetchall())
        for i in range(len(results)):
            results[i] = list(results[i]) 
            product_ticker_list.append(str(results[i][0]))
        return product_ticker_list


        # 날짜에 대응하는 금융상품의 가격을 가져오는 함수
    def getProductPrice(self, product_date, product_ticker):
        sql_query = "select high_price from price_"+product_ticker+" where product_date='"+product_date+"'"
        snowball.execute(sql_query) 
        result=snowball.fetchone()
        result=list(result)[0]
        return result
    
    
    def getRebalanceStrategyProductCode(self,backtesting_date):
        self.strategy_product_code = [[] for _ in range(self.strategy_count)]
        for i,strategy_query in enumerate(self.strategy_sql_query_list):
            product_ticker_list = self.getProductTicker(strategy_query, backtesting_date)
            for product_ticker in product_ticker_list:
                self.strategy_product_code[i].append(product_ticker)
        # print('리밸런싱하면서 구매할 금융상품 코드 :', self.strategy_product_code)
        
        
    def getStrategyProductPrice(self,backtesting_date):
        self.strategy_product_price = [[] for _ in range(self.strategy_count)]
        for i,product_code_list in enumerate(self.strategy_product_code):
            for product_ticker in product_code_list:
                self.strategy_product_price[i].append(self.getProductPrice(backtesting_date,product_ticker))
        # print('현재 금융상품 가격 :', self.strategy_product_price)
    
    
    def getRebalanceStrategyProductCountandBalance(self,to_calculate_money):
        # print('구매에 사용할 금액(잔액포함) :', to_calculate_money)
        self.purchase_assignment_amount_list= [[] for _ in range(self.strategy_count)]
        self.strategy_accumulate_product_count = [[] for _ in range(self.strategy_count)]
        self.current_balance = 0
        for i,strategy_ratio in enumerate(self.strategy_ratio_list):
            for j in range(self.strategy_product_number[i]):
                self.purchase_assignment_amount_list[i].append((strategy_ratio*to_calculate_money//100)//self.strategy_product_number[i])
                if j == 0:
                    self.current_balance+=(strategy_ratio*to_calculate_money//100)%self.strategy_product_number[i]
        # print('구매하려 할당한 금액 :', self.purchase_assignment_amount_list)
        # print('구매하려 할당한 후 잔액 :', self.current_balance)
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j,product_price in enumerate(product_price_list):
                self.strategy_accumulate_product_count[i].append(int(self.purchase_assignment_amount_list[i][j]//product_price))
                self.current_balance+=self.purchase_assignment_amount_list[i][j]%product_price
        # print('리밸런싱 후 금융상품들 개수 :',self.strategy_accumulate_product_count)
        # print('리밸런싱 후 현재 잔액 :', self.current_balance)


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
        # print('구매에 사용할 금액(잔액포함) :', to_calculate_money)
        self.purchase_assignment_amount_list= [[] for _ in range(self.strategy_count)]
        self.strategy_add_product_count = [[] for _ in range(self.strategy_count)]
        self.current_balance = 0
        for i,strategy_ratio in enumerate(self.strategy_ratio_list):
            for j in range(self.strategy_product_number[i]):
                self.purchase_assignment_amount_list[i].append((strategy_ratio*to_calculate_money//100)//self.strategy_product_number[i])
                if j == 0:
                    self.current_balance+=(strategy_ratio*to_calculate_money//100)%self.strategy_product_number[i]
        # print('구매하려 할당한 금액 :', self.purchase_assignment_amount_list)
        # print('구매하려 할당한 후 잔액 :', self.current_balance)
        for i,product_price_list in enumerate(self.strategy_product_price):
            for j,product_price in enumerate(product_price_list):
                self.strategy_add_product_count[i].append(int(self.purchase_assignment_amount_list[i][j]//product_price))
                self.current_balance+=self.purchase_assignment_amount_list[i][j]%product_price
        for i,add_product_count_list in enumerate(self.strategy_add_product_count):
            for j, add_product_count in enumerate(add_product_count_list):
                self.strategy_accumulate_product_count[i][j]+=add_product_count
        
        # print('구매할 금융상품들 개수 :', self.strategy_add_product_count)
        # print('구매한 후 금융상품 누적 개수 :', self.strategy_accumulate_product_count)
        # print('현재 잔액 :', self.current_balance)           
    
    def getRealPortfolioValue(self):
        portfolio_value_account=dict()
        for backtesting_date in self.backtesting_date_list:
            portfolio_value_account[backtesting_date] = self.balance_account[backtesting_date]+self.portfolio_value_without_balance_account[backtesting_date]
        return portfolio_value_account
            
    
    def doBackTest(self):  
        # tax가 0이면 세제혜택 X, tax가 1이면 세제혜택 O
        for tax in range(2):
            self.tax_benefit_money = self.portfolio_start_money
            self.current_balance = self.portfolio_start_money
            self.current_portfolio_value_without_balance = 0
            self.input_money_sum = self.portfolio_start_money
            
            for backtesting_date in self.backtesting_date_list:
                if (backtesting_date in self.tax_benfit_date_list) and tax ==1:# 세제혜택을 고려하고, 날짜가 세제환급받을 날짜들 중 하나이면
                    print('**********************************')
                    print('세제혜택 받을 금액 :', self.tax_benefit_money)
                    if self.tax_benefit_money >= 7000000: # 세제혜택 받을 금액이 700만원 이상이면(irp 기준)
                        self.tax_benefit_money = 7000000 * 0.165 # 700만원의 16.5% 금액을 환급
                    else:# 세제혜택 받을 금액이 700만원 이하이면
                        self.tax_benefit_money *= 0.165 # 금액의 16.5% 금액을 환급
                    
                    self.current_balance += self.tax_benefit_money
                    print('세제환급 받을 금액 :', self.tax_benefit_money)
                    print('세제혜택 받은 후 현재 총 잔액 : ',self.current_balance)
                    self.tax_benefit_money =0 # 세제혜택 받을 금액으로 0원으로 초기화
        
                if backtesting_date in self.rebalance_date_list:
                    # print("==================================")
                    # print(backtesting_date, "리밸런싱")
                    # print("==================================")
                    if backtesting_date in self.input_date_list and backtesting_date != self.rebalance_date_list[0]:  # '조회날짜'가 '납입날짜 리스트'에 있고, '조회날짜'가 '리밸런싱 날짜리스트'의 첫번째 날짜가 아니면(리밸런싱하는 첫번째 날이면 납입금액을 더하지 않아야 하므로)
                        self.getInputwork(backtesting_date)
                    self.getRebalanceStrategyProductCode(backtesting_date)
                    self.getStrategyProductPrice(backtesting_date)
                    self.getRebalanceStrategyProductCountandBalance(self.current_balance + self.current_portfolio_value_without_balance)
                    self.getStrategyProductValue()
                    self.getStrategyValue()
                    self.getPortfolioValueWithoutBalanceAndBalance(backtesting_date)
                    print()
                
                elif backtesting_date in self.input_date_list:
                    # print("==================================")
                    # print(backtesting_date, "납입날짜")
                    # print("==================================")
                    self.getStrategyProductPrice(backtesting_date)
                    self.getInputwork(backtesting_date)
                    self.updateProductcount(self.current_balance)
                    self.getStrategyProductValue()
                    self.getStrategyValue()
                    self.getPortfolioValueWithoutBalanceAndBalance(backtesting_date)
                    print()
                
                else:
                    # print("==================================")
                    # print(backtesting_date, "나머지 경우")
                    # print("==================================")
                    self.input_money_to_portfolio_account[backtesting_date] = self.input_money_sum
                    self.getStrategyProductPrice(backtesting_date)
                    # print('금융상품 코드 :', self.strategy_product_code)
                    # print('금융상품들 개수 :',self.strategy_accumulate_product_count)
                    self.getStrategyProductValue()
                    self.getStrategyValue()
                    self.getPortfolioValueWithoutBalanceAndBalance(backtesting_date)
                    print()
                    
            if tax == 0: # 세제혜택 X 인 경우 결과값들 입력
                self.portfolio_without_tax_benefit_account = self.getRealPortfolioValue() # 포트폴리오 가치 추이
                self.portfolio_object.portfolio_account_without_tax_benefit = self.portfolio_object.get_portVariables(self.portfolio_without_tax_benefit_account, self.input_money_to_portfolio_account) # 포트폴리오 출력결과 변수
                self.portfolio_object.portfolio_receive_without_tax_benefit = self.portfolio_object.receipt_simul(self.portfolio_object.portfolio_account_without_tax_benefit,10) # 포트폴리오 수령방법, 몇년 수령할지 입력(10년 디폴트이고 나중에 사용자 맞게 수정 가능)

            elif tax == 1:# 세제혜택 0 인 경우 결과값들 입력
                self.portfolio_with_tax_benefit_account = self.getRealPortfolioValue()
                self.portfolio_object.portfolio_account_with_tax_benefit = self.portfolio_object.get_portVariables(self.portfolio_with_tax_benefit_account, self.input_money_to_portfolio_account)
                self.portfolio_object.portfolio_receive_with_tax_benefit = self.portfolio_object.receipt_simul(self.portfolio_object.portfolio_account_with_tax_benefit,10) # 몇년 수령할지 입력(10년 디폴트이고 나중에 사용자 맞게 수정 가능)
        
        # print("*************** 세제혜택X ***************")
        # print()
        # print('포트폴리오 가치 추이(잔액포함0):',self.portfolio_without_tax_benefit_account)
        # print()
        # print('포트폴리오 납입금액 추이:', self.input_money_to_portfolio_account)
        # print()
        # print('포트폴리오 결과 :',self.portfolio_object.portfolio_account_without_tax_benefit)
        # print()
        # print('포트폴리오 수령방법 :',self.portfolio_object.portfolio_receive_without_tax_benefit)
        #
        # print()
        # print("*************** 세제혜택0 ***************")
        # print()
        # print('포트폴리오 가치 추이(잔액포함0):',self.portfolio_with_tax_benefit_account)
        # print()
        # print('포트폴리오 납입금액 추이:', self.input_money_to_portfolio_account)
        # print()
        # print('포트폴리오 결과 :', self.portfolio_object.portfolio_account_with_tax_benefit)
        # print()
        # print('포트폴리오 수령방법 :',self.portfolio_object.portfolio_receive_with_tax_benefit)
    

 
# 실행하는 부분이 메인함수이면 실행 
if __name__ == "__main__":

    db = pymysql.connect(host='localhost', port=3306, user='snowball_test', passwd='909012', db='snowball_core', charset='utf8') 
    snowball=db.cursor() 

    backtest_object = Backtest('123456', 'teststrategy', 5000000, [25,25,25,25], '2017-10-01', '2018-05-01', 3, '0', 700000, [['PER 저', 2, 0, 0], ['PER 고', 3, 0, 0], ['PER', 3, 10, 15], ['PBR 저', 3, 0, 0]])


    backtest_object.doBackTest()
    db.close()  
