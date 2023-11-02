import json
import pymongo
from datetime import datetime
from pymongo import MongoClient
from fastapi import APIRouter, HTTPException
from src.model.portfolioModel import Portfolio
from src.core.second_build import Backtest
from motor.motor_asyncio import AsyncIOMotorClient

from src.utils.numpyEncoder import NumpyEncoder

router = APIRouter()
client = AsyncIOMotorClient("mongodb://host.docker.internal:27017/")
db = client["insert_server"]
collection = db["portfolio_detail"]
summary_collection = db["portfolio_summary"]

client_for_date = MongoClient("host.docker.internal", 27017)
db_for_date = client_for_date['snowball_data_engineer']
evaluate_collection = db_for_date['etf_evaluate']


@router.post("/getInfo")
async def backtestAPI(portfolioInput: Portfolio):
    
    some_result = evaluate_collection.find_one(sort=[("etf_date", pymongo.DESCENDING)])
    date = str(some_result['etf_date'])[:10]
    createdAt = datetime.now()
    portfolioInput_summary = portfolioToDict(portfolioInput=portfolioInput, time=createdAt)
    
    
        
    try:
        backtest_object = setBackTest(portfolioInput)
        backtest_object.doBackTest(date)
        info =  {
            "onlyMoney": backtest_object.input_money_to_portfolio_account,
            "value_with_tax": backtest_object.portfolio_with_tax_benefit_account,
            "value_without_tax": backtest_object.portfolio_without_tax_benefit_account,
            "result_with_tax": backtest_object.portfolio_object.portfolio_account_with_tax_benefit,
            "result_without_tax": backtest_object.portfolio_object.portfolio_account_without_tax_benefit,
            "recieve_with_tax": backtest_object.portfolio_object.portfolio_receive_with_tax_benefit,
            "recieve_without_tax": backtest_object.portfolio_object.portfolio_receive_without_tax_benefit,
            "recent_etf_codes": backtest_object.recent_strategy_product_code
        }
        
        encoded_info = json.loads(json.dumps(info, cls=NumpyEncoder))
        
        encoded_info['username'] = portfolioInput_summary['username']
        encoded_info['created_date'] = createdAt
        encoded_info['portfolio_name'] = portfolioInput_summary['name']
        
        collection.insert_one(encoded_info)
        
        portfolioInput_summary["profit"] = backtest_object.portfolio_object.portfolio_receive_with_tax_benefit[1]["총 수령액"]
        portfolioInput_summary["profit_ratio"] = backtest_object.portfolio_object.portfolio_account_with_tax_benefit["총 수익률"]
        portfolioInput_summary["montly_profit_ratio"] = backtest_object.portfolio_object.portfolio_account_with_tax_benefit["월 수익률 추이"]["월 수익률 평균"]
        portfolioInput_summary["mdd"] = backtest_object.portfolio_object.portfolio_account_with_tax_benefit["mdd"]
        portfolioInput_summary["daily_lowest_profit_ratio"] = backtest_object.portfolio_object.portfolio_account_with_tax_benefit["일별 최저수익률"]
        
        summary_collection.insert_one(portfolioInput_summary)
        
        if "_id" in encoded_info:
            del encoded_info["_id"]
        
        result = dict()
        for K,V in encoded_info.items():
            result[K]=V
                
        if "_id" in result:
            del result["_id"]
        
        return result
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

def setBackTest(portfolioInput: Portfolio):
    strategyRateList = setStrategyRateList(portfolioInput)
    strategyList = setStrategyList(portfolioInput)

    backtest_object = Backtest(
        str(portfolioInput.id),
        str(portfolioInput.name),
        int(portfolioInput.startMoney),
        strategyRateList,
        str(portfolioInput.startDate),
        str(portfolioInput.endDate),
        int(portfolioInput.rebalancingDuration),
        str(portfolioInput.inputWay),
        int(portfolioInput.inputMoney),
        strategyList
    )

    return backtest_object

def setStrategyRateList(portfolioInput: Portfolio):
    strategyRateList = list()

    for i in portfolioInput.strategies:
        strategyRateList.append(int(i.productRate))

    return strategyRateList

def setStrategyList(portfolioInput: Portfolio):
    strategyList = list()

    for i in portfolioInput.strategies:
        tempList = list()
        tempList.append(str(i.productName))
        tempList.append(int(i.productNumber))
        tempList.append(int(i.productStartRate))
        tempList.append(int(i.productEndRate))
        strategyList.append(tempList)

    return strategyList

def portfolioToDict(portfolioInput: Portfolio, time: datetime):
    portfolio_dict = {}
    
    for key, value in portfolioInput.__dict__.items():
        if key != '__dict__' and not key.startswith('_'):
            if key == 'strategies':
                strategy_list = []
                for strategy in value:
                    strategy_dict = strategy_to_dict(strategy)
                    strategy_list.append(strategy_dict)
                portfolio_dict[key] = strategy_list
            else:
                portfolio_dict[key] = value
                
    portfolio_dict['hits'] = 0
    portfolio_dict['created_date'] = time
    return portfolio_dict

def strategy_to_dict(strategyInput):
    strategy_dict = {}
    
    for key, value in strategyInput.__dict__.items():
        if key != '__dict__' and not key.startswith('_'):
            strategy_dict[key] = value
            
    return strategy_dict