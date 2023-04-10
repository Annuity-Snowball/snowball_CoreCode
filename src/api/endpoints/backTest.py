from fastapi import APIRouter
from src.model.portfolioModel import Portfolio
from src.core.second_build import Backtest
from pymongo import MongoClient

router = APIRouter()
client = MongoClient("mongodb://localhost:27017/")
db = client["insert_server"]
collection = db["portfolio"]

@router.post("/getInfo")
async def backtestAPI(portfolioInput: Portfolio):

    backtest_object = setBackTest(portfolioInput)

    backtest_object.doBackTest()

    info =  {
        "onlyMoney": backtest_object.input_money_to_portfolio_account,
        "value_with_tax": backtest_object.portfolio_with_tax_benefit_account,
        "value_without_tax": backtest_object.portfolio_without_tax_benefit_account,
        "result_with_tax": backtest_object.portfolio_object.portfolio_account_with_tax_benefit,
        "result_without_tax": backtest_object.portfolio_object.portfolio_account_without_tax_benefit,
        "recieve_with_tax": backtest_object.portfolio_object.portfolio_receive_without_tax_benefit,
        "recieve_without_tax": backtest_object.portfolio_object.portfolio_receive_without_tax_benefit
    }

    collection.insert_one(info)

    return {
        "onlyMoney": backtest_object.input_money_to_portfolio_account,
        "value_with_tax": backtest_object.portfolio_with_tax_benefit_account,
        "value_without_tax": backtest_object.portfolio_without_tax_benefit_account,
        "result_with_tax": backtest_object.portfolio_object.portfolio_account_with_tax_benefit,
        "result_without_tax": backtest_object.portfolio_object.portfolio_account_without_tax_benefit,
        "recieve_with_tax": backtest_object.portfolio_object.portfolio_receive_without_tax_benefit,
        "recieve_without_tax": backtest_object.portfolio_object.portfolio_receive_without_tax_benefit
    }

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
