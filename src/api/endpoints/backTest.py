from fastapi import APIRouter
from src.model.portfolioModel import Portfolio
from src.core.second_build import Backtest

router = APIRouter()

@router.post("/getInfo")
async def backtestAPI(portfolioInput: Portfolio):


    backtest_object = setBackTest(portfolioInput)

    backtest_object.doBackTest()

    return {
        "with": backtest_object.portfolio_with_tax_benefit_account,
        "without": backtest_object.portfolio_without_tax_benefit_account
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
