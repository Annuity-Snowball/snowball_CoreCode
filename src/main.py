from fastapi import FastAPI
from first_build import Backtest, Portfolio, Strategy
from typing import Optional
from pydantic import BaseModel

class Strategi(BaseModel):
    id: int
    productName: str
    productNumber: int
    productRate: int
    productStartRate: int
    productEndRate: int
    
class PortfolioInput(BaseModel):
    id: int
    name: str
    startDate :str
    endDate: str
    rebalancingDuration: int
    inputMoney: int
    startMoney: int
    inputWay: int
    strategyNumber: int
    strategies:list[Strategi]

app = FastAPI()

@app.get("/")
async def root():
    return {
        "message":"this is root"
    }

@app.post("/backtest")
async def backtestAPI(portfolioInput: PortfolioInput):
    
    # backtest_object = backtest('123456', 'test', 1000000, [40,60], '2017-10-01', '2018-05-01', 3, '0', 600000, [['PER 저', 2, 0, 0], ['PER 고', 3, 0, 0]])
    
    strategyRateList = list()
    strategyList = list()
    
    for i in portfolioInput.strategies:
        strategyRateList.append(i.productRate)
        tempList = list()
        tempList.append(i.productName)
        tempList.append(i.productNumber)
        tempList.append(i.productStartRate)
        tempList.append(i.productEndRate)
        strategyList.append(tempList)
        
        
    backtest_object = Backtest(
        str(portfolioInput.id), 
        portfolioInput.name,
        portfolioInput.startMoney, 
        strategyRateList, 
        portfolioInput.startDate,
        portfolioInput.endDate,
        portfolioInput.rebalancingDuration,
        str(portfolioInput.inputWay),
        portfolioInput.inputMoney,
        strategyList
    )
    
    await backtest_object.doBacktest()

    
    print('date :', portfolioInput.startDate)
    
    return {
        "test":portfolioInput.startDate
    }