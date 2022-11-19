from dataclasses import dataclass

from fastapi import FastAPI

from pydantic import BaseModel

from src.second_build import Backtest


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
    
    strategyRateList = list()
    strategyList = list()


    
    for i in portfolioInput.strategies:
        strategyRateList.append(int(i.productRate))
        tempList = list()
        tempList.append(str(i.productName))
        tempList.append(int(i.productNumber))
        tempList.append(int(i.productStartRate))
        tempList.append(int(i.productEndRate))
        strategyList.append(tempList)
        
        
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
    # 값 제대로 들어온지 확인하기
    backtest_object.doBackTest()

    
    return {
        "with": backtest_object.portfolio_with_tax_benefit_account,
        "without": backtest_object.portfolio_without_tax_benefit_account
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)