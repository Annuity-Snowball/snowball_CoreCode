from dataclasses import dataclass

from fastapi import FastAPI

from pydantic import BaseModel

from src.first_build import Backtest


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
    # 값 제대로 들어온지 확인하기
    backtest_object.doBacktest()
    print(backtest_object.portfolio_object.portfolio_account_without_tax_benefit)
    return {
        "with":backtest_object.portfolio_object.portfolio_account_with_tax_benefit,
        "without":backtest_object.portfolio_object.portfolio_account_without_tax_benefit,
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

# @app.post("/backtest")
# async def backtestAPI(portfolioInput: PortfolioInput):

#     # backtest_object = backtest('123456', 'test', 1000000, [40,60], '2017-10-01', '2018-05-01', 3, '0', 600000, [['PER 저', 2, 0, 0], ['PER 고', 3, 0, 0]])

#     strategyRateList = list()
#     strategyList = list()

#     for i in portfolioInput.strategies:
#         strategyRateList.append(i.productRate)
#         tempList = list()
#         tempList.append(i.productName)
#         tempList.append(i.productNumber)
#         tempList.append(i.productStartRate)
#         tempList.append(i.productEndRate)
#         strategyList.append(tempList)


#     backtest_object = Backtest(
#         str(portfolioInput.id),
#         portfolioInput.name,
#         portfolioInput.startMoney,
#         strategyRateList,
#         portfolioInput.startDate,
#         portfolioInput.endDate,
#         portfolioInput.rebalancingDuration,
#         str(portfolioInput.inputWay),
#         portfolioInput.inputMoney,
#         strategyList
#     )



#     return await fuck(backtest_object)

# async def fuck(back):
#     backtest_object = back
#     await backtest_object.doBacktest()

#     print(backtest_object.portfolio_object.portfolio_account_without_tax_benefit)
#     print()
#     print(backtest_object.portfolio_object.portfolio_account_with_tax_benefit)

#     return {
#         "without": backtest_object.portfolio_object.portfolio_account_without_tax_benefit,
#         "with": backtest_object.portfolio_object.portfolio_account_with_tax_benefit
#     }
