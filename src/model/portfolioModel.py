from pydantic import BaseModel
from src.model.strategyModel import Strategy

class Portfolio(BaseModel):
    id: int
    username: str
    name: str
    startDate :str
    endDate: str
    rebalancingDuration: int
    inputMoney: int
    startMoney: int
    inputWay: int
    strategyNumber: int
    strategies:list[Strategy]                                  