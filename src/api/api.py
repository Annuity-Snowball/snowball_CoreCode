from fastapi import APIRouter
from src.api.endpoints import backTest

api_router = APIRouter()
api_router.include_router(backTest.router, prefix="/backtest", tags=["backtest"])