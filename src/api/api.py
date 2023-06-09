from fastapi import APIRouter
from src.api.endpoints.backTest import getData

api_router = APIRouter()
api_router.include_router(getData.router, prefix="/backtest", tags=["backtest"])