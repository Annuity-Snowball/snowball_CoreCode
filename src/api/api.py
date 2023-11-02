from fastapi import APIRouter
from src.api.endpoints.backTest import getData
from src.api.endpoints.backTest import userInteraction
from src.api.endpoints.backTest import globalIndicator

api_router = APIRouter()
api_router.include_router(getData.router, prefix="/backtest", tags=["backtest"])
api_router.include_router(userInteraction.router, prefix="/userInteraction", tags=["userInteraction"])
api_router.include_router(globalIndicator.router, prefix="/globalIndicator", tags=["globalIndicator"])