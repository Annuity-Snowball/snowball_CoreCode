from fastapi import APIRouter, HTTPException
from src.model.portfolioModel import Portfolio
from motor.motor_asyncio import AsyncIOMotorClient

router = APIRouter()
client = AsyncIOMotorClient("mongodb://host.docker.internal:27017/")
db = client["insert_server"]
collection = db["portfolio"]

# 저장하기
@router.post("/save")
async def savePortfolio():
    print("유저의 포트폴리오를 저장했습니다")
    return "유저의 포트폴리오를 저장했습니다"
    
# 삭제하기
@router.delete("/")
async def deletePortfolio():
    print("유저의 포트폴리오를 삭제했습니다.")
    return "유저의 포트폴리오를 삭제했습니다."
    
    
# 유저의 포트폴리오 리스트로 가져오기(간단한 데이터만), 페이지네이션 해서
@router.get("/portfolioList")
async def getPortfolioList():
    print("유저의 포트폴리오의 리스트를 가져왔습니다")
    return "유저의 포트폴리오의 리스트를 가져왔습니다"
    
# 유저의 포트폴리오 상세정보 가져오기
@router.get("/singlePortfolio")
async def getSinglePortfolio():
    print("유저의 단일 포트폴리오를 가져왔습니다.")
    return "유저의 단일 포트폴리오를 가져왔습니다."

# 유저의 포트폴리오 비교, 비교할 데이터들만 가져오기
@router.get("/comparePortfolio")
async def getPortfolioCompareDate():
    print("유저의 포트폴리오를 비교합니다")
    return "유저의 포폴을 비교합니다."
