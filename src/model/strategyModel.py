from pydantic import BaseModel

class Strategy(BaseModel):
    id: int
    productName: str
    productNumber: int
    productRate: int
    productStartRate: int
    productEndRate: int