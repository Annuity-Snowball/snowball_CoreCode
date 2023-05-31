import json
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from src.api.api import api_router
from src.utils.numpyEncoder import NumpyEncoder

app = FastAPI()

origins = [
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router)

# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
