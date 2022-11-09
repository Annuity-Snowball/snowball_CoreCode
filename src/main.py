from fastapi import FastAPI

app = FastAPI()

@app.get("/backTest")
async def root():
    return {
        "message":"fuck the world"
    }
