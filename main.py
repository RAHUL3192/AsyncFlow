
from fastapi import FastAPI

app = FastAPI(title="AsyncFlow")

@app.get("/")
async def home():
    return {
        "project": "AsyncFlow",
        "message": "API is running successfully!"
    }

@app.get("/health")
async def health_check():
    return {
        "status": "healthy"
    }
