from fastapi import FastAPI
import uvicorn

# Create the FastAPI app instance
app = FastAPI(
    title="Trader API",
    description="API for interacting with the trading system, getting live data, and managing models.",
    version="0.1.0",
)

@app.get("/health", tags=["General"])
async def health_check():
    """
    Simple health check endpoint to confirm the API is running.
    """
    return {"status": "ok"}

# Add other endpoints here in the future
# For example:
# @app.get("/status/pipeline", tags=["Status"])
# async def get_pipeline_status():
#     # Logic to get status from Prefect or other services
#     return {"pipeline_status": "running"}

if __name__ == "__main__":
    # This allows running the app directly for development
    # In production, you would use a process manager like Gunicorn with Uvicorn workers
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
