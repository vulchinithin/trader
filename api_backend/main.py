from fastapi import FastAPI, WebSocket, WebSocketDisconnect
import uvicorn
import asyncio
import json
from typing import List

# --- Local Imports ---
# Need to add the path to find the other services
import sys
import os
if __package__ is None or __package__ == '':
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

from api_backend.kafka_consumer import kafka_consumer

# --- Connection Manager ---
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: str):
        for connection in self.active_connections:
            await connection.send_text(message)

manager = ConnectionManager()

# --- FastAPI App ---
app = FastAPI(
    title="Trader API",
    description="API for interacting with the trading system, getting live data, and managing models.",
    version="0.1.0",
)

@app.on_event("startup")
async def startup_event():
    """Starts the Kafka consumer on app startup."""
    await kafka_consumer.start()
    # Start the broadcasting task
    asyncio.create_task(broadcast_messages())

@app.on_event("shutdown")
async def shutdown_event():
    """Stops the Kafka consumer on app shutdown."""
    await kafka_consumer.stop()

async def broadcast_messages():
    """Consumes messages from Kafka and broadcasts them to all clients."""
    async for message in kafka_consumer.message_generator():
        # The message from Kafka is bytes, we can send it as is or decode it.
        # Let's decode and send as a JSON string (text).
        await manager.broadcast(message.decode('utf-8'))

@app.get("/health", tags=["General"])
async def health_check():
    """Simple health check endpoint."""
    return {"status": "ok"}

@app.websocket("/ws/market-data")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for streaming live market data.
    """
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Client disconnected")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
