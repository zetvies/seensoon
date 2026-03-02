from fastapi import FastAPI, Request, BackgroundTasks
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import os
import aiohttp
import asyncio
import json
from dotenv import load_dotenv
from typing import List
from starlette.websockets import WebSocket, WebSocketDisconnect

# Load surroundings
base_path = os.path.dirname(os.path.abspath(__file__))
# Try to load .env from Server folder if not in Final
load_dotenv(os.path.join(base_path, ".env"))
load_dotenv(os.path.join(base_path, "..", "Server", ".env"))

app = FastAPI()

# Config
PI_IP = os.getenv("RASPBERRY_PI_IP", "100.118.120.26")
PI_PORT = os.getenv("RASPBERRY_PI_PORT", "5001")
PI_URL = f"http://{PI_IP}:{PI_PORT}"

# Global state for WebSockets
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            await connection.send_json(message)

manager = ConnectionManager()

# Paths
assets_path = os.path.join(base_path, "assets")
if os.path.exists(assets_path):
    app.mount("/assets", StaticFiles(directory=assets_path), name="assets")

@app.get("/")
async def read_index():
    return FileResponse(os.path.join(base_path, "index.html"))

# --- PI PROXY ENDPOINTS ---

@app.post("/api/preview/start")
async def proxy_preview_start():
    print(f"▶️ Preview start requested → {PI_URL}/preview/start")
    async with aiohttp.ClientSession() as session:
        # Always stop first to kill any lingering rpicam process
        try:
            await session.post(f"{PI_URL}/preview/stop", timeout=aiohttp.ClientTimeout(total=3))
            print("⏹️ Stopped old preview first")
            await asyncio.sleep(1)  # Give camera time to release
        except:
            pass
        
        try:
            async with session.post(f"{PI_URL}/preview/start", timeout=aiohttp.ClientTimeout(total=5)) as resp:
                data = await resp.json()
                if data.get("success"):
                    print("✅ Preview started successfully")
                    return {"success": True, "stream_url": "/api/stream"}
                print(f"❌ Preview start failed: {data.get('error')}")
                return {"success": False, "error": data.get("error")}
        except Exception as e:
            print(f"❌ Preview start error: {e}")
            return {"success": False, "error": str(e)}

@app.post("/api/preview/stop")
async def proxy_preview_stop():
    print("⏹️ Preview stop requested")
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{PI_URL}/preview/stop", timeout=5) as resp:
                print("✅ Preview stopped")
                return await resp.json()
        except Exception as e:
            print(f"❌ Preview stop error: {e}")
            return {"success": False, "error": str(e)}

@app.get("/api/stream")
async def proxy_stream(request: Request):
    print(f"📹 Stream proxy connecting to {PI_URL}/stream")
    async def stream_gen():
        timeout = aiohttp.ClientTimeout(total=None, sock_read=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                async with session.get(f"{PI_URL}/stream") as resp:
                    print(f"📹 Stream connected (status: {resp.status})")
                    chunk_count = 0
                    async for chunk in resp.content.iter_any():
                        chunk_count += 1
                        if chunk_count == 1:
                            print(f"📹 First chunk received ({len(chunk)} bytes)")
                        elif chunk_count % 200 == 0:
                            print(f"📹 Streamed {chunk_count} chunks...")
                        yield chunk
                    print(f"📹 Stream ended after {chunk_count} chunks")
            except asyncio.CancelledError:
                print("📹 Stream: client disconnected")
            except Exception as e:
                print(f"❌ Stream error: {e}")

    return StreamingResponse(
        stream_gen(),
        media_type="multipart/x-mixed-replace; boundary=frame"
    )

@app.post("/api/capture")
async def proxy_capture():
    print(f"📷 Capture triggered → {PI_URL}/capture")
    async with aiohttp.ClientSession() as session:
        try:
            # Capture can take a while (countdown + processing)
            async with session.post(f"{PI_URL}/capture", timeout=aiohttp.ClientTimeout(total=60)) as resp:
                result = await resp.json()
                print(f"📷 Capture result: {result}")
                return result
        except Exception as e:
            print(f"❌ Capture error: {e}")
            return {"success": False, "error": str(e)}

@app.post("/api/print")
async def proxy_print(request: Request):
    """Forward receipt image to Pi for thermal printing."""
    print(f"🖨️ Print requested → {PI_URL}/print")
    data = await request.json()
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(
                f"{PI_URL}/print",
                json=data,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as resp:
                result = await resp.json()
                print(f"🖨️ Print result: {result}")
                return result
        except Exception as e:
            print(f"❌ Print error: {e}")
            return {"success": False, "error": str(e)}

@app.post("/api/notify")
async def receive_notify(request: Request):
    """Receive notifications from Pi and broadcast to browser."""
    data = await request.json()
    await manager.broadcast(data)
    return {"success": True}

# --- WEBSOCKET FOR BROWSER ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

if __name__ == "__main__":
    print(f"Potboy Final Server starting...")
    print(f"Target Pi: {PI_URL}")
    print(f"Local URL: http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=8000)
