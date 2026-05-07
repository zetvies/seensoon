from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
import uvicorn
import os
import asyncio
import json
import uuid
from dotenv import load_dotenv
from typing import List, Optional
from starlette.websockets import WebSocket, WebSocketDisconnect

# Load env
base_path = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(base_path, ".env"))
load_dotenv(os.path.join(base_path, "..", "Server", ".env"))

app = FastAPI()

# ================================
# PI CONNECTION (reverse WebSocket)
# ================================

class PiConnection:
    """Manages the WebSocket connection from the Pi."""
    def __init__(self):
        self.ws: Optional[WebSocket] = None
        self.pending_requests: dict = {}  # id -> asyncio.Future
        self.latest_frame: Optional[bytes] = None
        self.frame_event = asyncio.Event()
        self.streaming = False

    @property
    def connected(self):
        return self.ws is not None

    async def send_command(self, action: str, data: dict = None, timeout: float = 30) -> dict:
        """Send a command to Pi and wait for response."""
        if not self.connected:
            return {"success": False, "error": "Pi not connected"}

        request_id = str(uuid.uuid4())[:8]
        msg = {"id": request_id, "action": action}
        if data:
            msg.update(data)

        # Create a future to await the response
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        self.pending_requests[request_id] = future

        try:
            await self.ws.send_json(msg)
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        except asyncio.TimeoutError:
            return {"success": False, "error": "Pi timeout"}
        except Exception as e:
            return {"success": False, "error": str(e)}
        finally:
            self.pending_requests.pop(request_id, None)

    def resolve_request(self, request_id: str, data: dict):
        """Resolve a pending request with response data."""
        future = self.pending_requests.get(request_id)
        if future and not future.done():
            future.set_result(data)

    def update_frame(self, frame_data: bytes):
        """Update the latest frame from Pi's stream."""
        self.latest_frame = frame_data
        self.frame_event.set()

pi = PiConnection()

# ================================
# BROWSER WEBSOCKET MANAGER
# ================================

class BrowserManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except:
                pass

browser_manager = BrowserManager()

# ================================
# STATIC FILES
# ================================

assets_path = os.path.join(base_path, "assets")
if os.path.exists(assets_path):
    app.mount("/assets", StaticFiles(directory=assets_path), name="assets")

@app.get("/")
async def read_index():
    return FileResponse(os.path.join(base_path, "index.html"))

# ================================
# PI WEBSOCKET ENDPOINT
# ================================

@app.websocket("/ws/pi")
async def pi_websocket(websocket: WebSocket):
    """Pi connects here. Receives commands, sends responses + stream frames."""
    await websocket.accept()
    pi.ws = websocket
    pi.streaming = False
    print("[PI] Connected!")

    try:
        while True:
            message = await websocket.receive()
            msg_type = message.get("type", "")

            # Handle disconnect
            if msg_type == "websocket.disconnect":
                print("[PI] Received disconnect message")
                break

            if msg_type == "websocket.receive":
                # Binary = MJPEG frame
                if "bytes" in message and message["bytes"]:
                    pi.update_frame(message["bytes"])
                # Text = JSON response
                elif "text" in message and message["text"]:
                    try:
                        data = json.loads(message["text"])
                        # Ignore heartbeats
                        if data.get("action") == "heartbeat":
                            continue
                        request_id = data.get("id")
                        if request_id:
                            pi.resolve_request(request_id, data)
                        # Handle special events (broadcast to browsers)
                        if data.get("event"):
                            await browser_manager.broadcast(data)
                    except json.JSONDecodeError:
                        pass
    except WebSocketDisconnect:
        print("[PI] Disconnected")
    except Exception as e:
        print(f"[PI] Error: {e}")
    finally:
        pi.ws = None
        pi.streaming = False
        pi.latest_frame = None

# ================================
# BROWSER WEBSOCKET ENDPOINT
# ================================

@app.websocket("/ws")
async def browser_websocket(websocket: WebSocket):
    """Browser connects here for real-time updates."""
    await browser_manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_json()
            # Print log messages from browser to server console
            if data.get("type") == "log":
                print(f"[BROWSER] {data.get('msg', '')}")
    except WebSocketDisconnect:
        browser_manager.disconnect(websocket)
    except Exception:
        browser_manager.disconnect(websocket)

# ================================
# API ENDPOINTS (browser calls these)
# ================================

@app.get("/api/pi/status")
async def pi_status():
    """Check if Pi is connected."""
    return {"connected": pi.connected}

@app.post("/api/preview/start")
async def api_preview_start():
    print("[API] Preview start (local/browser mode)")
    # We no longer need to command the Pi for preview
    return {"success": True, "stream_url": "/api/stream"}

@app.post("/api/preview/stop")
async def api_preview_stop():
    print("[API] Preview stop (local/browser mode)")
    return {"success": True}

@app.get("/api/stream")
async def api_stream():
    """Serve MJPEG stream from Pi's frames pushed over WebSocket."""
    print("[STREAM] Browser connected")

    async def generate():
        while pi.connected and pi.streaming:
            pi.frame_event.clear()
            try:
                await asyncio.wait_for(pi.frame_event.wait(), timeout=5)
            except asyncio.TimeoutError:
                continue

            if pi.latest_frame:
                yield (b'--frame\r\n'
                       b'Content-Type: image/jpeg\r\n\r\n' +
                       pi.latest_frame + b'\r\n')
        print("[STREAM] Ended")

    return StreamingResponse(
        generate(),
        media_type="multipart/x-mixed-replace; boundary=frame"
    )

@app.post("/api/capture")
async def api_capture():
    print("[API] Capture (handled by browser)")
    return {"success": True, "message": "Browser is capturing"}

@app.post("/api/print")
async def api_print(request: Request):
    print("[API] Print")
    data = await request.json()
    return await pi.send_command("print", data={"image": data.get("image", "")}, timeout=30)

# ================================
# BROWSER WEBSOCKET
# ================================

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await browser_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        browser_manager.disconnect(websocket)

# ================================
# MAIN
# ================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    print(f"Potboy Server starting on port {port}...")
    print(f"Local URL: http://localhost:{port}")
    print(f"Pi should connect to: ws://YOUR_IP:{port}/ws/pi")
    uvicorn.run(app, host="0.0.0.0", port=port)
