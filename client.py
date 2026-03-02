#!/usr/bin/env python3
"""
Potboy Camera Client — Connects outbound to server via WebSocket.
Handles camera streaming, capture, and thermal printing.
"""

import os
import sys
import time
import threading
import subprocess
import argparse
import json
import base64
import asyncio
import signal

# ==============================
# CONFIG FROM .ENV
# ==============================
from dotenv import load_dotenv

# Load .env from current dir or Client dir
for env_path in ['.env', '../Client/.env', os.path.join(os.path.dirname(__file__), '.env')]:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

PRINTER_DEVICE = os.getenv('PRINTER_DEVICE', '/dev/usb/lp0')
CAMERA_INDEX = int(os.getenv('CAMERA_INDEX', '0'))
LED_PIN = int(os.getenv('LED_PIN', '17'))
BUZZER_PIN = int(os.getenv('BUZZER_PIN', '27'))
IMAGE_PATH = '/tmp/capture.jpg'

# Printer USB config
PRINTER_USB_VENDOR = int(os.getenv('PRINTER_USB_VENDOR', '0x0456'), 16)
PRINTER_USB_PRODUCT = int(os.getenv('PRINTER_USB_PRODUCT', '0x0808'), 16)
PRINTER_USB_IN_EP = int(os.getenv('PRINTER_USB_IN_EP', '0x81'), 16)
PRINTER_USB_OUT_EP = int(os.getenv('PRINTER_USB_OUT_EP', '0x03'), 16)

# ==============================
# CAMERA DETECTION
# ==============================

USE_RPICAM = False
RPICAM_INDEX = CAMERA_INDEX

def check_rpicam():
    try:
        result = subprocess.run(['rpicam-still', '--list-cameras'],
                                capture_output=True, text=True, timeout=5)
        return 'Available cameras' in result.stdout and 'No cameras' not in result.stdout
    except:
        return False

def check_opencv_camera():
    try:
        import cv2
        cap = cv2.VideoCapture(CAMERA_INDEX)
        ret = cap.isOpened()
        cap.release()
        return ret
    except:
        return False

if check_rpicam():
    USE_RPICAM = True
    print("[OK] Camera: rpicam (Arducam/libcamera)")
elif check_opencv_camera():
    USE_RPICAM = False
    print("[OK] Camera: OpenCV (USB webcam)")
else:
    print("[WARN] No camera detected!")

# ==============================
# GPIO (optional)
# ==============================

GPIO_ENABLED = False
led = None
buzzer = None

try:
    from gpiozero import LED, Buzzer
    led = LED(LED_PIN)
    buzzer = Buzzer(BUZZER_PIN)
    led.off()
    buzzer.off()
    GPIO_ENABLED = True
    print("[OK] GPIO enabled (LED + Buzzer)")
except Exception as e:
    print(f"[WARN] GPIO disabled: {e}")

# ==============================
# CAMERA STREAMING
# ==============================

stream_process = None
stream_lock = threading.Lock()
stream_frame = None
preview_active = False
capture_in_progress = False

def stop_stream_process():
    global stream_process
    if stream_process:
        try:
            stream_process.terminate()
            stream_process.wait(timeout=3)
        except:
            try:
                stream_process.kill()
            except:
                pass
        stream_process = None

def generate_frames():
    """Generate MJPEG frames from camera. Yields raw JPEG bytes."""
    global stream_frame, stream_process

    if USE_RPICAM:
        cmd = [
            'rpicam-vid', '-t', '0', '--nopreview',
            '--camera', str(RPICAM_INDEX),
            '--framerate', '15', '--codec', 'mjpeg', '-o', '-'
        ]

        try:
            # Kill any lingering rpicam processes
            subprocess.run(['pkill', '-9', 'rpicam'], capture_output=True)
            time.sleep(0.3)

            print(f"[STREAM] Starting rpicam-vid")
            stream_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            buffer = b''
            frame_count = 0

            while preview_active and not capture_in_progress:
                chunk = stream_process.stdout.read(4096)
                if not chunk:
                    break
                buffer += chunk

                while True:
                    start = buffer.find(b'\xff\xd8')
                    end = buffer.find(b'\xff\xd9')
                    if start != -1 and end != -1 and end > start:
                        frame = buffer[start:end + 2]
                        buffer = buffer[end + 2:]
                        with stream_lock:
                            stream_frame = frame
                        frame_count += 1
                        if frame_count == 1:
                            print(f"[STREAM] First frame ({len(frame)} bytes)")
                        elif frame_count % 100 == 0:
                            print(f"[STREAM] {frame_count} frames")
                        yield frame
                    else:
                        break

            print(f"[STREAM] Ended after {frame_count} frames")
            stop_stream_process()
        except Exception as e:
            print(f"[ERR] Stream error: {e}")
            stop_stream_process()
    else:
        import cv2
        cap = cv2.VideoCapture(CAMERA_INDEX)
        while preview_active and not capture_in_progress:
            ret, frame = cap.read()
            if ret:
                _, jpeg = cv2.imencode('.jpg', frame)
                frame_bytes = jpeg.tobytes()
                with stream_lock:
                    stream_frame = frame
                yield frame_bytes
            time.sleep(1 / 15)
        cap.release()

# ==============================
# CAPTURE
# ==============================

def capture_image():
    """Capture image with camera."""
    if USE_RPICAM:
        cmd = [
            'rpicam-still', '-o', IMAGE_PATH,
            '-t', '1000', '-n',
            '--camera', str(RPICAM_INDEX),
            '--autofocus-mode', 'auto',
            '--width', '4624', '--height', '3472',
        ]
        try:
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            if result.returncode == 0 and os.path.exists(IMAGE_PATH):
                print(f"[SNAP] Captured: {IMAGE_PATH}")
                return True
            print(f"[ERR] rpicam-still error: {result.stderr}")
            return False
        except Exception as e:
            print(f"[ERR] Capture error: {e}")
            return False
    else:
        import cv2
        with stream_lock:
            if stream_frame is not None:
                cv2.imwrite(IMAGE_PATH, stream_frame)
                print(f"[SNAP] Captured from stream")
                return True
        cap = cv2.VideoCapture(CAMERA_INDEX)
        ret, frame = cap.read()
        cap.release()
        if ret:
            cv2.imwrite(IMAGE_PATH, frame)
            print(f"[SNAP] Captured: {IMAGE_PATH}")
            return True
        print("[ERR] Capture failed")
        return False

# ==============================
# PRINT
# ==============================

def print_receipt(image_b64):
    """Print a receipt image from base64 data."""
    try:
        from escpos.printer import File
        from PIL import Image, ImageChops
        import io

        # Remove data URL prefix
        if ',' in image_b64:
            image_b64 = image_b64.split(',')[1]

        image_bytes = base64.b64decode(image_b64)
        img = Image.open(io.BytesIO(image_bytes))

        # Auto-crop white margins
        img_gray = img.convert('L')
        bg = Image.new('L', img_gray.size, 255)
        diff = ImageChops.difference(img_gray, bg)
        bbox = diff.getbbox()
        if bbox:
            img = img.crop(bbox)

        # Resize to full printer width
        PRINTER_WIDTH = 576
        w_percent = PRINTER_WIDTH / float(img.size[0])
        h_size = int(float(img.size[1]) * w_percent)
        img = img.resize((PRINTER_WIDTH, h_size), Image.LANCZOS)

        # Floyd-Steinberg dithering
        img = img.convert('L')
        img = img.convert('1', dither=Image.FLOYDSTEINBERG)

        temp_path = '/tmp/print_receipt.png'
        img.save(temp_path)

        print("[PRINT] Printing...")
        p = File(PRINTER_DEVICE)
        p._raw(b'\x1B\x40')
        p.profile.profile_data['media']['width']['pixels'] = 576
        p.image(temp_path, impl='bitImageColumn',
                high_density_vertical=True, high_density_horizontal=True,
                center=True)
        p.text('\n\n\n')
        p.cut()
        p.close()

        if os.path.exists(temp_path):
            os.remove(temp_path)

        print("[PRINT] Done!")
        return True
    except Exception as e:
        print(f"[ERR] Print error: {e}")
        import traceback
        traceback.print_exc()
        return False

# ==============================
# WEBSOCKET CLIENT (connects to server)
# ==============================

async def websocket_client(server_url):
    """Persistent WebSocket connection to the server."""
    import websockets

    global preview_active, capture_in_progress

    while True:
        try:
            print(f"[WS] Connecting to {server_url}...")

            # Handle wss:// with no cert verification for dev
            ssl_context = None
            if server_url.startswith('wss://'):
                import ssl
                ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE

            async with websockets.connect(server_url, ssl=ssl_context,
                                          ping_interval=20, ping_timeout=10,
                                          max_size=50 * 1024 * 1024) as ws:
                print("[WS] Connected to server!")

                # Stream task reference
                stream_task = None

                async def stream_frames_to_server():
                    """Push MJPEG frames to server as binary WS messages."""
                    try:
                        for frame in generate_frames():
                            if not preview_active:
                                break
                            await ws.send(frame)
                            await asyncio.sleep(1 / 15)  # ~15fps
                    except Exception as e:
                        print(f"[STREAM] Stream to server ended: {e}")
                    finally:
                        stop_stream_process()

                while True:
                    message = await ws.recv()

                    if isinstance(message, str):
                        data = json.loads(message)
                        action = data.get("action")
                        request_id = data.get("id", "")

                        print(f"[WS] Command: {action} (id={request_id})")

                        # --- PREVIEW START ---
                        if action == "preview_start":
                            # Kill old stream
                            preview_active = False
                            if stream_task and not stream_task.done():
                                stream_task.cancel()
                                try:
                                    await stream_task
                                except:
                                    pass
                            stop_stream_process()
                            subprocess.run(['pkill', '-9', 'rpicam'], capture_output=True)
                            await asyncio.sleep(0.5)

                            preview_active = True
                            capture_in_progress = False

                            # Start streaming in background
                            stream_task = asyncio.create_task(stream_frames_to_server())

                            await ws.send(json.dumps({
                                "id": request_id,
                                "success": True
                            }))

                        # --- PREVIEW STOP ---
                        elif action == "preview_stop":
                            preview_active = False
                            if stream_task and not stream_task.done():
                                stream_task.cancel()
                                try:
                                    await stream_task
                                except:
                                    pass
                            stop_stream_process()

                            await ws.send(json.dumps({
                                "id": request_id,
                                "success": True
                            }))

                        # --- CAPTURE ---
                        elif action == "capture":
                            capture_in_progress = True

                            # Stop stream for rpicam
                            if USE_RPICAM and stream_process:
                                print("[PAUSE] Stopping stream for capture...")
                                preview_active = False
                                if stream_task and not stream_task.done():
                                    stream_task.cancel()
                                    try:
                                        await stream_task
                                    except:
                                        pass
                                stop_stream_process()
                                await asyncio.sleep(0.5)

                            # Capture in thread (blocking I/O)
                            loop = asyncio.get_event_loop()
                            success = await loop.run_in_executor(None, capture_image)

                            capture_in_progress = False

                            await ws.send(json.dumps({
                                "id": request_id,
                                "success": success,
                                "message": "Captured!" if success else "Capture failed"
                            }))

                        # --- PRINT ---
                        elif action == "print":
                            image_data = data.get("image", "")
                            loop = asyncio.get_event_loop()
                            success = await loop.run_in_executor(None, print_receipt, image_data)

                            await ws.send(json.dumps({
                                "id": request_id,
                                "success": success,
                                "message": "Printed!" if success else "Print failed"
                            }))

                        else:
                            print(f"[WS] Unknown action: {action}")
                            await ws.send(json.dumps({
                                "id": request_id,
                                "success": False,
                                "error": f"Unknown action: {action}"
                            }))

        except Exception as e:
            print(f"[WS] Connection error: {e}")
            preview_active = False
            stop_stream_process()

        # Always retry
        print("[WS] Reconnecting in 3 seconds...")
        await asyncio.sleep(3)

# ==============================
# FLASK (local HTTP endpoints, optional)
# ==============================

from flask import Flask, Response, jsonify, request as flask_request

flask_app = Flask(__name__)

@flask_app.route('/preview/start', methods=['POST'])
def local_preview_start():
    global preview_active
    subprocess.run(['pkill', '-9', 'rpicam'], capture_output=True)
    time.sleep(0.3)
    preview_active = True
    print("[PREVIEW] Started (local)")
    return jsonify({"success": True})

@flask_app.route('/preview/stop', methods=['POST'])
def local_preview_stop():
    global preview_active
    preview_active = False
    stop_stream_process()
    print("[PREVIEW] Stopped (local)")
    return jsonify({"success": True})

@flask_app.route('/stream')
def local_stream():
    def gen():
        for frame in generate_frames():
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + frame + b'\r\n')
    return Response(gen(), mimetype='multipart/x-mixed-replace; boundary=frame')

@flask_app.route('/capture', methods=['POST', 'GET'])
def local_capture():
    global capture_in_progress
    if capture_in_progress:
        return jsonify({"success": False, "error": "Capture in progress"}), 429
    capture_in_progress = True
    try:
        if USE_RPICAM and stream_process:
            stop_stream_process()
            time.sleep(0.5)
        success = capture_image()
        return jsonify({"success": success, "message": "Captured!" if success else "Failed"})
    finally:
        capture_in_progress = False

@flask_app.route('/print', methods=['POST'])
def local_print():
    data = flask_request.get_json()
    image_data = data.get('image', '')
    success = print_receipt(image_data)
    return jsonify({"success": success})

# ==============================
# MAIN
# ==============================

def run_flask(port):
    """Run Flask in a separate thread."""
    flask_app.run(host='0.0.0.0', port=port, threaded=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Potboy Camera Client')
    parser.add_argument('--port', type=int, default=int(os.getenv('HTTP_PORT', '5001')))
    parser.add_argument('--server', type=str,
                        default=os.getenv('WS_SERVER_URL', ''),
                        help='WebSocket server URL (e.g., wss://potboy.onrender.com/ws/pi)')
    args = parser.parse_args()

    import socket
    ip = socket.gethostbyname(socket.gethostname())
    camera_type = "rpicam (Arducam)" if USE_RPICAM else "OpenCV"

    print("\n" + "=" * 50)
    print("POTBOY CAMERA CLIENT")
    print("=" * 50)
    print(f"Local HTTP:  http://{ip}:{args.port}")
    print(f"Camera:      {camera_type}")
    print(f"GPIO:        {'Enabled' if GPIO_ENABLED else 'Disabled'}")
    print(f"Printer:     {PRINTER_DEVICE}")
    print(f"WS Server:   {args.server or '(none - local only)'}")
    print("=" * 50 + "\n")

    # Start Flask in background thread for local access
    flask_thread = threading.Thread(target=run_flask, args=(args.port,), daemon=True)
    flask_thread.start()

    # If server URL provided, connect via WebSocket
    if args.server:
        asyncio.run(websocket_client(args.server))
    else:
        print("[INFO] No WS server URL. Running in local-only mode.")
        print("[INFO] Use --server wss://your-app.onrender.com/ws/pi for remote mode.")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")