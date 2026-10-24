#!/usr/bin/env python3
"""External TCP/JSON client and tiny HTTP canvas viewer for Project 3."""

import argparse
import json
import math
import os
import socket
import threading
import time
import xml.etree.ElementTree as ET
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


HERE = Path(__file__).resolve().parent
# Keep the view anchored in the world so robot motion does not recenter or zoom it.
CAMERA_CENTER = (0.0, 0.0, 0.85)
CAMERA_SCALE = 0.23
DEMO_URDF = """<robot name="demo_arm">
  <link name="base_link"/><link name="shoulder_link"/><link name="elbow_link"/>
  <link name="wrist_link"/><link name="tool_link"/>
  <joint name="shoulder" type="continuous"><parent link="base_link"/><child link="shoulder_link"/><origin xyz="0 0 0.65"/><axis xyz="0 0 1"/></joint>
  <joint name="elbow" type="revolute"><parent link="shoulder_link"/><child link="elbow_link"/><origin xyz="0.85 0 0"/><axis xyz="0 1 0"/><limit lower="-1.4" upper="1.4" effort="1" velocity="1"/></joint>
  <joint name="wrist" type="revolute"><parent link="elbow_link"/><child link="wrist_link"/><origin xyz="0.72 0 0"/><axis xyz="0 1 0"/><limit lower="-1.8" upper="1.8" effort="1" velocity="1"/></joint>
  <joint name="tool_fixed" type="fixed"><parent link="wrist_link"/><child link="tool_link"/><origin xyz="0.38 0 0"/></joint>
</robot>"""
DEMO_FSM = {
    "initial_state": "home",
    "states": [
        {"name": "home", "target": {"joint_names": ["shoulder", "elbow", "wrist"], "positions": [0.0, 0.0, 0.0]}, "epsilon": 0.025, "on_arrival": "raise", "transitions": [{"name": "raise", "next_state": "reach"}]},
        {"name": "reach", "target": {"joint_names": ["shoulder", "elbow", "wrist"], "positions": [0.65, -0.9, 0.7]}, "epsilon": 0.025, "on_arrival": "wave", "transitions": [{"name": "wave", "next_state": "wave"}]},
        {"name": "wave", "target": {"joint_names": ["shoulder", "elbow", "wrist"], "positions": [-0.55, -0.45, 1.15]}, "epsilon": 0.025, "on_arrival": "return", "transitions": [{"name": "return", "next_state": "home"}]},
    ],
}


class RosClient:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.reader = self.sock.makefile("rb")
        self.send_lock = threading.Lock()
        self.lock = threading.Lock()
        self.next_id = 0
        self.waiters = {}
        self.state = {"transforms": [], "parents": {}, "status": None, "error": None}
        self.recording = False
        self.frames = []
        self.reader_thread = threading.Thread(target=self._read_loop, daemon=True)
        self.reader_thread.start()
        self.send({"op": "subscribe", "topic": "/xform_world", "type": "xform_world/MatrixTransformArray"})
        self.send({"op": "subscribe", "topic": "/fsm/status", "type": "fsm/Status"})

    def send(self, message):
        data = (json.dumps(message, separators=(",", ":"), ensure_ascii=False) + "\n").encode("utf-8")
        with self.send_lock:
            self.sock.sendall(data)

    def call(self, service, args, timeout=8):
        with self.lock:
            self.next_id += 1
            request_id = f"viewer-{self.next_id}"
            event = threading.Event()
            slot = {"event": event, "response": None}
            self.waiters[request_id] = slot
        self.send({"op": "call_service", "service": service, "id": request_id, "args": args})
        if not event.wait(timeout):
            with self.lock:
                self.waiters.pop(request_id, None)
            raise TimeoutError(f"service timed out: {service}")
        response = slot["response"]
        if response is None:
            raise RuntimeError(f"connection closed during {service}")
        if not response.get("result", False):
            raise RuntimeError(f"{service}: {response.get('status', 'failed')}")
        return response.get("values", {})

    def _read_loop(self):
        try:
            for raw in self.reader:
                if len(raw) > 4 * 1024 * 1024:
                    raise ValueError("gateway frame exceeds 4 MiB")
                message = json.loads(raw)
                if message.get("op") == "service_response":
                    with self.lock:
                        slot = self.waiters.get(message.get("id"))
                        if slot:
                            slot["response"] = message
                            slot["event"].set()
                elif message.get("op") == "publish":
                    with self.lock:
                        if message.get("topic") == "/xform_world":
                            self.state["transforms"] = message.get("msg", {}).get("transforms", [])
                            if self.recording:
                                self.frames.append({"transforms": self.state["transforms"], "parents": self.state["parents"], "status": self.state["status"]})
                        elif message.get("topic") == "/fsm/status":
                            self.state["status"] = message.get("msg")
        except Exception as exc:
            with self.lock:
                self.state["error"] = str(exc)
                for slot in self.waiters.values():
                    slot["event"].set()

    def load_demo(self):
        response = self.call("/param_server/set_param", {"name": "robot_description", "value": DEMO_URDF})
        version = response["version"]
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            try:
                status = self.call("/param_server/get_param", {"name": "/robot_state_publisher/description_status"})["value"]
            except RuntimeError as exc:
                if "parameter has never been set" not in str(exc):
                    raise
                time.sleep(0.05)
                continue
            if status and status.get("version", 0) >= version:
                if not status.get("accepted"):
                    raise RuntimeError(f"demo URDF rejected: {status.get('error')}")
                break
            time.sleep(0.05)
        else:
            raise TimeoutError("robot_state_publisher did not accept the demo URDF")
        root = ET.fromstring(DEMO_URDF)
        parents = {}
        for joint in root:
            if joint.tag == "joint":
                parent, child = joint.find("parent"), joint.find("child")
                if parent is not None and child is not None:
                    parents[child.attrib["link"]] = parent.attrib["link"]
        with self.lock:
            self.state["parents"] = parents
        # Let the recommended joint_state_publisher finish its own description poll.
        time.sleep(0.35)
        deadline = time.monotonic() + 4
        while True:
            try:
                self.call("/fsm/update_logic", DEMO_FSM)
                break
            except RuntimeError as exc:
                if "no service provider" not in str(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.1)


def project(point, scale, center, width, height):
    # Convert z-up URDF world coordinates into a y-up view, then use an isometric camera.
    x, up, depth = point[0] - center[0], point[2] - center[2], -(point[1] - center[1])
    yaw, elev = -0.72, 0.48
    xx = math.cos(yaw) * x + math.sin(yaw) * depth
    zz = -math.sin(yaw) * x + math.cos(yaw) * depth
    return (width / 2 + xx * scale,
            height * 0.52 - (math.cos(elev) * up + math.sin(elev) * zz) * scale,
            -math.sin(elev) * up + math.cos(elev) * zz)


def render_frame(frame, size=(1000, 680)):
    from PIL import Image, ImageDraw, ImageFont
    width, height = size
    image = Image.new("RGB", size, "#121a24")
    draw = ImageDraw.Draw(image)
    transforms = frame.get("transforms", [])
    by_name = {t["child_frame_id"]: t for t in transforms}
    if not by_name:
        draw.text((30, 30), "Waiting for /xform_world", fill="#e6edf5")
        return image
    center = CAMERA_CENTER
    scale = min(width, height) * CAMERA_SCALE
    # Ground grid.
    for k in range(-5, 6):
        a = project([center[0] + k * .3, center[1] - 1.5, 0], scale, center, width, height)
        b = project([center[0] + k * .3, center[1] + 1.5, 0], scale, center, width, height)
        draw.line((a[0], a[1], b[0], b[1]), fill="#293647", width=1)
        a = project([center[0] - 1.5, center[1] + k * .3, 0], scale, center, width, height)
        b = project([center[0] + 1.5, center[1] + k * .3, 0], scale, center, width, height)
        draw.line((a[0], a[1], b[0], b[1]), fill="#293647", width=1)
    parents = frame.get("parents", {})
    colors = ["#56c7f2", "#76e6bd", "#ffc66d", "#f28fba", "#a9a1ff", "#90d48b"]
    color_by_name = {name: colors[i % len(colors)] for i, name in enumerate(sorted(by_name))}
    for child, parent in parents.items():
        if child in by_name and parent in by_name:
            a = project(by_name[parent]["matrix"][12:15], scale, center, width, height)
            b = project(by_name[child]["matrix"][12:15], scale, center, width, height)
            draw.line((a[0], a[1], b[0], b[1]), fill="#71839a", width=14)
    ordered = sorted(transforms, key=lambda t: project(t["matrix"][12:15], scale, center, width, height)[2])
    font = ImageFont.load_default()
    for i, transform in enumerate(ordered):
        matrix = transform["matrix"]
        p = project(matrix[12:15], scale, center, width, height)
        radius = 10
        color = color_by_name[transform["child_frame_id"]]
        draw.ellipse((p[0]-radius, p[1]-radius, p[0]+radius, p[1]+radius), fill=color, outline="#071018", width=2)
        tip = [matrix[12] + matrix[0]*.24, matrix[13] + matrix[1]*.24, matrix[14] + matrix[2]*.24]
        q = project(tip, scale, center, width, height)
        draw.line((p[0], p[1], q[0], q[1]), fill=color, width=4)
        draw.text((p[0]+14, p[1]-7), transform["child_frame_id"], fill="#eef5fc", font=font)
    status = frame.get("status") or {}
    draw.rectangle((0, 0, width, 34), fill="#0b1119")
    draw.text((16, 11), f"Project 3 FK   ·   {status.get('current_state') or 'demo arm'}   ·   {len(transforms)} links", fill="#dce8f5", font=font)
    return image


def save_gif(frames, path, seconds, playback_speed=0.5):
    if not frames:
        raise RuntimeError("no /xform_world frames received; ensure make demo is running")
    if not math.isfinite(playback_speed) or playback_speed <= 0:
        raise ValueError("playback_speed must be a finite number greater than 0")
    from PIL import Image
    images = [render_frame(frame) for frame in frames]
    output = Path(path).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    frame_duration = seconds * 1000 / len(images) / playback_speed
    images[0].save(output, save_all=True, append_images=images[1:], duration=max(40, int(frame_duration)), loop=0, optimize=True)
    return output


def make_handler(client):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/state":
                with client.lock:
                    body = json.dumps(client.state).encode()
                self.send_response(200); self.send_header("Content-Type", "application/json"); self.send_header("Cache-Control", "no-store"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
            else:
                body = (HERE / "index.html").read_bytes()
                self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(body))); self.end_headers(); self.wfile.write(body)
        def do_POST(self):
            if self.path != "/load-demo":
                self.send_error(404); return
            def load():
                try: client.load_demo()
                except Exception as exc:
                    with client.lock: client.state["error"] = str(exc)
            threading.Thread(target=load, daemon=True).start()
            self.send_response(202); self.end_headers()
        def log_message(self, fmt, *args):
            pass
    return Handler


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1", help="Rosbridge TCP host")
    parser.add_argument("--port", type=int, default=9095, help="Rosbridge TCP port")
    parser.add_argument("--http-port", type=int, default=8765, help="local viewer page port")
    parser.add_argument("--load-demo", action="store_true", help="load the demo robot and looping FSM")
    parser.add_argument("--record", metavar="GIF", help="record incoming /xform_world frames to an animated GIF")
    parser.add_argument("--seconds", type=float, default=12.0, help="recording duration")
    parser.add_argument("--playback-speed", type=float, default=0.5,
                        help="GIF playback speed (0.5 is half speed; default: 0.5)")
    args = parser.parse_args()
    if not math.isfinite(args.playback_speed) or args.playback_speed <= 0:
        parser.error("--playback-speed must be a finite number greater than 0")
    client = RosClient(args.host, args.port)
    server = ThreadingHTTPServer(("127.0.0.1", args.http_port), make_handler(client))
    threading.Thread(target=server.serve_forever, daemon=True).start()
    print(f"FK viewer: http://127.0.0.1:{args.http_port}")
    if args.load_demo:
        client.load_demo()
        print("Loaded the demo arm and looping choreography.")
    if args.record:
        with client.lock:
            client.frames.clear(); client.recording = True
        deadline = time.monotonic() + args.seconds
        while time.monotonic() < deadline:
            time.sleep(0.1)
        with client.lock:
            client.recording = False
            frames = list(client.frames)
        output = save_gif(frames, args.record, args.seconds, args.playback_speed)
        print(f"Recorded {len(frames)} /xform_world frames to {output}")
        return
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
