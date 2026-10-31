"""Pomocniki testow procesowych: gateway jako podproces, sztuczny rosbridge i broker MQTT.
Wszystko tylko na 127.0.0.1."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

from websockets.exceptions import ConnectionClosed

import contracts as c

GATEWAY_DIR = Path(__file__).resolve().parent.parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_until(fn, timeout: float, step: float = 0.05):
    deadline = time.monotonic() + timeout
    while True:
        try:
            v = fn()
            if v:
                return v
        except Exception:
            pass
        if time.monotonic() > deadline:
            raise TimeoutError(f"condition not met in {timeout} s")
        time.sleep(step)


def dockerfile_cmd(port: int) -> list[str]:
    """CMD z Dockerfile (to samo co w kontenerze), z hostem/portem podmienionym na lokalne."""
    for line in (GATEWAY_DIR / "Dockerfile").read_text().splitlines():
        if line.startswith("CMD "):
            cmd = json.loads(line[4:])
    cmd[cmd.index("--host") + 1] = "127.0.0.1"
    cmd[cmd.index("--port") + 1] = str(port)
    assert cmd[0] == "uvicorn"
    return [sys.executable, "-m", *cmd]


class Gateway:
    """Gateway w podprocesie; stdout+stderr zbierane w tle."""

    def __init__(self, cmd: list[str], env: dict[str, str]):
        full_env = {k: v for k, v in os.environ.items() if k not in ("API_KEY", "ROSBRIDGE_ENABLED", "MQTT_ENABLED")}
        full_env.update({"PYTHONDONTWRITEBYTECODE": "1", "PYTHONUNBUFFERED": "1", **env})  # env moze nadpisac
        self.t0 = time.monotonic()
        self.proc = subprocess.Popen(cmd, cwd=GATEWAY_DIR, env=full_env, stdout=subprocess.PIPE,
                                     stderr=subprocess.STDOUT, text=True)
        self.lines: list[str] = []
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self):
        for line in self.proc.stdout:
            self.lines.append(line)

    @property
    def output(self) -> str:
        return "".join(self.lines)

    def stop(self) -> str:
        if self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(10)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(5)
        self._reader.join(5)
        return self.output


class BgLoop:
    """Petla asyncio w osobnym watku (sztuczne serwery)."""

    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self.loop.run_forever, daemon=True)
        self.thread.start()

    def run(self, coro, timeout: float = 10):
        return asyncio.run_coroutine_threadsafe(coro, self.loop).result(timeout)

    def close(self):
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(5)


# --- sztuczny broker MQTT 3.1.1 (CONNECT/SUBSCRIBE/PINGREQ, jedna wiadomosc po SUBSCRIBE) ---


def _enc_len(n: int) -> bytes:
    out = bytearray()
    while True:
        b, n = n % 128, n // 128
        out.append(b | (0x80 if n else 0))
        if not n:
            return bytes(out)


async def start_fake_mqtt(port: int, topic: str, payload: bytes, every: float | None = None):
    """every: po SUBSCRIBE publikuje payload co tyle sekund (inaczej raz)."""
    def publish_frame() -> bytes:
        t = topic.encode()
        vh = len(t).to_bytes(2, "big") + t + payload
        return b"\x30" + _enc_len(len(vh)) + vh

    async def repeat(writer):
        with contextlib.suppress(Exception):
            while True:
                await asyncio.sleep(every)
                writer.write(publish_frame())
                await writer.drain()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        repeater = None
        try:
            while True:
                head = (await reader.readexactly(1))[0]
                length, mult = 0, 1
                while True:
                    b = (await reader.readexactly(1))[0]
                    length += (b & 0x7F) * mult
                    mult *= 128
                    if not b & 0x80:
                        break
                body = await reader.readexactly(length)
                kind = head >> 4
                if kind == 1:  # CONNECT -> CONNACK
                    writer.write(b"\x20\x02\x00\x00")
                elif kind == 8:  # SUBSCRIBE -> SUBACK + PUBLISH QoS0
                    writer.write(b"\x90\x03" + body[:2] + b"\x00")
                    writer.write(publish_frame())
                    if every and repeater is None:
                        repeater = asyncio.create_task(repeat(writer))
                elif kind == 12:  # PINGREQ
                    writer.write(b"\xd0\x00")
                elif kind == 14:  # DISCONNECT
                    break
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            if repeater:
                repeater.cancel()
            writer.close()

    return await asyncio.start_server(handle, "127.0.0.1", port)


# --- sztuczny rosbridge: zbiera subskrypcje, po komplecie publikuje zadane wiadomosci ---


class FakeRosbridge:
    def __init__(self, expected_topics: int, publish: list[str], flood: str | None = None, flood_hz: int = 1000):
        self.expected = expected_topics
        self.publish = publish  # surowe ramki tekstowe (moga zawierac NaN - jak json.dumps z ROS)
        self.flood = flood  # ramka wysylana bez konca po publish (zalew zdarzeniami)
        self.flood_hz = flood_hz  # tyle, ile gateway przetrawi - inaczej pong do niego utknie za zalewem
        self.paused = False  # True: zalew wstrzymany, polaczenie zostaje (robot przestal nadawac)
        self.subscribed: set[str] = set()
        self.sub_ops = 0  # wszystkie operacje subscribe (duplikaty tez)
        self.server = None

    async def _flood(self, ws):
        while True:
            if not self.paused:
                for _ in range(max(1, self.flood_hz // 50)):
                    await ws.send(self.flood)
            await asyncio.sleep(0.02)

    async def _handler(self, ws):
        sent, flooder = False, None
        try:
            async for raw in ws:
                m = json.loads(raw)
                if m.get("op") == "subscribe":
                    self.subscribed.add(m["topic"])
                    self.sub_ops += 1
                if not sent and len(self.subscribed) >= self.expected:
                    sent = True
                    for frame in self.publish:
                        await ws.send(frame)
                    if self.flood:
                        flooder = asyncio.create_task(self._flood(ws))
        except ConnectionClosed:  # gateway przy stopie zrywa polaczenie - to nie blad testu
            pass
        finally:
            if flooder:
                flooder.cancel()

    async def start(self, port: int):
        from websockets.asyncio.server import serve

        self.server = await serve(self._handler, "127.0.0.1", port)

    async def stop(self):
        self.server.close()
        await self.server.wait_closed()


# --- milczacy peer: TCP przyjete przez kernel (backlog), handshake WS nigdy nie odpowiada ---


def silent_peer(port: int) -> socket.socket:
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.listen(64)  # bez accept
    return s


async def start_accept_close(port: int, accepted: list):
    """Peer, ktory przyjmuje TCP i od razu zamyka (rosbridge w restarcie, proxy): kazda proba to 'close'
    w roslibpy bez 'ready'."""
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        accepted.append(time.monotonic())
        writer.close()

    return await asyncio.start_server(handle, "127.0.0.1", port)


async def start_http_503(port: int):
    """Peer, ktory odpowiada na handshake WS 503 i zamyka (proxy bez backendu)."""
    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        with contextlib.suppress(Exception):
            await reader.readuntil(b"\r\n\r\n")
            writer.write(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            await writer.drain()
        writer.close()

    return await asyncio.start_server(handle, "127.0.0.1", port)


def raw_ws_client(port: int, path: str, rcvbuf: int = 4096) -> socket.socket:
    """Klient WS, ktory robi handshake i nigdy nic nie czyta (maly bufor odbioru)."""
    s = socket.socket()
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    s.connect(("127.0.0.1", port))
    nonce = base64.b64encode(os.urandom(16)).decode()  # losowy jak w RFC 6455 (stala wyglada skanerom na sekret)
    s.sendall((f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nUpgrade: websocket\r\n"
               f"Connection: Upgrade\r\nSec-WebSocket-Key: {nonce}\r\n"
               "Sec-WebSocket-Version: 13\r\n\r\n").encode())
    return s


def fin_wait_on(port: int) -> int:
    """Gniazda w FIN_WAIT_1/2 po stronie serwera na porcie (sieroty po zamknieciu fd nie maja procesu)."""
    out = subprocess.run(["netstat", "-an", "-p", "tcp"] if sys.platform == "darwin" else ["netstat", "-ant"],
                         capture_output=True, text=True).stdout
    return sum(("FIN_WAIT" in line) and (f"127.0.0.1.{port} " in line or f"127.0.0.1:{port} " in line)
               for line in out.splitlines())


def established_on(pid: int, port: int) -> int:
    """Liczba polaczen ESTABLISHED procesu pid po stronie serwera na porcie port (lsof)."""
    out = subprocess.run(["lsof", "-nP", "-a", "-p", str(pid), "-iTCP", "-sTCP:ESTABLISHED"],
                         capture_output=True, text=True).stdout
    return sum(f":{port}->" in line for line in out.splitlines())


# --- atrapa petli z recznym zegarem (throttling, status) ---


class _Handle:
    def __init__(self, when, fn, args):
        self.when, self.fn, self.args, self.cancelled = when, fn, args, False

    def cancel(self):
        self.cancelled = True


class FakeLoop:
    """call_later z recznym zegarem; advance odpala timery po kolei."""

    def __init__(self):
        self.now = 0.0
        self.timers: list[_Handle] = []
        self.created = 0

    def call_later(self, delay, fn, *args):
        h = _Handle(self.now + delay, fn, args)
        self.timers.append(h)
        self.created += 1
        return h

    def advance(self, dt):
        target = self.now + dt
        while True:
            due = [h for h in self.timers if h.when <= target and not h.cancelled]
            if not due:
                break
            h = min(due, key=lambda x: x.when)
            self.timers.remove(h)
            self.now = h.when
            h.fn(*h.args)
        self.now = target

    def active(self):
        return [h for h in self.timers if not h.cancelled]


class FakeClient:
    """Klient z robot.clients: zapisuje (czas, ramka) z broadcast."""

    def __init__(self, loop):
        self.loop, self.frames = loop, []

    def offer(self, text):
        self.frames.append((self.loop.now, json.loads(text)["event"]))
        return True

    def of(self, kind):
        return [(t, e) for t, e in self.frames if e["type"] == kind]


def imu(ts, x):
    q = c.Quaternion(x=0.0, y=0.0, z=0.0, w=1.0)
    v = c.Vec3(x=x, y=0.0, z=0.0)
    return c.ImuEvent(robot_id="r1", ts=ts, data=c.Imu(orientation=q, roll=0, pitch=0, yaw=0,
                                                       angular_velocity=v, linear_acceleration=v))
