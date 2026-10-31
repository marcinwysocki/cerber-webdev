"""Gateway jako prawdziwy proces (CMD z Dockerfile / python main.py): start bez rosbridge,
pozne polaczenie, milczacy peer i 503 (cicha faza wolna), szybki stop, NaN z ROS, MQTT GPS, redakcja api_key w logach,
limit rozmiaru ramki WS, zrywanie klientow, ktore nie czytaja (RST), status/heartbeat, resubskrypcja po reconnect."""

import base64
import contextlib
import json
import math
import os
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time

import httpx
import pytest
from websockets.exceptions import ConnectionClosed
from websockets.sync.client import connect

import main
from conftest import GATEWAY_DIR, TEST_KEY
from helpers import (BgLoop, FakeRosbridge, Gateway, dockerfile_cmd, established_on, fin_wait_on, free_port,
                     raw_ws_client, silent_peer, start_accept_close, start_fake_mqtt, start_http_503, wait_until)

RID = main.ROBOT_ID
H = {"X-API-Key": TEST_KEY}
WS_PATH = f"/api/v1/robots/{RID}/telemetry/stream"


def _clean_exit(gw, out):
    # uvicorn po czystym shutdownie ponawia przechwycony SIGTERM (kod -15 / 143 w dockerze)
    return gw.proc.returncode in (0, -signal.SIGTERM) and "Application shutdown complete." in out


def _timed_stop(gw):
    t0 = time.monotonic()
    out = gw.stop()
    return out, time.monotonic() - t0


def _strict_json(text):
    def no_const(c):
        raise AssertionError(f"non-finite {c} in public frame")
    return json.loads(text, parse_constant=no_const)


def test_starts_without_rosbridge_then_connects_late():
    ros_port, mqtt_port, http_port = free_port(), free_port(), free_port()
    base = f"http://127.0.0.1:{http_port}"
    bg = BgLoop()
    gps = {"date": "2026-10-26T14:03:11Z", "latitude": 51.1, "longitude": 17.0, "altitude": 118.4,
           "satellites": 9, "fix_quality": 1}
    mqtt = bg.run(start_fake_mqtt(mqtt_port, main.MQTT_GPS_TOPIC, json.dumps(gps).encode()))
    imu_nan = {"orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
               "angular_velocity": {"x": float("nan"), "y": 0.0, "z": 0.0},
               "linear_acceleration": {"x": 0.0, "y": float("inf"), "z": 9.81}}
    imu_ok = {**imu_nan, "angular_velocity": {"x": 0.25, "y": 0.0, "z": 0.0},
              "linear_acceleration": {"x": 0.0, "y": 0.0, "z": 9.81}}
    ros = FakeRosbridge(len(main.ROS_SUBSCRIPTIONS), [
        json.dumps({"op": "publish", "topic": "/ptz/status", "msg": {"data": "[PTZ] MOVING | IP: 192.0.2.10"}}),
        json.dumps({"op": "publish", "topic": "/imu/data", "msg": imu_nan}),  # NaN/Infinity jak z json ROS
        json.dumps({"op": "publish", "topic": "/imu/data", "msg": imu_ok}),
    ])
    gw = Gateway(dockerfile_cmd(http_port), {
        "API_KEY": TEST_KEY, "ROSBRIDGE_HOST": "127.0.0.1", "ROSBRIDGE_PORT": str(ros_port),
        "MQTT_HOST": "127.0.0.1", "MQTT_PORT": str(mqtt_port),
    })
    try:
        # rosbridge nieosiagalny: proces zyje, /health odpowiada szybko i mowi degraded
        wait_until(lambda: httpx.get(f"{base}/health", timeout=1).status_code == 200, 10)
        startup_s = time.monotonic() - gw.t0
        assert gw.proc.poll() is None
        assert startup_s < 4, f"startup {startup_s:.1f} s"  # z importami; sam lifespan nie czeka na ROS
        h = httpx.get(f"{base}/health").json()
        assert h["status"] == "degraded" and h["rosbridge_connected"] is False

        # MQTT/GPS dziala niezaleznie od rosbridge
        fix = wait_until(lambda: httpx.get(f"{base}/api/v1/robots/{RID}/gps/latest", headers=H, timeout=1)
                         .json().get("latitude"), 5)
        assert fix == 51.1

        # petla asyncio nie jest blokowana przez probe polaczenia z ROS
        for _ in range(5):
            t0 = time.monotonic()
            httpx.get(f"{base}/health", timeout=1)
            assert time.monotonic() - t0 < 0.5

        with connect(f"ws://127.0.0.1:{http_port}/api/v1/robots/{RID}/telemetry/stream?api_key={TEST_KEY}") as ws:
            assert _strict_json(ws.recv(timeout=2))["event"]["type"] == "snapshot"

            # rosbridge pojawia sie pozniej: reconnect w tle (backoff <= ~10 s), on_ready subskrybuje
            bg.run(ros.start(ros_port))
            wait_until(lambda: httpx.get(f"{base}/health").json()["rosbridge_connected"], 20, 0.2)
            wait_until(lambda: len(ros.subscribed) == len(main.ROS_SUBSCRIPTIONS), 5)
            assert ros.subscribed == set(main.ROS_SUBSCRIPTIONS)

            frames, deadline = [], time.monotonic() + 5
            while not any(f["event"]["type"] == "imu" for f in frames):
                assert time.monotonic() < deadline, f"no imu frame, got {frames}"
                try:
                    frames.append(_strict_json(ws.recv(timeout=1)))
                except TimeoutError:
                    pass
        assert "ptz" in [f["event"]["type"] for f in frames]
        # pierwsza ramka imu to ta poprawna: NaN/Infinity odrzucone na wejsciu, nie wyslane jako null
        imu = next(f for f in frames if f["event"]["type"] == "imu")["event"]["data"]
        assert imu["angular_velocity"]["x"] == 0.25

        snap = httpx.get(f"{base}/api/v1/robots/{RID}/telemetry/latest", headers=H).text
        assert "192.0.2" not in snap and json.loads(snap)["ptz"]["state"] == "MOVING"
        assert all(math.isfinite(v) for v in json.loads(snap)["imu"]["angular_velocity"].values())
        assert httpx.get(f"{base}/health").json()["status"] == "ok"
    finally:
        out, stop_s = _timed_stop(gw)
        if ros.server:
            bg.run(ros.stop())
        mqtt.close()
        bg.close()
    assert "[rosbridge] /imu/data" in out and "finite" in out  # zla wiadomosc zalogowana i porzucona
    # stop przy otwartym WS do rosbridge: szybko i czysto (grace dockera to 10 s)
    assert stop_s < 3 and _clean_exit(gw, out), (stop_s, gw.proc.returncode)
    assert "Traceback" not in out


SLOW_LOG = "[rosbridge] still unreachable after 1 s, retrying every 1 s"
FAST_SLOW_1S = {"ROSBRIDGE_FAST_RETRY_S": "1", "ROSBRIDGE_SLOW_RETRY_S": "1"}


def _quiet_slow_phase(out):
    # N1: jedna linia przy wejsciu w faze wolna; autobahn (logger "twisted") nie loguje kazdej proby
    assert out.count("[rosbridge] still unreachable") == 1 and SLOW_LOG in out, out[-3000:]
    assert "dropping connection to peer" not in out and "upgrade failed" not in out, out[-3000:]


def test_silent_peer_is_degraded_and_stop_is_fast():
    # TCP do rosbridge sie zestawia, ale handshake WS nigdy nie przychodzi (zawieszony peer/proxy)
    ros_port, http_port = free_port(), free_port()
    peer = silent_peer(ros_port)
    gw = Gateway(dockerfile_cmd(http_port), {"API_KEY": TEST_KEY, "ROSBRIDGE_HOST": "127.0.0.1",
                                             "ROSBRIDGE_PORT": str(ros_port), "MQTT_ENABLED": "0", **FAST_SLOW_1S})
    base = f"http://127.0.0.1:{http_port}"
    try:
        wait_until(lambda: httpx.get(f"{base}/health", timeout=1).status_code == 200, 10)
        # proba = 5 s timeoutu handshake'u autobahn + 1 s przerwy: nieudane w ~5 (faza szybka), ~11 i ~17 s
        states, deadline = set(), gw.t0 + 13
        while time.monotonic() < deadline:
            h = httpx.get(f"{base}/health", timeout=1).json()
            states.add((h["status"], h["rosbridge_connected"]))
            time.sleep(0.05)
        assert states == {("degraded", False)}, states
    finally:
        out, stop_s = _timed_stop(gw)  # stop w trakcie wiszacego handshake
        peer.close()
    assert stop_s < 3 and _clean_exit(gw, out), (stop_s, gw.proc.returncode, out[-2000:])
    _quiet_slow_phase(out)


def test_non_101_peer_slow_phase_is_quiet():
    # cos na porcie rosbridge odpowiada 503 (proxy bez backendu): kazda proba konczy sie od razu
    ros_port, http_port = free_port(), free_port()
    bg = BgLoop()
    peer = bg.run(start_http_503(ros_port))
    gw = Gateway(dockerfile_cmd(http_port), {"API_KEY": TEST_KEY, "ROSBRIDGE_HOST": "127.0.0.1",
                                             "ROSBRIDGE_PORT": str(ros_port), "MQTT_ENABLED": "0", **FAST_SLOW_1S})
    try:
        wait_until(lambda: SLOW_LOG in gw.output, 10)
        time.sleep(3.5)  # kilka prob w fazie wolnej
        assert httpx.get(f"http://127.0.0.1:{http_port}/health").json()["rosbridge_connected"] is False
    finally:
        out = gw.stop()
        peer.close()
        bg.close()
    _quiet_slow_phase(out)
    assert "Traceback" not in out


def test_rosbridge_lines_flushed_without_pythonunbuffered():
    # uruchomienie lokalne bez ENV PYTHONUNBUFFERED z Dockerfile, stdout do pliku/pipe (blokowy bufor):
    # linie [rosbridge] z watku Twisted musza wyjsc od razu, bez zadnego zapytania HTTP (access log flushuje)
    ros_port, http_port = free_port(), free_port()  # na ros_port nikt nie slucha: odmowa od razu
    gw = Gateway(dockerfile_cmd(http_port), {"API_KEY": TEST_KEY, "ROSBRIDGE_HOST": "127.0.0.1",
                                             "ROSBRIDGE_PORT": str(ros_port), "MQTT_ENABLED": "0",
                                             "PYTHONUNBUFFERED": "", "ROSBRIDGE_FAST_RETRY_S": "1",
                                             "ROSBRIDGE_SLOW_RETRY_S": "30"})
    line = "[rosbridge] still unreachable after 1 s, retrying every 30 s"
    try:
        wait_until(lambda: line in gw.output, 10)
        assert "[rosbridge] connecting to" in gw.output and gw.proc.poll() is None
    finally:
        out = gw.stop()
    assert out.count(line) == 1 and _clean_exit(gw, out), out[-2000:]


def _main_py_cmd(port):
    return [sys.executable, "main.py"], {"HOST": "127.0.0.1", "PORT": str(port)}


def _raw_request(port, head: bytes):
    with socket.create_connection(("127.0.0.1", port), timeout=2) as s:
        s.sendall(head)
        try:
            s.recv(256)
        except TimeoutError:  # h11 + websockets: zly naglowek -> traceback w logu, odpowiedz moze nie przyjsc
            pass


@pytest.mark.parametrize("launch", ["dockerfile_cmd", "dockerfile_cmd_debug", "dockerfile_cmd_trace", "python_main",
                                    "dockerfile_cmd_h11_debug"])
def test_api_key_never_logged_and_ws_frame_capped(launch):
    port = free_port()
    if launch == "python_main":
        cmd, env = _main_py_cmd(port)
    else:
        cmd, env = dockerfile_cmd(port), {}
        if launch.endswith("_h11_debug"):  # h11: websockets loguje traceback z wartoscia naglowka
            cmd += ["--http", "h11"]
        if launch.endswith(("_debug", "_trace")):  # debug: naglowki handshake WS; trace: tez scope ASGI
            cmd += ["--log-level", launch.rsplit("_", 1)[1]]
    wrong = "wrong-" + TEST_KEY[::-1]
    encoded = ["".join(f"%{x:02x}" for x in TEST_KEY.encode()), "".join(f"%{x:02X}" for x in TEST_KEY.encode())]
    gw = Gateway(cmd, {"API_KEY": TEST_KEY, "ROSBRIDGE_ENABLED": "0", "MQTT_ENABLED": "0", **env})
    base, ws_url = f"http://127.0.0.1:{port}", f"ws://127.0.0.1:{port}/api/v1/robots/{RID}/telemetry/stream"
    try:
        wait_until(lambda: httpx.get(f"{base}/health", timeout=1).status_code == 200, 10)
        # access log z query (REST ignoruje query, ale klient moze je wyslac)
        httpx.get(f"{base}/api/v1/robots?api_key={TEST_KEY}", headers=H)
        httpx.get(f"{base}/api/v1/robots?api_key={wrong}")
        # klucz w sciezce / z zakodowanym separatorem: uvicorn loguje sciezke przez quote() -> api_key%3D...
        for key in (TEST_KEY, wrong):
            for path in (f"/x/api_key%3D{key}", f"/x%3Bapi_key%3D{key}", f"{WS_PATH}%3Fapi_key%3D{key}",
                         f"/api/v1/robots?api_key%3D{key}"):
                _raw_request(port, f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
        # sam klucz bez etykiety, zakodowany niekanonicznie (male %xx jak .NET UrlEncode, pelne %XX): access log
        # z surowym query, debug "< GET ...", trace raw_path - wszedzie surowa forma
        for enc in encoded:
            httpx.get(f"{base}/api/v1/robots?token={enc}")
            httpx.get(f"{base}/api/v1/robots?apikey={enc}")
            _raw_request(port, f"GET /api/v1/robots/{enc} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
            with connect(f"ws://127.0.0.1:{port}/api/v1/robots/{enc}/telemetry/stream?api_key={TEST_KEY}") as ws:
                with pytest.raises(ConnectionClosed) as ei:
                    ws.recv(timeout=2)
                assert ei.value.rcvd.code == 4404
        # etykieta z separatorem ":" / cudzyslowami (uvicorn loguje je jako %3A / %22): obcy klucz
        for path in (f"/x/api_key:{wrong}", f"/x/api_key%3a{wrong}", f"/api/v1/robots?api_key%3A{wrong}",
                     f"/x/%22api_key%22:%22{wrong}%22"):
            _raw_request(port, f"GET {path} HTTP/1.1\r\nHost: x\r\n\r\n".encode())
        # niepoprawny bajt w wartosci naglowka: h11 przepuszcza, websockets loguje wyjatek z wartoscia
        for key in (TEST_KEY, wrong):  # obcy klucz: bez nazwy naglowka w bledzie, nie lapie go podmiana API_KEY
            nonce = base64.b64encode(os.urandom(16)).decode()
            _raw_request(port, (f"GET {WS_PATH} HTTP/1.1\r\nHost: x\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                                f"Sec-WebSocket-Key: {nonce}\r\nSec-WebSocket-Version: 13\r\n"
                                f"X-API-Key: {key}\x7f\r\n\r\n").encode())

        with connect(f"{ws_url}?api_key={wrong}") as ws:
            with pytest.raises(ConnectionClosed) as ei:
                ws.recv(timeout=2)
            assert ei.value.rcvd.code == 4401

        # nazwa parametru z zakodowanymi literami: Starlette dekoduje, wiec klucz dziala - log tez musi go ukryc
        with connect(f"{ws_url}?%61pi_key={TEST_KEY}") as ws:
            assert json.loads(ws.recv(timeout=2))["event"]["type"] == "snapshot"
        with connect(f"{ws_url}?%61%70i%5F%6bey={wrong}") as ws:
            with pytest.raises(ConnectionClosed) as ei:
                ws.recv(timeout=2)
            assert ei.value.rcvd.code == 4401

        # naglowek X-API-Key w handshake WS (websockets loguje naglowki na DEBUG)
        with connect(f"{ws_url}?api_key={TEST_KEY}", additional_headers={"X-API-Key": TEST_KEY}) as ws:
            ws.recv(timeout=2)
        with connect(ws_url, additional_headers={"X-API-Key": wrong}) as ws:
            with pytest.raises(ConnectionClosed):
                ws.recv(timeout=2)

        # 16 KiB zer po deflate to kilkadziesiat bajtow na kablu - limit musi dzialac po dekompresji
        with connect(f"{ws_url}?api_key={TEST_KEY}", compression="deflate") as ws:
            assert ws.protocol.extensions, "permessage-deflate not negotiated"
            assert json.loads(ws.recv(timeout=2))["event"]["type"] == "snapshot"
            ws.send("0" * 16384)
            with pytest.raises(ConnectionClosed) as ei:
                while True:
                    ws.recv(timeout=2)
            assert ei.value.rcvd.code == 1009

        # ramka w limicie jest po prostu ignorowana
        with connect(f"{ws_url}?api_key={TEST_KEY}") as ws:
            ws.recv(timeout=2)
            ws.send("x" * 1000)
            # klucz w ramkach od klienta (debug websockets loguje ramki, przyciete w srodku)
            ws.send(json.dumps({"type": "auth", "client": "x" * 16, "ts": time.time(), "api_key": TEST_KEY}))
            ws.send(json.dumps({"api_key": wrong}))
            ws.send(f"api_key: {wrong}")
            ws.ping().wait(2)
        # PING wraca jako PONG, CLOSE z reason - serwer loguje echo ("> %s"), trace takze reason w ASGI
        with connect(f"{ws_url}?api_key={TEST_KEY}") as ws:
            ws.recv(timeout=2)
            for key in (TEST_KEY, wrong):
                ws.ping("p" * 30 + key + "q" * 60).wait(2)
                ws.ping("p" * 60 + key + "q").wait(2)
            ws.close(4000, "c" * 20 + wrong + "d" * 60)
        time.sleep(0.2)
    finally:
        out = gw.stop()
    assert TEST_KEY not in out and wrong not in out
    for enc in encoded:  # zakodowany klucz, takze fragment (8 znakow)
        leaked = [enc[i:i + 24] for i in range(0, len(enc) - 23, 3) if enc[i:i + 24].lower() in out.lower()]
        assert not leaked, leaked
    for key in (TEST_KEY, wrong):  # nawet fragment klucza (przyciecie srodka ramki)
        leaked = [key[i:i + 12] for i in range(len(key) - 11) if key[i:i + 12] in out]
        assert not leaked, leaked
    assert "api_key=***" in out
    assert "[accepted]" in out  # logi INFO zostaja, tylko bez klucza
    assert "GET /api/v1/robots?api_key=*** HTTP/1.1" in out
    assert "%61pi_key=***" in out
    assert "/x/api_key%3D*** HTTP/1.1" in out and "stream%3Fapi_key%3D***" in out
    assert "GET /api/v1/robots?token=*** HTTP/1.1" in out and "/x/api_key%3A*** HTTP/1.1" in out
    if launch.endswith("_h11_debug"):
        assert "invalid HTTP header value: ***" in out  # traceback zostaje, wartosc nie
    if launch.endswith(("_debug", "_trace")):
        assert "x-api-key: ***" in out.lower()  # linia naglowka zostaje, wartosc nie
        assert "< TEXT [" in out and " bytes]" in out  # ramki klienta: typ i rozmiar, bez tresci
        assert "> PONG [" in out  # echo PING tez bez tresci
    if launch.endswith("_trace"):
        assert "'reason': '<" in out  # reason CLOSE od klienta tylko jako dlugosc


def _reads_until_reset(s: socket.socket) -> bool:
    s.settimeout(2)
    try:
        while s.recv(65536):
            pass
    except ConnectionResetError:
        return True
    return False


@pytest.mark.skipif(not shutil.which("lsof"), reason="lsof potrzebny do liczenia gniazd serwera")
def test_never_reading_clients_are_really_disconnected():
    # klienci, ktorzy nie czytaja, przy zalewie zdarzen: po przepelnieniu kolejki gniazdo musi zniknac
    # po stronie serwera (abort), inaczej zombie trzymaja bufory i nie licza sie do MAX_WS_CLIENTS
    ros_port, http_port, cap = free_port(), free_port(), 5
    bg = BgLoop()
    # ~1.4 KB na ramke x 1000/s (bez throttlingu): bufory TCP klienta i serwera pelne w ~1-2 s
    joints = json.dumps({"op": "publish", "topic": "/joint_states",
                         "msg": {"name": [f"J{i}_" + "x" * 100 for i in range(12)], "position": [0.1] * 12}})
    ros = FakeRosbridge(len(main.ROS_SUBSCRIPTIONS), [], flood=joints, flood_hz=1000)
    bg.run(ros.start(ros_port))
    gw = Gateway(dockerfile_cmd(http_port), {"API_KEY": TEST_KEY, "ROSBRIDGE_HOST": "127.0.0.1",
                                             "ROSBRIDGE_PORT": str(ros_port), "MQTT_ENABLED": "0",
                                             "MAX_WS_CLIENTS": str(cap), "WS_THROTTLE_S": "0"})
    base, socks = f"http://127.0.0.1:{http_port}", []
    try:
        wait_until(lambda: httpx.get(f"{base}/health", timeout=1).json()["rosbridge_connected"], 20, 0.2)
        for _wave in range(3):  # 3 x cap > cap: bez zwalniania slotow kolejne fale dostalyby 1013
            wave = [raw_ws_client(http_port, f"{WS_PATH}?api_key={TEST_KEY}") for _ in range(cap)]
            socks += wave
            wait_until(lambda: established_on(gw.proc.pid, http_port) == cap, 5, 0.1)
            # przepelnienie + 1 s na 1008 + RST; potem gniazda po stronie serwera znikaja
            wait_until(lambda: established_on(gw.proc.pid, http_port) == 0, 30, 0.2)
            # bez sierot FIN_WAIT_1 z niewyslanym buforem; klient widzi reset, nie stare ramki + FIN
            assert fin_wait_on(http_port) == 0
            for s in wave:
                assert _reads_until_reset(s), "expected RST, got EOF"
        # zywy klient nadal dostaje dane
        with connect(f"ws://127.0.0.1:{http_port}{WS_PATH}?api_key={TEST_KEY}") as ws:
            assert json.loads(ws.recv(timeout=2))["event"]["type"] == "snapshot"
            assert json.loads(ws.recv(timeout=2))["event"]["type"] == "joints"
    finally:
        for s in socks:
            s.close()
        out = gw.stop()
        bg.run(ros.stop())
        bg.close()
    assert "Traceback" not in out, out[-3000:]
    assert "[rosbridge] connection closed" not in out  # zrodlo (rosbridge) nie ucierpialo przez wolnych klientow


class _Reader:
    """Klient WS, ktory nic nie wysyla; ramki (czas monotoniczny, zdarzenie) zbierane w watku."""

    def __init__(self, url):
        self.url, self.ws, self.frames = url, None, []
        self.t = threading.Thread(target=self._run, daemon=True)
        self.t.start()
        wait_until(lambda: self.ws, 5)

    def _run(self):
        with contextlib.suppress(ConnectionClosed), connect(self.url) as ws:
            self.ws = ws
            for raw in ws:
                self.frames.append((time.monotonic(), json.loads(raw)["event"]))

    def status(self, since=0.0):
        return [(t, e["data"]) for t, e in self.frames if e["type"] == "status" and t >= since]

    def wait_status(self, pred, timeout, since=0.0):
        return wait_until(lambda: next(((t, d) for t, d in self.status(since) if pred(d)), None), timeout, 0.02)


def test_status_transitions_heartbeat_and_single_resubscribe():
    # robot nadaje -> online; przestaje (rosbridge dalej polaczony, GPS dalej idzie) -> offline po STALE+~1 s;
    # heartbeat do klienta, ktory nic nie wysyla; rosbridge znika -> status od razu; nieudane proby
    # (accept-and-close) nie kolejkuja subscribe: po powrocie dokladnie 11 operacji subscribe
    ros_port, mqtt_port, http_port = free_port(), free_port(), free_port()
    stale, hb = 1.0, 0.5
    bg = BgLoop()
    gps = {"latitude": 51.1, "longitude": 17.0, "satellites": 9, "fix_quality": 1}
    mqtt = bg.run(start_fake_mqtt(mqtt_port, main.MQTT_GPS_TOPIC, json.dumps(gps).encode(), every=0.2))
    imu = json.dumps({"op": "publish", "topic": "/imu/data", "msg": {
        "orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0}, "angular_velocity": {"x": 0.0, "y": 0.0, "z": 0.0},
        "linear_acceleration": {"x": 0.0, "y": 0.0, "z": 9.81}}})
    ros = FakeRosbridge(len(main.ROS_SUBSCRIPTIONS), [], flood=imu, flood_hz=50)
    bg.run(ros.start(ros_port))
    gw = Gateway(dockerfile_cmd(http_port), {
        "API_KEY": TEST_KEY, "ROSBRIDGE_HOST": "127.0.0.1", "ROSBRIDGE_PORT": str(ros_port),
        "MQTT_HOST": "127.0.0.1", "MQTT_PORT": str(mqtt_port), "STALE_AFTER_S": str(stale),
        "WS_HEARTBEAT_S": str(hb), "ROSBRIDGE_FAST_RETRY_S": "0.5", "ROSBRIDGE_SLOW_RETRY_S": "0.25"})
    base, rd, closer, ros2 = f"http://127.0.0.1:{http_port}", None, None, None
    try:
        wait_until(lambda: httpx.get(f"{base}/health", timeout=1).status_code == 200, 10)
        rd = _Reader(f"ws://127.0.0.1:{http_port}{WS_PATH}?api_key={TEST_KEY}")
        wait_until(lambda: rd.frames, 2)
        first = rd.frames[0][1]
        assert first["type"] == "snapshot"
        assert set(first["data"]["status"]) == {"online", "last_seen", "rosbridge_connected", "mqtt_connected"}

        rd.wait_status(lambda d: d["online"] and d["rosbridge_connected"] and d["mqtt_connected"], 10)
        assert ros.sub_ops == len(main.ROS_SUBSCRIPTIONS)
        rest = httpx.get(f"{base}/api/v1/robots/{RID}", headers=H).json()
        assert rest["online"] is True and rest["last_seen"] is not None

        # robot milknie (polaczenie zostaje), GPS nadal przychodzi
        ros.paused = True
        t_pause = time.monotonic()
        t_off, d_off = rd.wait_status(lambda d: not d["online"], stale + 3, since=t_pause)
        assert t_off - t_pause <= stale + 1.0 + 0.5, t_off - t_pause
        assert d_off["rosbridge_connected"] and d_off["mqtt_connected"]
        assert any(e["type"] == "gps" and t > t_pause for t, e in rd.frames)  # GPS szedl, a online spadl
        snap = httpx.get(f"{base}/api/v1/robots/{RID}/telemetry/latest", headers=H).json()
        assert snap["status"]["online"] is False and snap["gps_at"] > snap["status"]["last_seen"]
        assert httpx.get(f"{base}/api/v1/robots/{RID}", headers=H).json()["online"] is False

        # heartbeat: klient nic nie wysyla, a status przychodzi co WS_HEARTBEAT_S
        time.sleep(2.2)
        beats = [t for t, _ in rd.status(since=t_off + 0.01)]
        assert len(beats) >= 3, beats
        assert all(g < hb + 0.4 for g in (b - a for a, b in zip([t_off] + beats, beats))), beats

        # rosbridge znika: status rosbridge_connected=false od razu (nie czeka na tick ani heartbeat)
        t_kill = time.monotonic()
        bg.run(ros.stop())
        t_dn, _ = rd.wait_status(lambda d: not d["rosbridge_connected"], 3, since=t_kill)
        assert t_dn - t_kill < 0.5, t_dn - t_kill

        # nieudane proby: TCP przyjete i od razu zamkniete (kazda to 'close' w roslibpy bez 'ready')
        accepted = []
        closer = bg.run(start_accept_close(ros_port, accepted))
        wait_until(lambda: len(accepted) >= 6, 10, 0.05)
        closer.close()
        bg.run(closer.wait_closed())
        closer = None

        ros2 = FakeRosbridge(len(main.ROS_SUBSCRIPTIONS), [], flood=imu, flood_hz=50)
        bg.run(ros2.start(ros_port))
        t_up, d_up = rd.wait_status(lambda d: d["rosbridge_connected"], 5, since=t_dn + 0.01)
        rd.wait_status(lambda d: d["online"], 3, since=t_up)
        time.sleep(2.5)  # roslibpy z reconnect_on_close wysylalby zakolejkowane subscribe po 1 s
        assert ros2.subscribed == set(main.ROS_SUBSCRIPTIONS)
        assert ros2.sub_ops == len(main.ROS_SUBSCRIPTIONS), ros2.sub_ops  # nie 11 x (1 + liczba prob)
    finally:
        if rd:
            rd.ws.close()
        out = gw.stop()
        if closer is not None:
            closer.close()
        for r in (ros, ros2):
            if r is not None and r.server is not None:
                with contextlib.suppress(Exception):
                    bg.run(r.stop())
        mqtt.close()
        bg.close()
    assert len(accepted) >= 6
    assert out.count("[rosbridge] connected to") == 1 and out.count("[rosbridge] reconnected to") == 1, out[-3000:]
    assert out.count("[rosbridge] connection closed") == 1
    assert out.count("[rosbridge] still unreachable") == 1  # wejscie w faze wolna: jedna linia
    assert "Traceback" not in out, out[-3000:]


def test_dockerfile_copies_all_modules(tmp_path):
    # obraz dostaje tylko pliki z COPY - import main z samej tej listy (np. nowy modul bez wpisu w Dockerfile)
    line = next(ln for ln in (GATEWAY_DIR / "Dockerfile").read_text().splitlines()
                if ln.startswith("COPY ") and "main.py" in ln)
    for name in line.split()[1:-1]:
        shutil.copy(GATEWAY_DIR / name, tmp_path)
    env = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1", "PYTHONPATH": ""}
    r = subprocess.run([sys.executable, "-c", "import main"], cwd=tmp_path, env=env,
                       capture_output=True, text=True, timeout=60)
    assert r.returncode == 0, r.stderr
