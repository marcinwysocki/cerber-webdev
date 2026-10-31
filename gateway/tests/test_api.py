"""Zachowanie in-process: autoryzacja REST/WS, ptz, limit klientow WS, NaN/Inf, redakcja logow."""

import asyncio
import contextlib
import json
import logging
import math
import socket
import struct
import sys

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError
from starlette.websockets import WebSocketDisconnect

import contracts as c
import logs
import main
from conftest import TEST_KEY

RID = main.ROBOT_ID
WS = f"/api/v1/robots/{RID}/telemetry/stream"
H = {"X-API-Key": TEST_KEY}


@pytest.fixture()
def client(monkeypatch):
    # swiezy stan robota: asercje REST nie zaleza od testow, ktore wczesniej karmily globalnego robota
    monkeypatch.setitem(main.robots, RID, main.RobotState(RID))
    with TestClient(main.app) as tc:
        yield tc


def test_health_public_and_ok_without_sources(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.parametrize("headers", [{}, {"X-API-Key": "wrong"}])
def test_rest_requires_key(client, headers):
    assert client.get("/api/v1/robots", headers=headers).status_code == 401


def test_rest_with_key(client):
    assert client.get("/api/v1/robots", headers=H).json()["robots"][0]["robot_id"] == RID
    latest = client.get(f"/api/v1/robots/{RID}/telemetry/latest", headers=H)
    assert latest.status_code == 200
    assert latest.json()["status"] == {"online": False, "last_seen": None, "rosbridge_connected": False,
                                       "mqtt_connected": False}
    assert client.get("/api/v1/robots/nope", headers=H).status_code == 404
    assert client.get(f"/api/v1/robots/{RID}/gps/latest", headers=H).status_code == 404


@pytest.mark.parametrize("path,code", [(f"{WS}?api_key=wrong", 4401), (f"{WS}", 4401),
                                       (f"/api/v1/robots/nope/telemetry/stream?api_key={TEST_KEY}", 4404)])
def test_ws_close_codes(client, path, code):
    with client.websocket_connect(path) as ws:
        with pytest.raises(WebSocketDisconnect) as ei:
            ws.receive_text()
    assert ei.value.code == code


def test_ws_first_frame_is_snapshot(client):
    with client.websocket_connect(f"{WS}?api_key={TEST_KEY}") as ws:
        ev = ws.receive_json()["event"]
        assert ev["type"] == "snapshot"
        assert set(ev["data"]["status"]) == {"online", "last_seen", "rosbridge_connected", "mqtt_connected"}
        c.TelemetryEventEnvelope.model_validate({"event": ev})


def test_ping_event_removed():
    # `status` zastapil `ping`: envelope nie przyjmuje juz ping, OpenAPI nie zna keepalive
    with pytest.raises(ValidationError):
        c.TelemetryEventEnvelope.model_validate({"event": {"type": "ping", "ts": 1.0}})
    assert not hasattr(c, "PingEvent")
    mapping = c.TelemetryEventEnvelope.model_json_schema()["properties"]["event"]["discriminator"]["mapping"]
    assert "ping" not in mapping and mapping["status"].endswith("/StatusEvent")
    st = {"online": True, "last_seen": 1.0, "rosbridge_connected": True, "mqtt_connected": False}
    env = c.TelemetryEventEnvelope.model_validate({"event": {"type": "status", "robot_id": RID, "ts": 2.0, "data": st}})
    assert env.event.data.online is True


def test_ws_client_cap_closes_1013(client, monkeypatch):
    monkeypatch.setattr(main, "MAX_WS_CLIENTS", 2)
    url = f"{WS}?api_key={TEST_KEY}"
    with client.websocket_connect(url) as a, client.websocket_connect(url) as b:
        a.receive_json(), b.receive_json()
        with client.websocket_connect(url) as extra:
            with pytest.raises(WebSocketDisconnect) as ei:
                extra.receive_text()
        assert ei.value.code == 1013
    # po rozlaczeniu sloty sie zwalniaja
    with client.websocket_connect(url) as again:
        assert again.receive_json()["event"]["type"] == "snapshot"


def test_close_codes_documented(client):
    ws_doc = client.get("/openapi.json").json()["x-websockets"][WS.replace(RID, "{robot_id}")]
    assert {"4401", "4404", "1008", "1009", "1011", "1013"} <= set(ws_doc["closeCodes"])
    assert "keepalive" not in ws_doc and ws_doc["status"]["heartbeat_s"] == main.WS_HEARTBEAT_S
    assert "ping`" not in ws_doc["description"] and "WS_HEARTBEAT_S" in ws_doc["description"]


def test_ptz_drops_ip():
    p = main.ptz_from_ros({"data": "[PTZ] MOVING | IP: 192.0.2.10"})
    assert p.state == c.PtzState.MOVING and "192.0.2" not in p.model_dump_json()
    assert main.ptz_from_ros({"data": "garbage"}).state == c.PtzState.UNKNOWN


IMU_OK = {"orientation": {"x": 0.0, "y": 0.0, "z": 0.0, "w": 1.0},
          "angular_velocity": {"x": 0.0, "y": 0.0, "z": 0.0},
          "linear_acceleration": {"x": 0.0, "y": 0.0, "z": 9.81}}


@pytest.mark.parametrize("bad", [math.nan, math.inf, -math.inf])
def test_non_finite_rejected(bad):
    imu = {**IMU_OK, "linear_acceleration": {"x": bad, "y": 0.0, "z": 9.81}}
    with pytest.raises(ValidationError):
        main.imu_from_ros(imu)
    with pytest.raises(ValidationError):
        main.imu_from_ros({**IMU_OK, "orientation": {"x": bad, "y": 0.0, "z": 0.0, "w": 1.0}})
    with pytest.raises(ValidationError):
        c.UltrasoundEvent(robot_id=RID, ts=1.0, data=bad)
    with pytest.raises(ValidationError):
        main.gps_from_mqtt({"latitude": bad, "longitude": 17.0})
    with pytest.raises(ValidationError):
        main.joints_from_ros({"name": ["j"] * 12, "position": [0.0] * 11 + [bad]})
    assert main.imu_from_ros(IMU_OK).linear_acceleration.z == 9.81


def _record(msg, args):
    return logging.LogRecord("uvicorn.error", logging.INFO, __file__, 1, msg, args, None)


def test_redact_filter_msg_and_args():
    f = logs.RedactApiKey(TEST_KEY)
    r = _record('%s - "WebSocket %s" [accepted]', ("127.0.0.1:1", f"/x?a=1&api_key={TEST_KEY}&b=2"))
    f.filter(r)
    assert TEST_KEY not in r.getMessage() and "api_key=***&b=2" in r.getMessage()
    r = _record(f"GET /x?API%5FKEY={TEST_KEY} HTTP/1.1", None)
    f.filter(r)
    assert TEST_KEY not in r.getMessage()
    r = _record("%(p)s", ({"p": f"/x?api_key={TEST_KEY}"},))  # LogRecord rozpakowuje (dict,)
    f.filter(r)
    assert TEST_KEY not in r.getMessage()
    # --log-level trace: uvicorn.asgi loguje caly scope z query_string w bytes
    r = _record("%s [%d] Started scope=%s", ("x", 3, {"query_string": f"api_key={TEST_KEY}".encode()}))
    f.filter(r)
    assert TEST_KEY not in r.getMessage() and "[3]" in r.getMessage()
    # nazwa z zakodowanymi literami (Starlette dekoduje ja do api_key)
    for name in ("%61pi_key", "%61%70%69%5f%6b%65%79", "a%50I_KEY"):
        r = _record('%s - "WebSocket %s" [accepted]', ("127.0.0.1:1", f"/x?{name}={TEST_KEY}"))
        f.filter(r)
        assert TEST_KEY not in r.getMessage() and f"{name}=***" in r.getMessage()
    # --log-level debug: websockets loguje naglowki handshake jako "< %s: %s"
    for name in ("x-api-key", "X-API-Key"):
        r = _record("< %s: %s", (name, TEST_KEY))
        f.filter(r)
        assert TEST_KEY not in r.getMessage() and r.getMessage() == f"< {name}: ***"
    r = _record("< %s: %s", ("user-agent", "python/3"))  # inne naglowki bez zmian
    f.filter(r)
    assert r.args == ("user-agent", "python/3")


class _Op:
    name = "TEXT"


class _Frame:
    """Jak websockets Frame: __str__ pokazuje poczatek i koniec tresci, srodek wycina."""

    opcode = _Op()

    def __init__(self, data: bytes):
        self.data = data

    def __str__(self):
        t = self.data.decode()
        return f"TEXT {t[:48]!r}...{t[-24:]!r} [{len(self.data)} bytes]"


def test_redact_client_frames_and_json_key():
    f = logs.RedactApiKey(TEST_KEY)
    other = "other-" + TEST_KEY[::-1]
    for key in (TEST_KEY, other):
        body = json.dumps({"type": "auth", "client": "x" * 16, "ts": 1.0, "api_key": key}).encode()
        r = logging.LogRecord("uvicorn.error", logging.DEBUG, __file__, 1, "< %s", (_Frame(body),), None)
        f.filter(r)
        assert r.getMessage() == f"< TEXT [{len(body)} bytes]"
        # ten sam tekst bez ramki (np. inny log): klucz po "api_key": / api_key: tez ukryty
        for text in (f'{{"api_key": "{key}"}}', f"api_key: {key}", f"API_KEY:{key}"):
            r = _record("%s", (text,))
            f.filter(r)
            assert key not in r.getMessage() and "***" in r.getMessage(), r.getMessage()
    # ramki wychodzace tez bez tresci: echo PING klienta (PONG) i CLOSE niosa jego dane
    body = ("p" * 30 + TEST_KEY + "q" * 60).encode()
    r = logging.LogRecord("uvicorn.error", logging.DEBUG, __file__, 1, "> %s", (_Frame(body),), None)
    f.filter(r)
    assert r.getMessage() == f"> TEXT [{len(body)} bytes]"
    # trace uvicorn.asgi: reason z websocket.disconnect tylko jako dlugosc
    msg = {"type": "websocket.disconnect", "code": 4000, "reason": "c" * 20 + other}
    r = _record("%s ASGI [%d] Receive %s", ("127.0.0.1:1 - ", 1, msg))
    f.filter(r)
    assert other not in r.getMessage() and f"<{len(msg['reason'])} chars>" in r.getMessage()


def test_redact_header_parse_error_any_key():
    # websockets (h11): "invalid HTTP header value: <wartosc>" bez nazwy naglowka - tez obcy klucz
    f = logs.RedactApiKey(TEST_KEY)
    other = "other-" + TEST_KEY[::-1]
    for what in ("value", "name", "line"):
        try:
            raise ValueError(f"invalid HTTP header {what}: {other}\x7f")
        except ValueError:
            r = logging.LogRecord("uvicorn.error", logging.DEBUG, __file__, 1, "! no valid HTTP request", None,
                                  sys.exc_info())
        f.filter(r)
        text = logging.Formatter().format(r)
        assert other not in text and f"invalid HTTP header {what}: ***" in text
    r = _record("invalid HTTP header value: %s", (other,))  # format z %s: placeholder zostaje, wartosc nie
    f.filter(r)
    assert r.getMessage() == "invalid HTTP header value: ***"


def test_redact_keeps_access_log_args():
    # AccessFormatter uvicorna rozpakowuje record.args (5 pol) - filtr nie moze ich skleic
    f = logs.RedactApiKey(TEST_KEY)
    r = logging.LogRecord("uvicorn.access", logging.INFO, __file__, 1, '%s - "%s %s HTTP/%s" %d',
                          ("127.0.0.1:1", "GET", f"/x?api_key={TEST_KEY}", "1.1", 200), None)
    f.filter(r)
    assert isinstance(r.args, tuple) and len(r.args) == 5 and r.args[2] == "/x?api_key=***"


def test_redact_filter_installed_on_uvicorn_loggers():
    for name in ("uvicorn.error", "uvicorn.access", "uvicorn.asgi"):
        assert any(isinstance(f, logs.RedactApiKey) for f in logging.getLogger(name).filters)


def test_redact_percent_encoded_separator():
    # uvicorn loguje sciezke przez quote(): "/x/api_key=K" -> "/x/api_key%3DK"; surowe query bez zmian
    f = logs.RedactApiKey(TEST_KEY)
    other = "other-" + TEST_KEY[::-1]  # nie skonfigurowany klucz: tu dziala tylko regex, nie podmiana klucza
    for key in (TEST_KEY, other):
        for path in (f"/x/api_key%3D{key}", f"/x%3Bapi_key%3D{key}", f"/stream%3Fapi_key%3D{key}",
                     f"/stream?api_key%3d{key}", f"/stream?%61pi_key%3D{key}&b=2"):
            r = _record('%s - "WebSocket %s" [accepted]', ("127.0.0.1:1", path))
            f.filter(r)
            assert key not in r.getMessage() and "***" in r.getMessage(), r.getMessage()


def test_redact_bare_key_and_traceback():
    f = logs.RedactApiKey(TEST_KEY)
    # klucz bez etykiety (np. pozycja w komunikacie wyjatku)
    r = _record("invalid HTTP header value: %s", (TEST_KEY + "\x7f",))
    f.filter(r)
    assert TEST_KEY not in r.getMessage()
    # exc_info: traceback formatowany pozniej przez Formatter - filtr przygotowuje zredagowany exc_text
    try:
        raise ValueError(f"invalid HTTP header value: {TEST_KEY}")
    except ValueError:
        r = logging.LogRecord("uvicorn.error", logging.DEBUG, __file__, 1, "! invalid handshake", None,
                              sys.exc_info())
    f.filter(r)
    text = logging.Formatter().format(r)
    assert TEST_KEY not in text and "ValueError: invalid HTTP header value: ***" in text
    # krotki klucz nie jest podmieniany wszedzie (zamaskowalby przypadkowe fragmenty logu)
    assert logs.key_re("abcdefg") is None
    r = _record("abc def", None)
    logs.RedactApiKey("abc").filter(r)
    assert r.getMessage() == "abc def"


def _pct(s: str, fmt: str = "%{:02X}") -> str:
    return "".join(fmt.format(b) for b in s.encode())


@pytest.mark.parametrize("key", [TEST_KEY, "Ab+/=c d~Q9==", "klucz-zażółć-123"])
def test_redact_bare_key_any_percent_encoding(key):
    # klucz bez etykiety (?token=, w sciezce, raw_path na trace) zakodowany niekanonicznie: male %2b (.NET),
    # pelne %XX, mieszane; repr bytes (raw_path) z \xNN dla znakow spoza ASCII
    from urllib.parse import quote
    f = logs.RedactApiKey(key)
    lower = "".join(ch if ch.isalnum() and ch.isascii() else _pct(ch, "%{:02x}") for ch in key)
    forms = {key, quote(key), quote(key, safe=""), lower, _pct(key), _pct(key, "%{:02x}"),
             "".join(_pct(ch) if i % 2 else ch for i, ch in enumerate(key))}
    for form in forms:
        for msg, args in (('%s - "GET %s HTTP/%s" %d', ("127.0.0.1:1", f"/api/v1/robots?token={form}", "1.1", 200)),
                          ('%s - "WebSocket %s" [accepted]', ("127.0.0.1:1", f"/api/v1/robots/{form}/telemetry/stream")),
                          ("< GET %s HTTP/1.1", (f"/api/v1/robots/{form}/telemetry/stream",)),
                          ("%s", ({"path": "/api/v1/robots/***", "raw_path": f"/api/v1/robots/{form}".encode()},))):
            r = _record(msg, args)
            f.filter(r)
            m = r.getMessage()
            assert form not in m and "***" in m, (form, m)
    r = _record("%s", ({"raw_path": f"/r/{key}".encode()},))  # bytes repr: \xc5\xbc...
    f.filter(r)
    assert r.getMessage().count("***") == 1, r.getMessage()


def test_redact_encoded_colon_and_quote_separators():
    # quote() w logu uvicorna: ":" -> %3A, '"' -> %22; obcy klucz lapie tylko regex z etykieta
    f = logs.RedactApiKey(TEST_KEY)
    other = "other-" + TEST_KEY[::-1]
    for path in (f"/x/api_key%3A{other}", f"/x/api_key%3a{other}", f"/api/v1/robots?api_key%3A{other}",
                 f"/x/%22api_key%22%3A%22{other}%22", f"/x/api_key:{other}", f'/x/"api_key":"{other}"'):
        r = _record('%s - "GET %s HTTP/%s" %d', ("127.0.0.1:1", path, "1.1", 404))
        f.filter(r)
        assert other not in r.getMessage() and "***" in r.getMessage(), r.getMessage()
    r = _record("%s", ({"raw_path": f"/x/api_key%3a{other}".encode()},))
    f.filter(r)
    assert other not in r.getMessage()


def test_twisted_warnings_silenced():
    # autobahn przez PythonLoggingObserver roslibpy: WARN "dropping connection to peer" przy kazdej probie
    lg = logging.getLogger("twisted")
    assert not lg.isEnabledFor(logging.WARNING) and lg.isEnabledFor(logging.ERROR)


class _Sock:
    def __init__(self):
        self.opts = []

    def setsockopt(self, *a):
        self.opts.append(a)


class _Tr:
    def __init__(self):
        self.aborted = 0
        self.sock = _Sock()

    def get_extra_info(self, name):
        return self.sock if name == "socket" else None

    def abort(self):
        self.aborted += 1


class _Proto:
    def __init__(self):
        self.transport = _Tr()

    async def send(self, msg):
        pass


@pytest.mark.parametrize("fails", [False, True])
def test_ws_transport_aborted_after_handler(monkeypatch, fails):
    # uvicorn transport.close() czeka na oproznienie bufora; nieczytajacy klient -> twardy abort po czasie
    monkeypatch.setattr(main, "WS_ABORT_AFTER_S", 0.05)

    async def app(scope, receive, send):
        if scope["type"] == "http":
            return
        assert scope["gateway.transport"] is proto.transport
        if fails:
            raise RuntimeError("boom")

    async def scenario():
        mw = main.KeepTransport(app)
        with pytest.raises(RuntimeError) if fails else contextlib.nullcontext():
            await mw({"type": "websocket"}, None, proto.send)
        assert proto.transport.aborted == 0  # nie od razu: normalny close handshake ma czas
        await asyncio.sleep(0.1)
        assert proto.transport.aborted == 1
        # RST zamiast FIN: SO_LINGER 0 ustawione tuz przed abort (wczesniej gniazdo zamyka sie lagodnie)
        assert proto.transport.sock.opts == [(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))]
        await mw({"type": "http"}, None, proto.send)  # HTTP bez zmian
        await asyncio.sleep(0.1)
        assert proto.transport.aborted == 1

    proto = _Proto()
    asyncio.run(scenario())


def test_drop_log_one_line_and_rate_limited(capsys):
    # NaN z czujnika 200 Hz: pierwsza linia od razu, potem najwyzej co DROP_LOG_EVERY_S z liczba pominietych
    now = [0.0]
    drop = logs.DropLog("[rosbridge]", every=10.0, clock=lambda: now[0])
    try:
        main.imu_from_ros({**IMU_OK, "linear_acceleration": {"x": math.nan, "y": 0.0, "z": 9.81}})
    except ValidationError as e:
        ex = e
    for _ in range(1000):  # 5 s po 200 Hz
        drop("/imu/data", ex)
        now[0] += 0.005
    drop("/leg_odom2", KeyError("pose"))  # inne zrodlo ma wlasny limit
    lines = capsys.readouterr().out.splitlines()
    assert lines == ["[rosbridge] /imu/data: drop Vec3 x: Input should be a finite number",
                     "[rosbridge] /leg_odom2: drop KeyError('pose')"]
    now[0] = 10.0
    drop("/imu/data", ex)
    assert capsys.readouterr().out == ("[rosbridge] /imu/data: drop Vec3 x: Input should be a finite number"
                                       " (999 more dropped since last line)\n")
    try:
        main.gps_from_mqtt({"latitude": math.nan, "longitude": math.inf})
    except ValidationError as e:
        assert logs.why(e) == "GPSFix latitude: Input should be a finite number (+1 more)"
