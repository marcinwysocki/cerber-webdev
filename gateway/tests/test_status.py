"""Status robota: online = rosbridge polaczony i swieza wiadomosc ROS (GPS sie nie liczy), zdarzenie `status`
od razu przy zmianie (bez throttlingu) i jako heartbeat co WS_HEARTBEAT_S; snapshot z polem status.
Rosbridge: subscribe topikow dokladnie raz na polaczenie, niezaleznie od liczby nieudanych prob."""

import asyncio
import json
import time
import types

import pytest
from twisted.internet.task import Clock

import contracts as c
import main
from conftest import TEST_KEY
from helpers import imu
from test_slow_consumer import FakeWS, _until


@pytest.fixture()
def rb(monkeypatch):
    """Stan rosbridge jak ustawiany w watku reactora (ros_ready); monkeypatch przywraca go po tescie."""
    monkeypatch.setattr(main, "ros_client", object())
    monkeypatch.setattr(main, "ros_ready", False)
    monkeypatch.setattr(main, "mqtt_connected", False)

    def set_(v: bool):
        main.ros_ready = v
    return set_


class Rec:
    """Klient z robot.clients: (czas monotoniczny, zdarzenie)."""

    def __init__(self):
        self.frames = []

    def offer(self, text):
        self.frames.append((time.monotonic(), json.loads(text)["event"]))
        return True

    def of(self, kind):
        return [(t, e) for t, e in self.frames if e["type"] == kind]


def gps(ts):
    return c.GpsEvent(robot_id="r1", ts=ts, data=main.gps_from_mqtt({"latitude": 51.1, "longitude": 17.0}))


def test_gps_alone_does_not_make_online(env, rb):
    loop, robot, client = env
    rb(True)
    robot.check_status()
    for _ in range(5):
        robot.apply(gps(time.time()))
    assert robot.snapshot.gps_at is not None  # swiezosc GPS w gps_at
    assert robot.last_seen is None and not robot.online()
    s = robot.summary()
    assert s.online is False and s.last_seen is None
    assert [e["data"]["online"] for _, e in client.of("status")] == [False]  # tylko zmiana rosbridge
    t = time.time()
    robot.apply(imu(t, 1.0))  # wiadomosc ROS: online od razu, bez czekania na tick
    assert robot.online() and robot.summary().online and robot.summary().last_seen == t
    st = client.of("status")[-1][1]["data"]
    assert st == {"online": True, "last_seen": t, "rosbridge_connected": True, "mqtt_connected": False}


def test_online_requires_rosbridge(env, rb):
    loop, robot, client = env
    robot.apply(imu(time.time(), 1.0))  # swieza wiadomosc, ale rosbridge niepolaczony
    assert not robot.online() and robot.last_seen is not None and not client.of("status")
    rb(True)
    assert robot.check_status() and client.of("status")[-1][1]["data"]["online"] is True


def test_status_on_change_not_throttled(env, rb, monkeypatch):
    loop, robot, client = env
    assert main.WS_THROTTLE_S == 1.0
    for i in range(6):  # 6 zmian w jednej chwili - throttling (okno 1 s) by je zjadl
        rb(i % 2 == 0)
        assert robot.check_status()
    monkeypatch.setattr(main, "mqtt_connected", True)
    assert robot.check_status()
    assert not robot.check_status()  # bez zmiany nic nie wychodzi
    st = client.of("status")
    assert len(st) == 7 and all(t == 0.0 for t, _ in st)
    assert [e["data"]["rosbridge_connected"] for _, e in st[:6]] == [True, False] * 3
    assert st[-1][1]["data"]["mqtt_connected"] is True
    assert loop.created == 0 and not robot._windows  # status bez okien throttlingu
    rb(True)
    for _ in range(5):  # status nie zuzywa okna typu ani odwrotnie
        robot.apply(imu(time.time(), 1.0))
    assert len(client.of("imu")) == 1 and client.of("status")[-1][1]["data"]["online"] is True


def test_offline_after_stale_detected_by_task(monkeypatch, rb):
    # robot przestaje nadawac, rosbridge dalej polaczony: offline najpozniej ~STATUS_TICK_S po STALE_AFTER_S
    async def scenario():
        monkeypatch.setattr(main, "STALE_AFTER_S", 0.5)
        monkeypatch.setattr(main, "WS_HEARTBEAT_S", 0)  # tylko zmiany
        robot, client = main.RobotState("r1"), Rec()
        robot.clients.add(client)
        rb(True)
        task = asyncio.create_task(main.status_task(robot))
        try:
            for i in range(5):
                robot.apply(imu(time.time(), float(i)))
                await asyncio.sleep(0.02)
            t_last, last_seen = time.monotonic(), robot.last_seen
            assert client.of("status")[-1][1]["data"]["online"] is True
            await _until(lambda: client.of("status")[-1][1]["data"]["online"] is False, 3)
            t_off, ev = client.of("status")[-1]
            assert 0.5 - 0.05 <= t_off - t_last <= 0.5 + main.STATUS_TICK_S + 0.2, t_off - t_last
            assert ev["data"] == {"online": False, "last_seen": last_seen, "rosbridge_connected": True,
                                  "mqtt_connected": False}
            await asyncio.sleep(1.2)
            assert len(client.of("status")) == 2  # bez heartbeatu: tylko online i offline
        finally:
            task.cancel()

    asyncio.run(scenario())


class TimedWS(FakeWS):
    def __init__(self):
        super().__init__()
        self.times = []

    async def send_text(self, text):
        await super().send_text(text)
        self.times.append(time.monotonic())


def test_heartbeat_reaches_silent_client(monkeypatch, rb):
    # klient nic nie wysyla; heartbeat co WS_HEARTBEAT_S przez kolejke klienta (jeden sender)
    async def scenario():
        robot = main.RobotState("r1")
        monkeypatch.setitem(main.robots, "r1", robot)
        monkeypatch.setattr(main, "WS_HEARTBEAT_S", 0.2)
        rb(True)
        robot.check_status()
        ws = TimedWS()
        task = asyncio.create_task(main.status_task(robot))
        handler = asyncio.create_task(main.telemetry_stream(ws, "r1", api_key=TEST_KEY))
        try:
            await asyncio.sleep(1.3)
            frames = [json.loads(s)["event"] for s in ws.sent]
            assert frames[0]["type"] == "snapshot"
            assert frames[0]["data"]["status"]["rosbridge_connected"] is True
            hb = [t for t, f in zip(ws.times, frames) if f["type"] == "status"]
            assert 5 <= len(hb) <= 7, len(hb)
            gaps = [b - a for a, b in zip(hb, hb[1:])]
            assert all(0.15 < g < 0.35 for g in gaps), gaps
            assert all(f["data"]["rosbridge_connected"] for f in frames if f["type"] == "status")
            assert ws.max_inflight == 1
            ws.incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
            await asyncio.wait_for(handler, 1)
        finally:
            task.cancel()

    asyncio.run(scenario())


# --- rosbridge: subscribe raz na polaczenie, status przy connect/disconnect ---


class _Proto:
    _manual_disconnect = False

    def __init__(self):
        self.sent = []

    def send_ros_message(self, m):
        self.sent.append(dict(m))


class _Connector:
    def connect(self):
        pass

    def stopConnecting(self):
        pass


def test_resubscribe_exactly_once_per_connection(monkeypatch, capsys):
    async def scenario():
        clock, scheduled = Clock(), []
        make = main.make_ros
        monkeypatch.setattr(main, "ROSBRIDGE_HOST", "127.0.0.1")
        monkeypatch.setattr(main, "ROSBRIDGE_PORT", 1)

        def fake_make_ros():
            ros = make(clock)
            # bez reactora: run nic nie robi; call_later roslibpy (reconnect_on_close) bylby tu zapisany
            ros.factory._manager = types.SimpleNamespace(run=lambda: None,
                                                         call_later=lambda d, cb: scheduled.append(cb))
            return ros
        monkeypatch.setattr(main, "make_ros", fake_make_ros)
        monkeypatch.setattr(main, "ros_ready", False)
        monkeypatch.setattr(main, "ros_client", None)  # start_rosbridge ustawia globalny; przywracany po tescie
        robot, client = main.RobotState("r1"), Rec()
        robot.clients.add(client)
        ros = main.start_rosbridge(asyncio.get_running_loop(), robot)
        f, conn = ros.factory, _Connector()
        try:
            p1 = _Proto()
            f.ready(p1)  # pierwsze polaczenie
            assert sorted(m["topic"] for m in p1.sent) == sorted(main.ROS_SUBSCRIPTIONS)
            assert {m["op"] for m in p1.sent} == {"subscribe"}
            await asyncio.sleep(0)
            assert client.of("status")[-1][1]["data"]["rosbridge_connected"] is True

            f.clientConnectionLost(conn, None)  # utrata
            await asyncio.sleep(0)
            assert client.of("status")[-1][1]["data"]["rosbridge_connected"] is False
            for _ in range(40):  # nieudane proby z 'close' bez 'ready' (accept-and-close, milczacy peer, 503)
                f.clientConnectionLost(conn, None)
                clock.advance(60)
            await asyncio.sleep(0)
            assert len(client.of("status")) == 2  # nieudane proby nie zmieniaja stanu

            p2 = _Proto()
            f.ready(p2)  # reconnect: dokladnie 11 subscribe, te same id co za pierwszym razem
            clock.advance(3600)
            assert len(p2.sent) == len(main.ROS_SUBSCRIPTIONS) and len(p1.sent) == len(main.ROS_SUBSCRIPTIONS)
            assert p2.sent == p1.sent
            assert not scheduled  # nic nie kolejkuje resubskrypcji na pozniej
            await asyncio.sleep(0)
            assert client.of("status")[-1][1]["data"]["rosbridge_connected"] is True

            f.clientConnectionLost(conn, None)
            p3 = _Proto()
            f.ready(p3)
            assert p3.sent == p1.sent
        finally:
            f.stopTrying()

    asyncio.run(scenario())
    out = capsys.readouterr().out
    assert out.count("[rosbridge] connected to") == 1 and out.count("[rosbridge] reconnected to") == 2
    assert out.count("[rosbridge] connection closed") == 2  # tylko utraty otwartego WS, nie nieudane proby
