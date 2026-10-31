"""Throttling WS per typ (WS_THROTTLE_S): leading + trailing, jeden timer na typ, snapshot bez throttlingu.
Atrapa petli z recznym zegarem - deterministycznie, bez czekania."""

import asyncio
import json

import pytest

import contracts as c
import main
from conftest import TEST_KEY
from helpers import imu
from test_slow_consumer import FakeWS, _until


def ptz(ts, state):
    return c.PtzEvent(robot_id="r1", ts=ts, data=c.Ptz(state=state))


def test_max_one_frame_per_second_per_type(env):
    loop, robot, client = env
    n = 0
    for i in range(3 * 200):  # IMU 200 Hz przez 3 s + ultrasound 50 Hz rownolegle
        robot.apply(imu(loop.now, float(i)))
        if i % 4 == 0:
            robot.apply(c.UltrasoundEvent(robot_id="r1", ts=loop.now, data=float(i)))
        n = i
        loop.advance(0.005)
        assert len(loop.active()) <= 2  # jeden timer na typ, nigdy na zdarzenie
    loop.advance(5)
    for kind, last in (("imu", float(n)), ("ultrasound", float(n - n % 4))):
        frames = client.of(kind)
        times = [t for t, _ in frames]
        assert all(b - a >= 1.0 - 1e-9 for a, b in zip(times, times[1:])), times
        assert 3 <= len(frames) <= 4
        assert frames[0][0] == 0.0  # leading: pierwsze od razu
        value = frames[-1][1]["data"] if kind == "ultrasound" else frames[-1][1]["data"]["angular_velocity"]["x"]
        assert value == last  # trailing: ostatnia wartosc zawsze dochodzi
    assert loop.created <= 2 * 5  # timery = okna, nie zdarzenia (600 + 150 zdarzen)
    assert not loop.active() and not robot._windows and not robot._pending


def test_trailing_within_window_then_leading_again(env):
    loop, robot, client = env
    robot.apply(imu(0.0, 1.0))  # leading
    loop.advance(0.3)
    robot.apply(imu(0.3, 2.0))
    loop.advance(0.3)
    robot.apply(imu(0.6, 3.0))  # zastepuje 2.0
    assert [e["data"]["angular_velocity"]["x"] for _, e in client.of("imu")] == [1.0]
    loop.advance(0.4)  # t=1.0: koniec okna -> pending
    assert [(t, e["data"]["angular_velocity"]["x"]) for t, e in client.of("imu")] == [(0.0, 1.0), (1.0, 3.0)]
    assert client.of("imu")[-1][1]["ts"] == 0.6  # ts z momentu odbioru, nie wyslania
    loop.advance(1.0)  # t=2.0: okno bez pending -> zamkniete, nic nie wychodzi
    assert len(client.of("imu")) == 2 and not robot._windows
    loop.advance(0.5)
    robot.apply(imu(loop.now, 4.0))  # t=2.5: poza oknem -> od razu
    assert client.of("imu")[-1][0] == pytest.approx(2.5) and len(client.of("imu")) == 3


def test_ptz_moving_then_waiting_ends_on_waiting(env):
    loop, robot, client = env
    robot.apply(ptz(0.0, c.PtzState.WAITING))
    loop.advance(0.2)
    robot.apply(ptz(0.2, c.PtzState.MOVING))
    for _ in range(5):
        loop.advance(0.1)
        robot.apply(ptz(loop.now, c.PtzState.MOVING))
    robot.apply(ptz(loop.now, c.PtzState.WAITING))  # t=0.7, w oknie
    loop.advance(3)
    states = [e["data"]["state"] for _, e in client.of("ptz")]
    assert states == ["WAITING", "WAITING"]  # posrednie MOVING zastapione, koniec zawsze na ostatnim
    robot.apply(ptz(loop.now, c.PtzState.MOVING))
    robot.apply(ptz(loop.now, c.PtzState.WAITING))
    loop.advance(1.0)
    assert [e["data"]["state"] for _, e in client.of("ptz")][-2:] == ["MOVING", "WAITING"]


def test_types_independent_and_all_throttled(env):
    loop, robot, client = env
    events = [
        imu(0.0, 1.0),
        c.JointsEvent(robot_id="r1", ts=0.0, data=c.Joints(names=["j"] * 12, position=[0.0] * 12)),
        c.OdometryEvent(robot_id="r1", ts=0.0, data=c.Odometry(
            position=c.Vec3(x=0, y=0, z=0), yaw=0, velocity_body=c.Vec3(x=0, y=0, z=0), yaw_rate=0)),
        c.UltrasoundEvent(robot_id="r1", ts=0.0, data=1.0),
        ptz(0.0, c.PtzState.WAITING),
        c.SensorsAliveEvent(robot_id="r1", ts=0.0, data=c.SensorsAlive(imu=True)),
        c.GpsEvent(robot_id="r1", ts=0.0, data=main.gps_from_mqtt({"latitude": 51.1, "longitude": 17.0})),
    ]
    assert {e.type for e in events} == set(main.THROTTLED)
    for _ in range(10):
        for e in events:
            robot.apply(e)
    # kazdy typ: pierwsze od razu (typy sie nie blokuja), reszta czeka na koniec okna
    assert sorted(e["type"] for _, e in client.frames) == sorted(main.THROTTLED)
    loop.advance(1.0)
    assert len(client.frames) == 2 * len(main.THROTTLED)


def test_snapshot_not_throttled(env):
    loop, robot, client = env
    for i in range(50):
        robot.apply(imu(loop.now, float(i)))
        robot.apply(ptz(loop.now, c.PtzState.MOVING if i % 2 else c.PtzState.WAITING))
        assert robot.snapshot.imu.angular_velocity.x == float(i)
        assert robot.snapshot.imu_at == loop.now and robot.last_seen == loop.now
        assert robot.snapshot.ptz.state == (c.PtzState.MOVING if i % 2 else c.PtzState.WAITING)
        loop.advance(0.01)
    assert len(client.of("imu")) == 1  # WS w tym czasie: tylko leading


def test_zero_disables_throttling(env, monkeypatch):
    loop, robot, client = env
    monkeypatch.setattr(main, "WS_THROTTLE_S", 0)
    for i in range(100):
        robot.apply(imu(loop.now, float(i)))
        robot.apply(ptz(loop.now, c.PtzState.WAITING))
    assert len(client.of("imu")) == 100 and len(client.of("ptz")) == 100
    assert loop.created == 0 and not robot._windows


def test_real_loop_end_to_end(monkeypatch):
    # prawdziwa petla i handler WS: burst w jednym ticku -> leading + trailing, snapshot pierwszy
    async def scenario():
        robot = main.RobotState("r1")
        monkeypatch.setitem(main.robots, "r1", robot)
        monkeypatch.setattr(main, "WS_THROTTLE_S", 0.05)
        ws = FakeWS()
        t = asyncio.create_task(main.telemetry_stream(ws, "r1", api_key=TEST_KEY))
        await _until(lambda: len(robot.clients) == 1 and ws.sent)
        robot.apply(ptz(1.0, c.PtzState.WAITING))
        for _ in range(50):
            robot.apply(ptz(1.0, c.PtzState.MOVING))
        robot.apply(ptz(1.0, c.PtzState.WAITING))
        await _until(lambda: len(ws.sent) == 3)
        await asyncio.sleep(0.15)  # okno zamkniete, nic wiecej nie wychodzi
        frames = [json.loads(s)["event"] for s in ws.sent]
        assert [f["type"] for f in frames] == ["snapshot", "ptz", "ptz"]
        assert [f["data"]["state"] for f in frames[1:]] == ["WAITING", "WAITING"]
        assert not robot._windows
        ws.incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(t, 1)

    asyncio.run(scenario())
