"""Wolny klient WS nie blokuje innych ani zrodel: harness in-process z atrapami WebSocket."""

import asyncio
import time

import contracts as c
import main
from conftest import TEST_KEY


class FakeTransport:
    """abort() jak w asyncio: gniazdo znika od razu, uvicorn wrzuca websocket.disconnect."""

    def __init__(self, ws):
        self.ws = ws
        self.aborted = False

    def abort(self):
        self.aborted = True
        self.ws.incoming.put_nowait({"type": "websocket.disconnect", "code": 1006})


class FakeWS:
    """Atrapa starlette WebSocket. stuck=True: send/close wisza jak przy zapchanym TCP."""

    def __init__(self, stuck: bool = False, transport: bool = True):
        self.stuck = stuck
        self.sent: list[str] = []
        self.closed = None
        self.inflight = 0
        self.max_inflight = 0
        self.incoming: asyncio.Queue = asyncio.Queue()
        self.transport = FakeTransport(self) if transport else None
        self.scope = {"gateway.transport": self.transport}

    async def accept(self):
        pass

    async def send_text(self, text: str):
        self.inflight += 1
        self.max_inflight = max(self.max_inflight, self.inflight)
        try:
            if self.stuck:
                await asyncio.Event().wait()
            await asyncio.sleep(0)
            self.sent.append(text)
        finally:
            self.inflight -= 1

    async def receive(self):
        return await self.incoming.get()

    async def close(self, code: int = 1000, reason=None):
        self.closed = code
        if self.stuck:
            await asyncio.Event().wait()


async def _until(fn, timeout=2.0):
    deadline = time.monotonic() + timeout
    while not fn():
        assert time.monotonic() < deadline, "timeout"
        await asyncio.sleep(0.001)


def test_apply_is_sync():
    # zrodla (watek ROS przez call_soon_threadsafe, MQTT) wolaja apply bez await
    assert not asyncio.iscoroutinefunction(main.RobotState.apply)
    assert not asyncio.iscoroutinefunction(main.RobotState.broadcast)


def test_slow_client_dropped_without_blocking(monkeypatch):
    async def scenario():
        robot = main.RobotState("r1")
        monkeypatch.setitem(main.robots, "r1", robot)
        monkeypatch.setattr(main, "WS_THROTTLE_S", 0)  # kazde zdarzenie do klientow - test fan-outu
        fast, slow = FakeWS(), FakeWS(stuck=True)
        t_fast = asyncio.create_task(main.telemetry_stream(fast, "r1", api_key=TEST_KEY))
        t_slow = asyncio.create_task(main.telemetry_stream(slow, "r1", api_key=TEST_KEY))
        await _until(lambda: len(robot.clients) == 2)

        n = 3 * main.WS_QUEUE_MAX
        worst = 0.0
        for _ in range(n):
            ev = c.PtzEvent(robot_id="r1", ts=time.time(), data=c.Ptz(state=c.PtzState.WAITING))
            t0 = time.perf_counter()
            robot.apply(ev)  # nie czeka na siec: zapchany klient nie spowalnia zrodla
            worst = max(worst, time.perf_counter() - t0)
            await asyncio.sleep(0)  # tempo zrodla: szybki klient odbiera na biezaco
        assert worst < 0.05

        # wolny: proba 1008, close wisi (zapchany TCP) -> RST (abort) transportu, handler konczy sie
        assert any(cl.ws is slow for cl in robot.clients)  # slot zajety do zerwania gniazda
        await asyncio.wait_for(t_slow, 3)
        assert slow.closed == 1008 and slow.transport.aborted
        assert all(cl.ws is not slow for cl in robot.clients)

        # szybki: dostal snapshot i wszystkie zdarzenia, nigdy dwa send naraz
        await _until(lambda: len(fast.sent) == n + 1)
        assert '"type":"snapshot"' in fast.sent[0]
        assert fast.max_inflight == 1

        # rozlaczenie: sender anulowany, klient usuniety
        (fc,) = robot.clients
        fast.incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(t_fast, 1)
        assert not robot.clients
        await _until(lambda: fc.sender.done())

    asyncio.run(scenario())


def test_status_heartbeat_goes_through_sender(monkeypatch):
    async def scenario():
        robot = main.RobotState("r1")
        monkeypatch.setitem(main.robots, "r1", robot)
        monkeypatch.setattr(main, "WS_HEARTBEAT_S", 0.01)
        ws = FakeWS()
        hb = asyncio.create_task(main.status_task(robot))
        t = asyncio.create_task(main.telemetry_stream(ws, "r1", api_key=TEST_KEY))
        await _until(lambda: sum('"type":"status"' in s for s in ws.sent) >= 3)
        assert ws.max_inflight == 1
        ws.incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(t, 1)
        hb.cancel()

    asyncio.run(scenario())


def test_slow_client_without_transport_holds_slot(monkeypatch):
    # bez transportu (send owiniety) nie da sie zerwac TCP: klient trzyma slot MAX_WS_CLIENTS az zniknie
    async def scenario():
        robot = main.RobotState("r1")
        monkeypatch.setitem(main.robots, "r1", robot)
        monkeypatch.setattr(main, "WS_CLOSE_WAIT_S", 0.05)
        monkeypatch.setattr(main, "MAX_WS_CLIENTS", 1)
        monkeypatch.setattr(main, "WS_THROTTLE_S", 0)
        slow = FakeWS(stuck=True, transport=False)
        t = asyncio.create_task(main.telemetry_stream(slow, "r1", api_key=TEST_KEY))
        await _until(lambda: len(robot.clients) == 1)
        for _ in range(2 * main.WS_QUEUE_MAX):
            robot.apply(c.PtzEvent(robot_id="r1", ts=time.time(), data=c.Ptz(state=c.PtzState.WAITING)))
        await asyncio.sleep(0.3)
        assert not t.done() and len(robot.clients) == 1

        extra = FakeWS()  # limit liczy tez wolnego klienta w trakcie zrywania
        await asyncio.wait_for(main.telemetry_stream(extra, "r1", api_key=TEST_KEY), 1)
        assert extra.closed == 1013

        slow.incoming.put_nowait({"type": "websocket.disconnect", "code": 1006})
        await asyncio.wait_for(t, 1)
        assert not robot.clients

    asyncio.run(scenario())
