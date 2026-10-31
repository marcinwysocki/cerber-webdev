"""Prawdziwy uvicorn w watku: polaczenie WS zamykane przez uvicorna (keepalive 1011) z niepustym buforem
zapisu nie moze wisiec po wyjsciu handlera - transport.close() czekalby na klienta, ktory nie czyta."""

import asyncio
import os
import shutil
import threading

import pytest
import uvicorn

import contracts as c
import main
from conftest import TEST_KEY
from helpers import established_on, free_port, raw_ws_client, wait_until

RID = main.ROBOT_ID


@pytest.mark.skipif(not shutil.which("lsof"), reason="lsof potrzebny do liczenia gniazd serwera")
def test_closing_ws_is_aborted_after_handler(monkeypatch):
    monkeypatch.setattr(main, "WS_THROTTLE_S", 0)
    monkeypatch.setattr(main, "WS_HEARTBEAT_S", 1)  # heartbeat status ujawnia zamkniete gniazdo po pauzie ruchu
    monkeypatch.setattr(main, "WS_ABORT_AFTER_S", 1.0)
    # wlasny robot (lifespan i handler czytaja robots[...] w trakcie): globalny zostaje nietkniety
    monkeypatch.setitem(main.robots, RID, main.RobotState(RID))
    port = free_port()
    server = uvicorn.Server(uvicorn.Config(main.app, host="127.0.0.1", port=port, ws_max_size=main.WS_MAX_SIZE,
                                           ws_ping_interval=3, ws_ping_timeout=3, log_config=None))
    th = threading.Thread(target=server.run, daemon=True)
    th.start()
    socks = []
    try:
        wait_until(lambda: server.started, 10)
        loop = server.servers[0].get_loop()
        robot = main.robots[RID]

        def run(coro, timeout=10):
            return asyncio.run_coroutine_threadsafe(coro, loop).result(timeout)

        socks = [raw_ws_client(port, f"/api/v1/robots/{RID}/telemetry/stream?api_key={TEST_KEY}") for _ in range(2)]
        wait_until(lambda: len(robot.clients) == 2, 5)
        clients = list(robot.clients)
        ev = c.JointsEvent(robot_id=RID, ts=0.0, data=c.Joints(names=[f"J{i}_" + "x" * 100 for i in range(12)],
                                                                 position=[0.1] * 12))

        async def fill():
            # bufory kernela pelne, w transporcie 20..64 KiB: ponizej high-water, wiec kolejka sie nie zapcha
            trs = [cl.ws.scope["gateway.transport"] for cl in clients]
            while min(t.get_write_buffer_size() for t in trs) < 20000:
                robot.apply(ev)
                await asyncio.sleep(0.002)
            return trs

        trs = run(fill())  # potem cisza (jak po zniknieciu rosbridge)
        assert not any(cl.slow for cl in clients)  # to nie sciezka wolnego klienta (1008 + abort)

        async def state():
            return [(t.is_closing(), t.get_write_buffer_size()) for t in trs]

        # keepalive uvicorna (ping 3 s + 3 s) wysyla 1011 i transport.close(); handler konczy sie przy heartbeacie
        wait_until(lambda: not robot.clients, 15, 0.1)
        st = run(state())
        assert all(closing and buffered > 0 for closing, buffered in st), st  # bez abortu: zombie bez konca
        wait_until(lambda: established_on(os.getpid(), port) == 0, main.WS_ABORT_AFTER_S + 3, 0.1)
    finally:
        for s in socks:
            s.close()
        server.should_exit = True
        th.join(10)
