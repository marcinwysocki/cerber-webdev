"""Wspolna konfiguracja testow: klucz testowy i wylaczone zrodla przed importem main."""

import os
import secrets
import sys
from pathlib import Path

import pytest

GATEWAY_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(GATEWAY_DIR))

TEST_KEY = "test-" + secrets.token_hex(12)  # losowy, tylko na czas testow
os.environ["API_KEY"] = TEST_KEY
os.environ["ROSBRIDGE_ENABLED"] = "0"
os.environ["MQTT_ENABLED"] = "0"


@pytest.fixture()
def env(monkeypatch):
    """Atrapa petli z recznym zegarem (call_later), WS_THROTTLE_S=1, robot r1 z klientem zapisujacym ramki."""
    import main
    from helpers import FakeClient, FakeLoop

    loop = FakeLoop()
    monkeypatch.setattr(main.asyncio, "get_running_loop", lambda: loop)
    monkeypatch.setattr(main, "WS_THROTTLE_S", 1.0)
    robot = main.RobotState("r1")
    client = FakeClient(loop)
    robot.clients.add(client)
    return loop, robot, client
