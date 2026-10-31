"""Reconnect rosbridge: faza szybka (1 s .. ~ROS_RECONNECT_MAX_S) przez ROSBRIDGE_FAST_RETRY_S,
potem proba co ROSBRIDGE_SLOW_RETRY_S; udane polaczenie zeruje polityke. Zegar twisted task.Clock."""

import random

import pytest
from twisted.internet.protocol import ReconnectingClientFactory
from twisted.internet.task import Clock

import main

SLOW_LOG = "[rosbridge] still unreachable"


class _Connector:
    """Proba od razu sie nie udaje (jak connection refused) albo, gdy up, otwiera WS; zapisuje czas proby."""

    def __init__(self, f, clock):
        self.f, self.clock, self.attempts, self.up = f, clock, [], False

    def connect(self):
        self.attempts.append(self.clock.seconds())
        if self.up:
            self.f.ready(_Proto())  # WS otwarty - roslibpy wola resetDelay
        else:
            self.f.clientConnectionFailed(self, None)

    def stopConnecting(self):
        pass


class _Proto:
    _manual_disconnect = False


@pytest.fixture()
def ros(monkeypatch):
    monkeypatch.setattr(main, "ROSBRIDGE_HOST", "127.0.0.1")
    monkeypatch.setattr(main, "ROSBRIDGE_PORT", 1)
    random.seed(0)
    clock = Clock()
    f = main.make_ros(clock).factory  # reactor nie jest uruchamiany - polaczenie tylko zakolejkowane
    conn = _Connector(f, clock)
    return f, clock, conn


def _run(clock, until):
    while clock.seconds() < until and clock.getDelayedCalls():
        nxt = min(c.getTime() for c in clock.getDelayedCalls())
        clock.advance(max(0.0, min(nxt, until) - clock.seconds()))


def _gaps(times):
    return [b - a for a, b in zip(times, times[1:])]


def test_factory_config_is_per_instance(ros):
    f, _, _ = ros
    assert f.initialDelay == f.delay == 1.0 and main.ROS_RECONNECT_MAX_S <= 10  # sufit w retry gatewaya
    assert main.ROSBRIDGE_FAST_RETRY_S == 300 and main.ROSBRIDGE_SLOW_RETRY_S == 60  # domyslne
    # nie zmieniamy globalnie klasy twisted
    assert ReconnectingClientFactory.maxDelay == 3600 and ReconnectingClientFactory.jitter > 0
    assert ReconnectingClientFactory.retry is not f.retry


def test_fast_then_slow_forever(ros, capsys):
    f, clock, conn = ros
    conn.connect()  # pierwsza proba po starcie (t=0) - nieudana
    _run(clock, 3600)
    t = conn.attempts
    fast = [x for x in t if x < main.ROSBRIDGE_FAST_RETRY_S]
    g = _gaps(fast)
    assert 0.5 < g[0] < 1.5  # start ~1 s
    assert max(g) < main.ROS_RECONNECT_MAX_S * 1.6  # sufit ~10 s (+ jitter twisted ~12 %)
    assert len(fast) > 25
    # faza wolna: dokladnie co ROSBRIDGE_SLOW_RETRY_S (bez jittera), bez konca
    slow = [x for x in t if x >= main.ROSBRIDGE_FAST_RETRY_S + main.ROS_RECONNECT_MAX_S * 1.6]
    assert len(slow) > 40 and all(abs(d - main.ROSBRIDGE_SLOW_RETRY_S) < 1e-6 for d in _gaps(slow))
    # przejscie: pierwsza wolna przerwa zaczyna sie od pierwszej nieudanej proby po 300 s
    first_slow = min(x for x in t if x >= main.ROSBRIDGE_FAST_RETRY_S)
    assert first_slow < main.ROSBRIDGE_FAST_RETRY_S + main.ROS_RECONNECT_MAX_S * 1.6
    assert t[t.index(first_slow) + 1] - first_slow == main.ROSBRIDGE_SLOW_RETRY_S
    assert capsys.readouterr().out.count(SLOW_LOG) == 1  # jedna linia przy wejsciu, nie per proba


def test_short_env_values(monkeypatch, ros, capsys):
    f, clock, conn = ros
    monkeypatch.setattr(main, "ROSBRIDGE_FAST_RETRY_S", 20)
    monkeypatch.setattr(main, "ROSBRIDGE_SLOW_RETRY_S", 3)
    conn.connect()
    _run(clock, 200)
    slow = [x for x in conn.attempts if x >= 20 + main.ROS_RECONNECT_MAX_S * 1.6]
    assert len(slow) > 50 and all(abs(d - 3) < 1e-6 for d in _gaps(slow))
    out = capsys.readouterr().out
    assert out.count(SLOW_LOG) == 1 and "after 20 s, retrying every 3 s" in out


def test_success_resets_policy(ros, capsys):
    f, clock, conn = ros
    conn.connect()
    _run(clock, 1000)  # gleboko w fazie wolnej
    assert f.slow and capsys.readouterr().out.count(SLOW_LOG) == 1
    conn.up = True  # rosbridge wraca: najblizsza proba (faza wolna) sie udaje
    _run(clock, clock.seconds() + main.ROSBRIDGE_SLOW_RETRY_S)
    assert not f.slow and f.delay == 1.0 and f.jitter == ReconnectingClientFactory.jitter
    assert not clock.getDelayedCalls()

    lost_at = clock.seconds()
    n, conn.up = len(conn.attempts), False
    f.clientConnectionLost(conn, None)  # utrata: znow faza szybka od tej chwili
    _run(clock, lost_at + main.ROSBRIDGE_FAST_RETRY_S - 20)
    again = conn.attempts[n:]
    assert 0.5 < again[0] - lost_at < 1.5
    assert max(_gaps(again)) < main.ROS_RECONNECT_MAX_S * 1.6 and len(again) > 20
    assert not f.slow and capsys.readouterr().out.count(SLOW_LOG) == 0
    _run(clock, lost_at + main.ROSBRIDGE_FAST_RETRY_S + 200)  # znow wolna, znow jedna linia
    assert f.slow and capsys.readouterr().out.count(SLOW_LOG) == 1
    assert _gaps(conn.attempts[-3:]) == [main.ROSBRIDGE_SLOW_RETRY_S] * 2


def _old_twisted_retry(self, connector=None):
    # Twisted <= 25.x: najpierw delay = min(delay * factor, maxDelay), potem callLater - gdyby gateway
    # delegowal do retry twisted, faza wolna wychodzilaby co maxDelay (10 s), a pierwsza przerwa ~2.7 s
    raise AssertionError("retry twisted nie moze byc uzywane")


@pytest.mark.parametrize("fast,slow", [(300, 60), (20, 3)])
def test_gap_sequence_independent_of_twisted(monkeypatch, fast, slow):
    monkeypatch.setattr(ReconnectingClientFactory, "retry", _old_twisted_retry)
    monkeypatch.setattr(main, "ROSBRIDGE_HOST", "127.0.0.1")
    monkeypatch.setattr(main, "ROSBRIDGE_PORT", 1)
    monkeypatch.setattr(main, "ROSBRIDGE_FAST_RETRY_S", fast)
    monkeypatch.setattr(main, "ROSBRIDGE_SLOW_RETRY_S", slow)
    clock = Clock()
    f = main.make_ros(clock).factory
    f.jitter = 0  # dokladne przerwy
    conn = _Connector(f, clock)
    conn.connect()
    _run(clock, fast + 10 * slow + 30)
    # oczekiwane: 1, e, e^2, potem sufit 10 s, a od pierwszej nieudanej proby po `fast` - co `slow`
    expected, t, d = [], 0.0, 1.0
    while len(expected) < len(conn.attempts) - 1:
        gap = slow if t >= fast else d
        d = d if t >= fast else min(d * f.factor, main.ROS_RECONNECT_MAX_S)
        expected.append(gap)
        t += gap
    assert _gaps(conn.attempts) == pytest.approx(expected)
    assert expected[:3] == pytest.approx([1.0, f.factor, f.factor ** 2]) and expected.count(slow) >= 10


def test_stop_trying_stops_retries(ros, capsys):
    f, clock, conn = ros
    conn.connect()
    _run(clock, 30)
    f.stopTrying()
    n = len(conn.attempts)
    _run(clock, 3600)
    assert len(conn.attempts) == n and not clock.getDelayedCalls()
    assert SLOW_LOG not in capsys.readouterr().out


class _Delayed:
    def __init__(self, delay):
        self.delay = delay

    def active(self):
        return True

    def reset(self, delay):
        self.delay = delay


class _TcpConnector:
    """Jak twisted BaseConnector.connect: timeoutID ustawione przed startedConnecting."""

    def __init__(self):
        self.timeout = 30  # domyslne connectWS
        self.timeoutID = _Delayed(30)


def test_connect_timeout_shortened(monkeypatch):
    # milczacy host (peer VPN wylaczony): kazda proba czekalaby 30 s na TCP zamiast ROS_CONNECT_TIMEOUT_S
    monkeypatch.setattr(main, "ROSBRIDGE_HOST", "127.0.0.1")
    monkeypatch.setattr(main, "ROSBRIDGE_PORT", 1)
    f = main.make_ros().factory
    conn = _TcpConnector()
    f.startedConnecting(conn)  # pierwsza proba: biezacy timer skrocony
    assert conn.timeout == main.ROS_CONNECT_TIMEOUT_S <= 5 and conn.timeoutID.delay == main.ROS_CONNECT_TIMEOUT_S
    conn.timeoutID = _Delayed(conn.timeout)  # retry na tym samym connectorze juz z krotkim timeoutem
    f.startedConnecting(conn)
    assert conn.timeoutID.delay == main.ROS_CONNECT_TIMEOUT_S
