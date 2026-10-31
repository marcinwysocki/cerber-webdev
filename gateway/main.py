"""
Cerber Telemetry Gateway - publiczne, read-only API nad robotem Lite3.

Stoi w sieci VPN: czyta rosbridge (ws://JETSON:9090) i MQTT
(cerber/gps/data), skleja z tego TelemetrySnapshot i wystawia:

  GET  /health
  GET  /api/v1/robots
  GET  /api/v1/robots/{robot_id}
  GET  /api/v1/robots/{robot_id}/telemetry/latest
  GET  /api/v1/robots/{robot_id}/gps/latest
  GET  /api/v1/robots/{robot_id}/streams
  WS   /api/v1/robots/{robot_id}/telemetry/stream?api_key=...
  GET  /openapi.json, /docs

WS: kazdy typ zdarzenia najwyzej raz na WS_THROTTLE_S (ostatnia wartosc zawsze dochodzi),
snapshot aktualny zawsze. Zdarzenie `status` (online, rosbridge/mqtt) od razu przy zmianie i co
WS_HEARTBEAT_S. online = rosbridge polaczony i wiadomosc ROS w ciagu STALE_AFTER_S.
Rosbridge: reconnect szybki (1..10 s) przez ROSBRIDGE_FAST_RETRY_S, potem co ROSBRIDGE_SLOW_RETRY_S.

Zadnego sterowania - komendy zostaja za VPN. Autoryzacja: naglowek X-API-Key
(REST) lub query `api_key` (WebSocket). Konfiguracja przez zmienne srodowiskowe.

Uruchomienie:  uvicorn main:app --host 0.0.0.0 --port 8000 --ws-max-size 4096
Zaleznosci:    fastapi uvicorn pydantic roslibpy aiomqtt
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import random
import secrets
import socket
import struct
import time
from contextlib import asynccontextmanager, suppress
from typing import Any, Dict, Optional, Set

from fastapi import Depends, FastAPI, Header, HTTPException, Query, WebSocket, status

import logs
from logs import DropLog, log, why
from contracts import (
    API_VERSION, Error, GPSFix, GpsEvent, Health, Imu, ImuEvent, Joints, JointsEvent,
    Odometry, OdometryEvent, Ptz, PtzEvent, PtzState, Quaternion, RobotList, RobotStatus, RobotSummary,
    SensorsAlive, SensorsAliveEvent, SnapshotEvent, StatusEvent, TelemetryEventEnvelope,
    TelemetrySnapshot, UltrasoundEvent, Vec3, VideoStream, VideoStreamList,
)

# --- konfiguracja ----------------------------------------------------------

ROBOT_ID = os.getenv("ROBOT_ID", "cerber-lite3-01")
ROBOT_MODEL = os.getenv("ROBOT_MODEL", "DEEP Robotics Lite3 Venture")
API_KEY = os.getenv("API_KEY", "")  # wymagany: bez niego aplikacja nie wystartuje (lifespan)
ROSBRIDGE_HOST = os.getenv("ROSBRIDGE_HOST", "10.13.13.2")
ROSBRIDGE_PORT = int(os.getenv("ROSBRIDGE_PORT", "9090"))
MQTT_HOST = os.getenv("MQTT_HOST", "10.13.13.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_GPS_TOPIC = os.getenv("MQTT_GPS_TOPIC", "cerber/gps/data")
MEDIAMTX_BASE = os.getenv("MEDIAMTX_BASE", "").rstrip("/")  # pusta = whep_url wzgledne: /video/<cam>/whep
STALE_AFTER_S = float(os.getenv("STALE_AFTER_S", "5"))
WS_THROTTLE_S = float(os.getenv("WS_THROTTLE_S", "1.0"))  # max 1 ramka WS na tyle sekund na typ; 0 = bez
WS_HEARTBEAT_S = float(os.getenv("WS_HEARTBEAT_S", "5"))  # status do kazdego klienta co tyle; 0 = tylko zmiany
STATUS_TICK_S = 1.0  # co tyle task statusu sprawdza zmiany (np. offline przez brak wiadomosci ROS)
STREAMS_VPN_ONLY = os.getenv("STREAMS_VPN_ONLY", "1") == "1"
ROSBRIDGE_ENABLED = os.getenv("ROSBRIDGE_ENABLED", "1") == "1"
MQTT_ENABLED = os.getenv("MQTT_ENABLED", "1") == "1"
MAX_WS_CLIENTS = int(os.getenv("MAX_WS_CLIENTS", "20"))  # na robota; nadmiarowe -> close 1013
WS_QUEUE_MAX = 100  # ramek w kolejce klienta WS; przepelnienie = wolny klient -> rozlaczenie
WS_MAX_SIZE = 4096  # max ramka od klienta [B] (po dekompresji); to samo w Dockerfile i README
WS_CLOSE_WAIT_S = 1.0  # tyle czekamy na wyslanie 1008 do wolnego klienta, potem abort TCP
WS_ABORT_AFTER_S = 12.0  # twardy abort TCP po wyjsciu handlera WS; uvicorn czeka do 10 s na odpowiedz close
ROS_RECONNECT_MAX_S = 10.0  # sufit fazy szybkiej; twisted domyslnie 3600 s (factor e) - proba za ~godzine
ROSBRIDGE_FAST_RETRY_S = float(os.getenv("ROSBRIDGE_FAST_RETRY_S", "300"))  # faza szybka od utraty polaczenia
ROSBRIDGE_SLOW_RETRY_S = float(os.getenv("ROSBRIDGE_SLOW_RETRY_S", "60"))  # potem proba co tyle, bez konca
ROS_CONNECT_TIMEOUT_S = 5.0  # timeout TCP jednej proby; connectWS ma 30 s (milczacy host = proba co ~40 s)

ROS_SUBSCRIPTIONS = {
    "/imu/data": "sensor_msgs/Imu",
    "/joint_states": "sensor_msgs/JointState",
    "/leg_odom2": "nav_msgs/Odometry",
    "/us_publisher/ultrasound_distance": "std_msgs/Float64",
    "/ptz/status": "std_msgs/String",
    "/sensor_status/imu_isalive": "std_msgs/Bool",
    "/sensor_status/odom2_isalive": "std_msgs/Bool",
    "/sensor_status/joint_isalive": "std_msgs/Bool",
    "/sensor_status/realsense_isalive": "std_msgs/Bool",
    "/sensor_status/lidar_isalive": "std_msgs/Bool",
    "/sensor_status/ultrasound_isalive": "std_msgs/Bool",
}
SENSOR_ALIVE_KEYS = {
    "/sensor_status/imu_isalive": "imu",
    "/sensor_status/odom2_isalive": "odom",
    "/sensor_status/joint_isalive": "joints",
    "/sensor_status/realsense_isalive": "realsense",
    "/sensor_status/lidar_isalive": "lidar",
    "/sensor_status/ultrasound_isalive": "ultrasound",
}
VIDEO_STREAMS = [("normal_cam", "video/normal_cam"), ("IR_cam", "video/IR_cam")]
THROTTLED = ("imu", "joints", "odometry", "ultrasound", "ptz", "sensors_alive", "gps")  # bez snapshot i status


logs.install(API_KEY)  # klucz z query WS nie moze trafic do logow uvicorna


# --- konwersja ROS -> model publiczny --------------------------------------


def quat_to_rpy(q: Dict[str, float]) -> tuple[float, float, float]:
    x, y, z, w = q["x"], q["y"], q["z"], q["w"]
    roll = math.atan2(2 * (w * x + y * z), 1 - 2 * (x * x + y * y))
    s = max(-1.0, min(1.0, 2 * (w * y - z * x)))
    pitch = math.asin(s)
    yaw = math.atan2(2 * (w * z + x * y), 1 - 2 * (y * y + z * z))
    return roll, pitch, yaw


def imu_from_ros(m: Dict[str, Any]) -> Imu:
    r, p, y = quat_to_rpy(m["orientation"])
    return Imu(
        orientation=Quaternion(**m["orientation"]), roll=r, pitch=p, yaw=y,
        angular_velocity=Vec3(**m["angular_velocity"]),
        linear_acceleration=Vec3(**m["linear_acceleration"]),
    )


def joints_from_ros(m: Dict[str, Any]) -> Joints:
    return Joints(names=list(m["name"]), position=[float(v) for v in m["position"]])


def odom_from_ros(m: Dict[str, Any]) -> Odometry:
    pose = m["pose"]["pose"]
    tw = m["twist"]["twist"]
    _, _, yaw = quat_to_rpy(pose["orientation"])
    return Odometry(
        position=Vec3(**pose["position"]), yaw=yaw,
        velocity_body=Vec3(x=tw["linear"]["x"], y=tw["linear"]["y"], z=0.0),
        yaw_rate=tw["angular"]["z"],
    )


def ptz_from_ros(m: Dict[str, Any]) -> Ptz:
    # format: "[PTZ] WAITING | IP: ..." - IP glowicy celowo nie trafia do publicznego API
    raw = m.get("data", "")
    state = raw.partition("|")[0].replace("[PTZ]", "").strip()
    try:
        return Ptz(state=PtzState(state))
    except ValueError:
        return Ptz(state=PtzState.UNKNOWN)


def gps_from_mqtt(m: Dict[str, Any]) -> GPSFix:
    return GPSFix(
        time=m.get("date"), latitude=m["latitude"], longitude=m["longitude"],
        altitude=m.get("altitude", 0.0), satellites=m.get("satellites", 0),
        fix_quality=m.get("fix_quality", 0),
    )


# --- stan + rozglaszanie ---------------------------------------------------


class WsClient:
    """Klient WS: ograniczona kolejka i jedyny task (sender) piszacy do gniazda -
    snapshot, zdarzenia i status ida ta sama droga, wiec nigdy dwa send naraz."""

    def __init__(self, ws: WebSocket):
        self.ws = ws
        self.queue: asyncio.Queue[str] = asyncio.Queue(maxsize=WS_QUEUE_MAX)
        self.slow = False
        self.sender: Optional[asyncio.Task] = None

    def offer(self, text: str) -> bool:
        """Nieblokujace. Pelna kolejka = klient nie odbiera: zatrzymujemy sender, handler go rozlaczy."""
        if self.slow:
            return False
        try:
            self.queue.put_nowait(text)
            return True
        except asyncio.QueueFull:
            self.slow = True
            if self.sender is not None:
                self.sender.cancel()
            return False

    async def send_loop(self) -> None:
        with suppress(Exception):  # rozlaczony klient po prostu konczy sender
            while True:
                await self.ws.send_text(await self.queue.get())


class RobotState:
    def __init__(self, robot_id: str):
        self.snapshot = TelemetrySnapshot(robot_id=robot_id, ts=time.time())
        self.last_seen: Optional[float] = None  # ostatnia wiadomosc ROS (GPS/MQTT sie nie liczy)
        self.clients: Set[WsClient] = set()
        self._windows: Dict[str, asyncio.TimerHandle] = {}  # otwarte okna throttlingu, timer na typ
        self._pending: Dict[str, Any] = {}  # ostatnie zdarzenie typu, ktore przyszlo w oknie
        self._status_key = (False, False, False)  # (online, rosbridge, mqtt) ostatnio rozeslane

    def online(self) -> bool:
        return (rosbridge_is_connected() and self.last_seen is not None
                and time.time() - self.last_seen < STALE_AFTER_S)

    def status(self) -> RobotStatus:
        return RobotStatus(online=self.online(), last_seen=self.last_seen,
                           rosbridge_connected=rosbridge_is_connected(), mqtt_connected=mqtt_connected)

    def summary(self) -> RobotSummary:
        return RobotSummary(robot_id=self.snapshot.robot_id, model=ROBOT_MODEL,
                            online=self.online(), last_seen=self.last_seen)

    def current_snapshot(self) -> TelemetrySnapshot:
        self.snapshot.status = self.status()
        return self.snapshot

    def check_status(self) -> bool:
        """Rozsyla `status` (bez throttlingu), gdy zmienil sie online / rosbridge / mqtt. Wolane z taska
        statusu (offline przez brak wiadomosci), z watku ROS przez call_soon_threadsafe i z MQTT."""
        st = self.status()
        key = (st.online, st.rosbridge_connected, st.mqtt_connected)
        if key == self._status_key:
            return False
        self._status_key = key
        self.send_status(st)
        return True

    def send_status(self, st: Optional[RobotStatus] = None) -> None:
        self.broadcast(StatusEvent(robot_id=self.snapshot.robot_id, ts=time.time(), data=st or self.status()))

    def apply(self, event) -> None:
        """Wprowadza zdarzenie do snapshotu i rozsyla je klientom WS. Synchroniczne:
        zrodla (watek ROS, MQTT) nigdy nie czekaja na siec klientow."""
        now = event.ts
        kind = event.type
        if kind != "gps":
            self.last_seen = now
            if not self._status_key[0]:
                self.check_status()  # pierwsza wiadomosc ROS po ciszy -> online od razu
        s = self.snapshot
        s.ts = now
        if kind == "imu":
            s.imu, s.imu_at = event.data, now
        elif kind == "joints":
            s.joints, s.joints_at = event.data, now
        elif kind == "odometry":
            s.odometry, s.odometry_at = event.data, now
        elif kind == "ultrasound":
            s.ultrasound, s.ultrasound_at = event.data, now
        elif kind == "ptz":
            s.ptz, s.ptz_at = event.data, now
        elif kind == "gps":
            s.gps, s.gps_at = event.data, now
        elif kind == "sensors_alive":
            s.sensors_alive = event.data

        # throttling WS (snapshot wyzej aktualny zawsze): pierwsze zdarzenie od razu i otwiera okno,
        # w oknie tylko podmiana pending, na koniec okna pending wychodzi i otwiera nastepne
        if kind in THROTTLED and WS_THROTTLE_S > 0:
            if kind in self._windows:
                self._pending[kind] = event
                return
            self._open_window(kind)
        self.broadcast(event)

    def _open_window(self, kind: str) -> None:
        self._windows[kind] = asyncio.get_running_loop().call_later(WS_THROTTLE_S, self._window_end, kind)

    def _window_end(self, kind: str) -> None:
        event = self._pending.pop(kind, None)
        if event is None:
            del self._windows[kind]
            return
        self._open_window(kind)
        self.broadcast(event)

    def broadcast(self, event) -> None:
        if not self.clients:
            return
        text = TelemetryEventEnvelope(event=event).model_dump_json()
        for c in list(self.clients):
            c.offer(text)  # wolny klient zostaje w clients (zajmuje slot) az gniazdo naprawde zniknie


robots: Dict[str, RobotState] = {ROBOT_ID: RobotState(ROBOT_ID)}
ros_client = None  # roslibpy.Ros
ros_ready = False  # WS do rosbridge otwarty (event 'ready' .. 'close'); is_connected roslibpy to samo TCP
mqtt_connected = False


def rosbridge_is_connected() -> bool:
    return ros_client is not None and ros_ready


def get_robot(robot_id: str) -> RobotState:
    r = robots.get(robot_id)
    if r is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail=f"Unknown robot_id: {robot_id}")
    return r


# --- zrodla: rosbridge (watek) i MQTT (asyncio) -----------------------------


def make_ros(clock=None):
    """roslibpy.Ros z reconnectem: faza szybka (backoff 1 s .. ROS_RECONNECT_MAX_S) przez
    ROSBRIDGE_FAST_RETRY_S od utraty polaczenia (lub od startu), potem proba co ROSBRIDGE_SLOW_RETRY_S
    bez konca; udane polaczenie zeruje polityke. Polaczenie jest tylko zakolejkowane w reactorze
    Twisted - nic tu nie czeka na rosbridge."""
    import roslibpy
    from twisted.internet import reactor

    ros = roslibpy.Ros(host=ROSBRIDGE_HOST, port=ROSBRIDGE_PORT)
    f = ros.factory  # ReconnectingClientFactory; na instancji, nie globalnie na klasie
    f.initialDelay = f.delay = 1.0
    f.clock = clock or reactor  # testy: twisted.internet.task.Clock
    f.fast_since, f.slow = f.clock.seconds(), False  # faza szybka liczona od startu
    base_reset = f.resetDelay

    def retry(connector=None):
        # wolane z clientConnectionFailed/Lost (w reactorze) po kazdej nieudanej probie / utracie.
        # Wlasne planowanie zamiast retry twisted: kolejnosc jitter/mnozenie/sufit zmienia sie miedzy wersjami
        if not f.continueTrying:
            return
        connector = connector or f.connector
        if connector is None:
            return
        now = f.clock.seconds()
        if f.fast_since is None:
            f.fast_since = now
        if not f.slow and now - f.fast_since >= ROSBRIDGE_FAST_RETRY_S:
            f.slow = True
            log(f"[rosbridge] still unreachable after {ROSBRIDGE_FAST_RETRY_S:g} s,"
                f" retrying every {ROSBRIDGE_SLOW_RETRY_S:g} s")
        if f.slow:
            d = ROSBRIDGE_SLOW_RETRY_S
        else:  # 1, e, e^2, potem sufit; jitter jak w twisted (~12 %), ale nie kumuluje sie w delay
            d = max(0.1, random.normalvariate(f.delay, f.delay * f.jitter)) if f.jitter else f.delay
            f.delay = min(f.delay * f.factor, ROS_RECONNECT_MAX_S)
        f.retries += 1

        def reconnect():
            f._callID = None
            connector.connect()

        f._callID = f.clock.callLater(d, reconnect)

    def reset_delay():
        # ready() factory po otwarciu WS: nastepna utrata zaczyna od fazy szybkiej
        base_reset()
        f.fast_since, f.slow = None, False

    f.retry, f.resetDelay = retry, reset_delay

    def started_connecting(connector):
        # connectWS daje connectorowi 30 s; retry uzywa tego samego connectora, wiec wystarczy raz
        if connector.timeout != ROS_CONNECT_TIMEOUT_S:
            connector.timeout = ROS_CONNECT_TIMEOUT_S
            tid = getattr(connector, "timeoutID", None)
            if tid is not None and tid.active():
                tid.reset(ROS_CONNECT_TIMEOUT_S)

    f.startedConnecting = started_connecting  # hook twisted ClientFactory (w reactorze)
    # ping WS wykrywa martwe polaczenie (robot/VPN zgasl bez FIN) -> close -> reconnect
    f.setProtocolOptions(autoPingInterval=10, autoPingTimeout=5)
    return ros


def start_rosbridge(loop: asyncio.AbstractEventLoop, robot: RobotState):
    """roslibpy (Twisted) w watku daemon; callbacki wrzucaja zdarzenia do petli asyncio.
    Start nie czeka na rosbridge: gdy robot/VPN nie odpowiada, API dziala (health degraded),
    a factory laczy sie w tle i po kazdym polaczeniu on_open subskrybuje topiki (raz na polaczenie)."""
    global ros_client
    import roslibpy

    ros = make_ros()
    ros_client = ros
    alive_state = SensorsAlive()
    dropped = DropLog("[rosbridge]")

    def push(event):
        loop.call_soon_threadsafe(robot.apply, event)

    def notify():
        with suppress(RuntimeError):  # petla juz zamknieta (shutdown)
            loop.call_soon_threadsafe(robot.check_status)

    def handler(topic: str):
        def cb(msg: Dict[str, Any]):
            now = time.time()
            try:
                if topic == "/imu/data":
                    push(ImuEvent(robot_id=robot.snapshot.robot_id, ts=now, data=imu_from_ros(msg)))
                elif topic == "/joint_states":
                    push(JointsEvent(robot_id=robot.snapshot.robot_id, ts=now, data=joints_from_ros(msg)))
                elif topic == "/leg_odom2":
                    push(OdometryEvent(robot_id=robot.snapshot.robot_id, ts=now, data=odom_from_ros(msg)))
                elif topic == "/us_publisher/ultrasound_distance":
                    push(UltrasoundEvent(robot_id=robot.snapshot.robot_id, ts=now, data=float(msg["data"])))
                elif topic == "/ptz/status":
                    push(PtzEvent(robot_id=robot.snapshot.robot_id, ts=now, data=ptz_from_ros(msg)))
                elif topic in SENSOR_ALIVE_KEYS:
                    setattr(alive_state, SENSOR_ALIVE_KEYS[topic], bool(msg["data"]))
                    push(SensorsAliveEvent(robot_id=robot.snapshot.robot_id, ts=now,
                                           data=alive_state.model_copy()))
            except Exception as ex:  # zla wiadomosc nie moze zabic subskrypcji
                dropped(topic, ex)
        return cb

    topics = []

    def on_open(proto):
        # w reactorze, po kazdym otwarciu WS (factory._proto juz ustawione). Bez reconnect_on_close roslibpy:
        # ten kolejkuje subscribe na kazde 'close', takze nieudanych prob (bez ready), i wysyla je hurtem
        global ros_ready
        ros_ready = True
        if not topics:
            for t, mtype in ROS_SUBSCRIPTIONS.items():
                topic = roslibpy.Topic(ros, t, mtype, reconnect_on_close=False)
                topic.subscribe(handler(t))  # wysyla od razu przez proto
                topics.append(topic)
            log(f"[rosbridge] connected to {ROSBRIDGE_HOST}:{ROSBRIDGE_PORT}, {len(topics)} topics")
        else:
            for topic in topics:  # nowe polaczenie = nowa sesja rosbridge: subscribe dokladnie raz
                proto.send_ros_message(topic._connect_message)
            log(f"[rosbridge] reconnected to {ROSBRIDGE_HOST}:{ROSBRIDGE_PORT}, {len(topics)} topics")
        notify()

    def on_close(*_):
        global ros_ready
        was, ros_ready = ros_ready, False
        if was:
            log("[rosbridge] connection closed, roslibpy will reconnect")
            notify()

    ros.on("ready", on_open)  # factory emituje 'ready' po kazdym otwarciu WS, 'close' po kazdej utracie
    ros.on("close", on_close)
    # sam start reactora w watku daemon (bez signal handlers); NIE ros.run() - ten czeka do 10 s
    # na polaczenie (blokuje petle asyncio) i rzuca RosTimeoutError, gdy rosbridge nie odpowiada
    ros.factory.manager.run()
    log(f"[rosbridge] connecting to {ROSBRIDGE_HOST}:{ROSBRIDGE_PORT} in background")
    return ros


async def mqtt_task(robot: RobotState) -> None:
    global mqtt_connected
    import aiomqtt

    dropped = DropLog("[mqtt]")
    while True:
        try:
            async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as client:
                mqtt_connected = True
                robot.check_status()
                await client.subscribe(MQTT_GPS_TOPIC)
                async for msg in client.messages:
                    try:
                        data = json.loads(msg.payload.decode())
                        robot.apply(GpsEvent(robot_id=robot.snapshot.robot_id, ts=time.time(),
                                             data=gps_from_mqtt(data)))
                    except Exception as ex:
                        dropped(MQTT_GPS_TOPIC, ex)
        except asyncio.CancelledError:
            raise
        except Exception as ex:
            mqtt_connected = False
            robot.check_status()
            log(f"[mqtt] {why(ex)}; reconnect in 3 s")
            await asyncio.sleep(3)


async def status_task(robot: RobotState) -> None:
    """Co STATUS_TICK_S (lub czesciej, gdy heartbeat krotszy): zmiany statusu, ktorych nic nie zglasza
    (offline przez brak wiadomosci ROS), i heartbeat `status` co WS_HEARTBEAT_S do kazdego klienta."""
    next_hb = time.monotonic() + WS_HEARTBEAT_S
    while True:
        hb = WS_HEARTBEAT_S
        await asyncio.sleep(min(STATUS_TICK_S, hb) if hb > 0 else STATUS_TICK_S)
        changed = robot.check_status()
        now = time.monotonic()
        if hb > 0 and now >= next_hb:
            next_hb = max(next_hb + hb, now + hb / 2)  # staly rytm, bez serii po uspieniu procesu
            if not changed:  # zmiana wlasnie wyszla - ta sama ramka sluzy za heartbeat
                robot.send_status()


# --- aplikacja -------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not API_KEY:
        raise RuntimeError("API_KEY is not set; refusing to start a public API without a key")
    loop = asyncio.get_running_loop()
    robot = robots[ROBOT_ID]
    tasks = [asyncio.create_task(status_task(robot))]
    ros = start_rosbridge(loop, robot) if ROSBRIDGE_ENABLED else None
    if MQTT_ENABLED:
        tasks.append(asyncio.create_task(mqtt_task(robot)))
    yield
    for t in tasks:
        t.cancel()
    if ros is not None:
        stop_rosbridge(ros)


def stop_rosbridge(ros) -> None:
    """Bez czekania: ros.terminate() -> close() czeka do 10 s na 'ready', gdy handshake wisi
    (blokuje petle i przekracza 10 s grace dockera). stopTrying kasuje reconnect, close tylko
    przy otwartym WS, reactor zatrzymany zawsze."""
    global ros_ready
    from twisted.internet import reactor

    f = ros.factory
    with suppress(Exception):
        reactor.callFromThread(f.stopTrying)
    if ros_ready:
        with suppress(Exception):
            ros.close(timeout=1)
    ros_ready = False
    with suppress(Exception):
        f.manager.terminate()


def _rst(tr) -> None:
    """Zrywa TCP z RST: sam abort() zamyka fd, ale kernel trzyma niewyslany bufor w sierocie FIN_WAIT_1
    (klient, ktory nie czyta, dostalby po czasie stare ramki). SO_LINGER 0 tuz przed abort: close() wysyla
    RST i zwalnia bufor od razu; zwykle zamkniecia (bez tego) zostaja lagodne."""
    with suppress(Exception):
        tr.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    tr.abort()


class KeepTransport:
    """Czyste ASGI: zapisuje transport uvicorna w scope WS (gateway.transport), zeby dalo sie
    zerwac TCP (_rst) wolnemu klientowi - ASGI nie ma takiego wywolania. send to metoda protokolu
    uvicorna (sansio/websockets/wsproto maja .transport); gdy owiniety - None.
    Po wyjsciu handlera WS: _rst po WS_ABORT_AFTER_S. uvicorn konczy gniazdo transport.close(),
    a ten czeka na oproznienie bufora - klient, ktory nie czyta, trzymalby je bez konca (keepalive 1011,
    close_timer po 1008). Na zamknietym transporcie abort() nic nie robi, a SO_LINGER na zamknietym
    gniezdzie rzuca (wyciszone)."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "websocket":
            return await self.app(scope, receive, send)
        tr = scope["gateway.transport"] = getattr(getattr(send, "__self__", None), "transport", None)
        try:
            await self.app(scope, receive, send)
        finally:
            if tr is not None:
                asyncio.get_running_loop().call_later(WS_ABORT_AFTER_S, _rst, tr)


app = FastAPI(
    title="Cerber Telemetry Gateway",
    version=API_VERSION,
    description=(
        "Publiczne, read-only API telemetrii robota Cerber (DEEP Robotics Lite3). "
        "Dane pochodza z ROS 2 (rosbridge) i MQTT po stronie VPN. Sterowanie robotem "
        "nie jest wystawione. Autoryzacja: naglowek `X-API-Key` (REST) lub query `api_key` (WebSocket)."
    ),
    lifespan=lifespan,
)
app.add_middleware(KeepTransport)


def key_ok(key: str) -> bool:
    # porownanie w stalym czasie; pusty API_KEY nigdy nie przepuszcza
    return bool(API_KEY) and secrets.compare_digest(key.encode(), API_KEY.encode())


def require_key(x_api_key: str = Header(default="", alias="X-API-Key")) -> None:
    if not key_ok(x_api_key):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, detail="Invalid API key")


COMMON_ERRORS = {401: {"model": Error}, 404: {"model": Error}}


@app.get("/health", response_model=Health, tags=["system"])
async def health() -> Health:
    rb = rosbridge_is_connected()
    ok = (rb or not ROSBRIDGE_ENABLED) and (mqtt_connected or not MQTT_ENABLED)
    return Health(status="ok" if ok else "degraded", rosbridge_connected=rb,
                  mqtt_connected=mqtt_connected, ts=time.time())


@app.get("/api/v1/robots", response_model=RobotList, tags=["robots"],
         dependencies=[Depends(require_key)], responses=COMMON_ERRORS)
async def list_robots() -> RobotList:
    return RobotList(robots=[r.summary() for r in robots.values()])


@app.get("/api/v1/robots/{robot_id}", response_model=RobotSummary, tags=["robots"],
         dependencies=[Depends(require_key)], responses=COMMON_ERRORS)
async def get_robot_summary(robot_id: str) -> RobotSummary:
    return get_robot(robot_id).summary()


@app.get("/api/v1/robots/{robot_id}/telemetry/latest", response_model=TelemetrySnapshot,
         tags=["telemetry"], dependencies=[Depends(require_key)], responses=COMMON_ERRORS,
         summary="Ostatni znany stan robota (wszystkie sekcje)")
async def telemetry_latest(robot_id: str) -> TelemetrySnapshot:
    return get_robot(robot_id).current_snapshot()


@app.get("/api/v1/robots/{robot_id}/gps/latest", response_model=GPSFix, tags=["telemetry"],
         dependencies=[Depends(require_key)], responses=COMMON_ERRORS)
async def gps_latest(robot_id: str) -> GPSFix:
    gps = get_robot(robot_id).snapshot.gps
    if gps is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, detail="No GPS fix received yet")
    return gps


@app.get("/api/v1/robots/{robot_id}/streams", response_model=VideoStreamList, tags=["video"],
         dependencies=[Depends(require_key)], responses=COMMON_ERRORS,
         summary="Strumienie wideo (WHEP, tylko z VPN)")
async def streams(robot_id: str) -> VideoStreamList:
    get_robot(robot_id)
    return VideoStreamList(streams=[
        VideoStream(name=n, path=p, whep_url=f"{MEDIAMTX_BASE}/{p}/whep", vpn_only=STREAMS_VPN_ONLY)
        for n, p in VIDEO_STREAMS
    ])


@app.websocket("/api/v1/robots/{robot_id}/telemetry/stream")
async def telemetry_stream(ws: WebSocket, robot_id: str, api_key: str = Query(default="")):
    """Strumien zdarzen TelemetryEventEnvelope. Pierwsza ramka = `snapshot` (z polem `status`),
    potem zdarzenia per zrodlo. Throttling per typ (imu, joints, odometry, ultrasound, ptz,
    sensors_alive, gps; nie snapshot i status): najwyzej 1 ramka na WS_THROTTLE_S (domyslnie 1 s).
    Pierwsze zdarzenie typu idzie od razu, kolejne w oknie zastepuja sie nawzajem, a ostatnie
    wychodzi na koniec okna - ostatnia wartosc dochodzi zawsze, najpozniej po WS_THROTTLE_S.
    Snapshot (pierwsza ramka, /telemetry/latest) jest aktualizowany kazda wiadomoscia.
    Zdarzenie `status` (RobotStatus: online, last_seen, rosbridge_connected, mqtt_connected) bez
    throttlingu: od razu przy zmianie online / rosbridge_connected / mqtt_connected (offline
    wykrywany najpozniej ~1 s po STALE_AFTER_S bez wiadomosci ROS) i jako heartbeat co
    WS_HEARTBEAT_S (domyslnie 5 s) niezaleznie od aktywnosci klienta; klient nie musi odpowiadac.
    online = rosbridge polaczony i ostatnia wiadomosc ROS w ciagu STALE_AFTER_S (GPS sie nie liczy).
    Ramki od klienta (tekst i binarne, max 4096 B - wieksze -> 1009) sa ignorowane. Zly klucz ->
    zamkniecie kodem 4401, nieznany robot_id -> 4404 (po accept, wiec klient dostaje kod).
    Limit polaczen na robota (MAX_WS_CLIENTS) -> 1013. Brak odpowiedzi na ping WS (protokolu,
    serwer pinguje co 20 s, czeka 20 s na pong) -> 1011; przy WS_THROTTLE_S=1 tak zwykle
    jest rozlaczany klient, ktory przestal czytac (po ~40 s). Przepelniona kolejka (100 ramek;
    wyzsze tempo lub WS_THROTTLE_S=0) -> 1008 tylko jesli bufor TCP zdazy sie oproznic w 1 s,
    zwykle zerwanie polaczenia bez ramki close (u klienta 1006)."""
    await ws.accept()
    if not key_ok(api_key):
        await ws.close(code=4401, reason="invalid api key")
        return
    robot = robots.get(robot_id)
    if robot is None:
        await ws.close(code=4404, reason="unknown robot_id")
        return
    if len(robot.clients) >= MAX_WS_CLIENTS:
        await ws.close(code=1013, reason="too many clients")
        return
    client = WsClient(ws)
    first = SnapshotEvent(robot_id=robot_id, ts=time.time(), data=robot.current_snapshot())
    client.offer(TelemetryEventEnvelope(event=first).model_dump_json())  # snapshot przed zdarzeniami
    robot.clients.add(client)
    client.sender = asyncio.create_task(client.send_loop())
    receiver = asyncio.create_task(_receive_loop(ws))
    try:
        await asyncio.wait({client.sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
        if client.slow:
            await _drop_slow(ws, receiver)
    finally:
        # bez await w finally: anulowanie z zewnatrz (shutdown) ma przejsc czysto
        robot.clients.discard(client)
        client.sender.cancel()
        receiver.cancel()


async def _drop_slow(ws: WebSocket, receiver: asyncio.Task) -> None:
    """Rozlacza klienta, ktory nie odbiera. Przy zapchanym TCP send() uvicorna czeka na writable,
    wiec 1008 zwykle nie wyjdzie; transport.close() czekalby na oproznienie bufora w nieskonczonosc
    (zombie trzymajace pamiec). Dlatego po WS_CLOSE_WAIT_S _rst() - zerwanie RST bez ramki close,
    bufory (takze kernela) zwolnione."""
    sent = False
    with suppress(Exception):
        await asyncio.wait_for(ws.close(code=1008, reason="client too slow"), timeout=WS_CLOSE_WAIT_S)
        sent = True
    if sent:  # bufor zszedl: dajemy klientowi chwile na odpowiedz close
        with suppress(Exception):
            await asyncio.wait_for(asyncio.shield(receiver), timeout=WS_CLOSE_WAIT_S)
    if receiver.done():
        return
    transport = ws.scope.get("gateway.transport")
    if transport is not None:
        _rst(transport)
    # bez transportu (np. --log-level trace owija send): slot zajety az klient zniknie
    with suppress(Exception):
        await receiver


async def _receive_loop(ws: WebSocket) -> None:
    """Czyta (i ignoruje) ramki klienta az do rozlaczenia; heartbeat `status` wysyla status_task."""
    while True:
        try:
            msg = await ws.receive()
        except Exception:  # receive po rozlaczeniu
            return
        if msg.get("type") == "websocket.disconnect":
            return


# Opis WS w OpenAPI (OpenAPI 3.1 nie ma WS; dokumentujemy jako schemat + x-websocket)
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    schema = FastAPI.openapi(app)
    schema["components"]["schemas"].update({
        "TelemetryEventEnvelope": TelemetryEventEnvelope.model_json_schema(
            ref_template="#/components/schemas/{model}"
        )
    })
    env = schema["components"]["schemas"]["TelemetryEventEnvelope"]
    for name, sub in env.pop("$defs", {}).items():
        schema["components"]["schemas"].setdefault(name, sub)
    schema["x-websockets"] = {
        "/api/v1/robots/{robot_id}/telemetry/stream": {
            "description": telemetry_stream.__doc__.strip(),
            "query": {"api_key": "string, wymagany"},
            "closeCodes": {
                "4401": "zly klucz (po accept)", "4404": "nieznany robot_id (po accept)",
                "1013": "limit polaczen WS na robota (MAX_WS_CLIENTS) - sprobuj pozniej",
                "1008": "klient nie odbiera ramek (przepelniona kolejka 100) - rozlaczony; kod dochodzi tylko,"
                        " gdy bufor TCP zdazy sie oproznic w 1 s, zwykle zerwanie bez ramki close (1006)",
                "1009": "ramka od klienta wieksza niz 4096 B",
                "1011": "brak odpowiedzi na ping WS (ping co 20 s, 20 s na pong); przy WS_THROTTLE_S=1 zwykle tak"
                        " rozlaczany jest klient, ktory przestal czytac (po ~40 s)",
            },
            "message": {"$ref": "#/components/schemas/TelemetryEventEnvelope"},
            "status": {"event": "status", "data": {"$ref": "#/components/schemas/RobotStatus"},
                       "heartbeat_s": WS_HEARTBEAT_S, "on_change": ["online", "rosbridge_connected", "mqtt_connected"],
                       "throttled": False},
        }
    }
    schema["components"]["securitySchemes"] = {
        "ApiKey": {"type": "apiKey", "in": "header", "name": "X-API-Key"}
    }
    for path, ops in schema["paths"].items():
        if path.startswith("/api/"):
            for op in ops.values():
                op["security"] = [{"ApiKey": []}]
    app.openapi_schema = schema
    return schema


app.openapi = custom_openapi  # type: ignore[method-assign]


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host=os.getenv("HOST", "0.0.0.0"), port=int(os.getenv("PORT", "8000")),
                ws_max_size=WS_MAX_SIZE)
