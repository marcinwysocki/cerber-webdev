"""
Publiczny kontrakt API gatewaya Cerber (Lite3).

To jest warstwa "biznesowa": plaskie modele w jednostkach SI, z czasem epoch,
niezalezne od struktur ROS. Modele ROS 1:1 (do subskrypcji rosbridge) sa w
cerber_contracts.py. Z tego pliku generowane sa openapi.json i schemas/*.json.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

API_VERSION = "1.0.0"


class Strict(BaseModel):
    """Baza: nieznane pola sa odrzucane, zeby kontrakt byl jednoznaczny. NaN/Infinity
    (json.loads w roslibpy i MQTT je przyjmuje) sa odrzucane - JSON ich nie ma, wyszlyby jako null."""

    model_config = ConfigDict(extra="forbid", allow_inf_nan=False)


# --- skladowe telemetrii ----------------------------------------------------


class Vec3(Strict):
    x: float
    y: float
    z: float


class Quaternion(Strict):
    x: float
    y: float
    z: float
    w: float


class Imu(Strict):
    """Z /imu/data (sensor_msgs/Imu)."""

    orientation: Quaternion = Field(description="Orientacja z IMU (kwaternion z RPY motion hosta), frame_id=base_link")
    roll: float = Field(description="Przechyl [rad], wyliczony z orientation")
    pitch: float = Field(description="Pochylenie [rad]")
    yaw: float = Field(description="Odchylenie [rad]")
    angular_velocity: Vec3 = Field(description="Predkosc katowa; rad/s wg konwencji sensor_msgs/Imu - stockowy node przekazuje surowe wartosci z UDP bez konwersji, jednostka do potwierdzenia na robocie")
    linear_acceleration: Vec3 = Field(description="Przyspieszenie liniowe; m/s^2 wg konwencji sensor_msgs/Imu, surowe z UDP, do potwierdzenia")


class Joints(Strict):
    """Z /joint_states (sensor_msgs/JointState). 12 stawow w stalej kolejnosci."""

    names: List[str] = Field(
        min_length=12, max_length=12,
        description="Stala kolejnosc ze stocku Lite3: LF_Joint, LF_Joint_1, LF_Joint_2, RF_Joint, RF_Joint_1, RF_Joint_2, LB_Joint, LB_Joint_1, LB_Joint_2, RB_Joint, RB_Joint_1, RB_Joint_2",
    )
    position: List[float] = Field(min_length=12, max_length=12, description="Kat stawu; rad wg konwencji sensor_msgs/JointState (stock nie konwertuje, znak odwrocony wzgledem UDP)")


class Odometry(Strict):
    """Z /leg_odom2 (nav_msgs/Odometry) - odometria z nog, uklad 'odom'."""

    position: Vec3 = Field(description="Pozycja w ukladzie odom [m]")
    yaw: float = Field(description="Kurs [rad]")
    velocity_body: Vec3 = Field(description="Predkosc w ukladzie ciala: x do przodu, y w lewo (vel_body ze stocku, m/s wg konwencji); z = 0")
    yaw_rate: float = Field(description="Predkosc obrotu (rpy_vel[2] ze stocku, rad/s wg konwencji)")


class PtzState(str, Enum):
    MOVING = "MOVING"
    WAITING = "WAITING"
    DISCONNECTED = "DISCONNECTED"
    UNKNOWN = "UNKNOWN"


class Ptz(Strict):
    """Z /ptz/status (std_msgs/String), zdekodowany. IP glowicy (adres w sieci
    robota) jest pomijane - nie wystawiamy go publicznie."""

    state: PtzState


class GPSFix(Strict):
    """Z MQTT cerber/gps/data."""

    time: Optional[datetime] = Field(default=None, description="Czas fixu z odbiornika, UTC")
    latitude: float = Field(description="Szerokosc [deg]")
    longitude: float = Field(description="Dlugosc [deg]")
    altitude: float = Field(description="Wysokosc [m n.p.m.]")
    satellites: int = Field(ge=0, le=255)
    fix_quality: int = Field(ge=0, description="NMEA GGA: 0 brak fixu, 1 GPS, 2 DGPS, 4 RTK fix, 5 RTK float")


class SensorsAlive(Strict):
    """Z /sensor_status/*_isalive (std_msgs/Bool)."""

    imu: Optional[bool] = None
    odom: Optional[bool] = None
    joints: Optional[bool] = None
    realsense: Optional[bool] = None
    lidar: Optional[bool] = None
    ultrasound: Optional[bool] = None


# --- snapshot i zdarzenia --------------------------------------------------


class RobotStatus(Strict):
    """Stan robota i polaczen gatewaya. Na WS jako zdarzenie `status`: od razu przy zmianie
    online / rosbridge_connected / mqtt_connected i jako heartbeat co WS_HEARTBEAT_S (domyslnie 5 s)."""

    online: bool = Field(description="rosbridge polaczony i ostatnia wiadomosc ROS w ciagu STALE_AFTER_S (domyslnie 5 s); GPS/MQTT sie nie liczy")
    last_seen: Optional[float] = Field(description="epoch [s] ostatniej wiadomosci ROS; null, dopoki zadna nie przyszla")
    rosbridge_connected: bool = Field(description="WebSocket do rosbridge otwarty")
    mqtt_connected: bool = Field(description="Polaczenie z brokerem MQTT (GPS)")


def _status_unknown() -> RobotStatus:
    return RobotStatus(online=False, last_seen=None, rosbridge_connected=False, mqtt_connected=False)


class TelemetrySnapshot(Strict):
    """Ostatni znany stan robota. Kazda sekcja jest null, dopoki nie przyjdzie
    pierwsza wiadomosc z odpowiedniego zrodla; `*_at` mowi, kiedy ja odebrano."""

    robot_id: str = Field(examples=["cerber-lite3-01"])
    ts: float = Field(description="Czas zlozenia snapshotu, epoch [s] wg zegara gatewaya")
    imu: Optional[Imu] = None
    imu_at: Optional[float] = Field(default=None, description="epoch [s] odbioru ostatniego /imu/data")
    joints: Optional[Joints] = None
    joints_at: Optional[float] = None
    odometry: Optional[Odometry] = None
    odometry_at: Optional[float] = None
    ultrasound: Optional[float] = Field(default=None, description="Element [1] tablicy ultrasound[2] z RobotState (stock publikuje tylko ten); ktory czujnik i jednostka - do potwierdzenia na robocie")
    ultrasound_at: Optional[float] = None
    ptz: Optional[Ptz] = None
    ptz_at: Optional[float] = None
    gps: Optional[GPSFix] = None
    gps_at: Optional[float] = None
    sensors_alive: SensorsAlive = Field(default_factory=SensorsAlive)
    battery_percent: Optional[float] = Field(
        default=None, ge=0, le=100,
        description="Zarezerwowane: stockowy node Lite3 nie publikuje baterii; null dopoki ROS nie doda publishera",
    )
    status: RobotStatus = Field(default_factory=_status_unknown,
                                description="Stan w chwili wyslania snapshotu (/telemetry/latest, pierwsza ramka WS)")


class ImuEvent(Strict):
    type: Literal["imu"] = "imu"
    robot_id: str
    ts: float = Field(description="epoch [s] odbioru przez gateway")
    data: Imu


class JointsEvent(Strict):
    type: Literal["joints"] = "joints"
    robot_id: str
    ts: float
    data: Joints


class OdometryEvent(Strict):
    type: Literal["odometry"] = "odometry"
    robot_id: str
    ts: float
    data: Odometry


class UltrasoundEvent(Strict):
    type: Literal["ultrasound"] = "ultrasound"
    robot_id: str
    ts: float
    data: float = Field(description="Element [1] tablicy ultrasound[2]; jednostka do potwierdzenia")


class PtzEvent(Strict):
    type: Literal["ptz"] = "ptz"
    robot_id: str
    ts: float
    data: Ptz


class GpsEvent(Strict):
    type: Literal["gps"] = "gps"
    robot_id: str
    ts: float
    data: GPSFix


class SensorsAliveEvent(Strict):
    type: Literal["sensors_alive"] = "sensors_alive"
    robot_id: str
    ts: float
    data: SensorsAlive


class StatusEvent(Strict):
    """Zmiana stanu (online, rosbridge_connected, mqtt_connected) albo heartbeat co WS_HEARTBEAT_S.
    Bez throttlingu; klient nie musi odpowiadac."""

    type: Literal["status"] = "status"
    robot_id: str
    ts: float = Field(description="epoch [s] wyslania")
    data: RobotStatus


class SnapshotEvent(Strict):
    """Pierwsza ramka po podlaczeniu do WebSocketu: pelny stan."""

    type: Literal["snapshot"] = "snapshot"
    robot_id: str
    ts: float
    data: TelemetrySnapshot


TelemetryEvent = Union[
    SnapshotEvent, ImuEvent, JointsEvent, OdometryEvent,
    UltrasoundEvent, PtzEvent, GpsEvent, SensorsAliveEvent, StatusEvent,
]


class TelemetryEventEnvelope(BaseModel):
    """Ramka WebSocket /api/v1/robots/{robot_id}/telemetry/stream (tekst, JSON).
    Rozroznianie po polu `type`."""

    model_config = ConfigDict(extra="forbid")
    event: TelemetryEvent = Field(discriminator="type")


# --- odpowiedzi REST ---------------------------------------------------------


class RobotSummary(Strict):
    robot_id: str
    model: str = Field(examples=["DEEP Robotics Lite3 Venture"])
    online: bool = Field(description="True, gdy rosbridge polaczony i ostatnia wiadomosc ROS przyszla w ciagu STALE_AFTER_S; GPS/MQTT sie nie liczy")
    last_seen: Optional[float] = Field(default=None, description="epoch [s] ostatniej wiadomosci ROS")


class RobotList(Strict):
    robots: List[RobotSummary]


class VideoStream(Strict):
    name: str = Field(examples=["normal_cam", "IR_cam"])
    path: str = Field(description="Sciezka MediaMTX", examples=["video/normal_cam"])
    whep_url: str = Field(description="POST SDP offer tutaj (Content-Type: application/sdp). Bezwzgledny, gdy ustawiono MEDIAMTX_BASE; inaczej sciezka wzgledna na tym samym hoscie co API")
    codec: str = Field(default="H264")
    vpn_only: bool = Field(default=True, description="True, gdy proxy/firewall ogranicza /video do VPN (STREAMS_VPN_ONLY); flaga z konfiguracji, nie stan sieci")


class VideoStreamList(Strict):
    streams: List[VideoStream]


class Health(Strict):
    status: Literal["ok", "degraded"]
    version: str = API_VERSION
    rosbridge_connected: bool
    mqtt_connected: bool
    ts: float


class Error(Strict):
    detail: str
