"""
Generuje kontrakt JSON z kodu:
  contract/openapi.json            - pelna specyfikacja REST (+ x-websockets)
  contract/schemas/<Model>.json    - JSON Schema (draft 2020-12) kazdego modelu publicznego
  contract/examples/*.json         - przykladowe payloady, zwalidowane modelami

Uruchom:  python export_contract.py [katalog_docelowy]
"""

import json
import os
import sys

os.environ.setdefault("ROSBRIDGE_ENABLED", "0")
os.environ.setdefault("MQTT_ENABLED", "0")
os.environ.pop("WS_HEARTBEAT_S", None)  # heartbeat_s w x-websockets ma byc domyslny, nie z biezacej powloki

import contracts as c  # noqa: E402
from main import app  # noqa: E402

OUT = sys.argv[1] if len(sys.argv) > 1 else "contract"
MODELS = [
    c.TelemetrySnapshot, c.TelemetryEventEnvelope, c.GPSFix, c.Imu, c.Joints, c.Odometry,
    c.Ptz, c.SensorsAlive, c.RobotStatus, c.RobotList, c.RobotSummary, c.VideoStreamList, c.Health, c.Error,
]


def dump(path: str, obj) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
        f.write("\n")


def main() -> None:
    dump(f"{OUT}/openapi.json", app.openapi())
    for m in MODELS:
        schema = m.model_json_schema()
        schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
        schema["$id"] = f"https://cerber.konar.pwr.edu.pl/schemas/{m.__name__}.json"
        dump(f"{OUT}/schemas/{m.__name__}.json", schema)

    now = 1761487391.0
    snap = c.TelemetrySnapshot(
        robot_id="cerber-lite3-01", ts=now,
        imu=c.Imu(orientation=c.Quaternion(x=0.0, y=0.0, z=0.0998, w=0.995),
                  roll=0.0, pitch=0.0, yaw=0.2,
                  angular_velocity=c.Vec3(x=0.0, y=0.0, z=0.05),
                  linear_acceleration=c.Vec3(x=0.1, y=0.0, z=9.81)),
        imu_at=now - 0.004,
        joints=c.Joints(names=["LF_Joint", "LF_Joint_1", "LF_Joint_2", "RF_Joint", "RF_Joint_1", "RF_Joint_2",
                               "LB_Joint", "LB_Joint_1", "LB_Joint_2", "RB_Joint", "RB_Joint_1", "RB_Joint_2"],
                        position=[0.0, -0.8, 1.6] * 4),
        joints_at=now - 0.01,
        odometry=c.Odometry(position=c.Vec3(x=12.4, y=-3.1, z=0.33), yaw=0.2,
                            velocity_body=c.Vec3(x=0.5, y=0.0, z=0.0), yaw_rate=0.05),
        odometry_at=now - 0.01,
        ultrasound=1.42, ultrasound_at=now - 0.05,
        ptz=c.Ptz(state=c.PtzState.WAITING), ptz_at=now - 0.5,
        gps=c.GPSFix(time="2026-10-26T14:03:11Z", latitude=51.107885, longitude=17.038538,
                     altitude=118.4, satellites=9, fix_quality=1),
        gps_at=now - 1.0,
        sensors_alive=c.SensorsAlive(imu=True, odom=True, joints=True, realsense=True, lidar=False, ultrasound=True),
        status=c.RobotStatus(online=True, last_seen=now - 0.004, rosbridge_connected=True, mqtt_connected=True),
    )
    dump(f"{OUT}/examples/telemetry_latest.json", snap.model_dump(mode="json"))
    dump(f"{OUT}/examples/ws_snapshot_event.json",
         c.TelemetryEventEnvelope(event=c.SnapshotEvent(robot_id=snap.robot_id, ts=now, data=snap)).model_dump(mode="json"))
    dump(f"{OUT}/examples/ws_imu_event.json",
         c.TelemetryEventEnvelope(event=c.ImuEvent(robot_id=snap.robot_id, ts=now, data=snap.imu)).model_dump(mode="json"))
    dump(f"{OUT}/examples/ws_gps_event.json",
         c.TelemetryEventEnvelope(event=c.GpsEvent(robot_id=snap.robot_id, ts=now, data=snap.gps)).model_dump(mode="json"))
    dump(f"{OUT}/examples/ws_status_event.json",
         c.TelemetryEventEnvelope(event=c.StatusEvent(robot_id=snap.robot_id, ts=now, data=snap.status))
         .model_dump(mode="json"))
    dump(f"{OUT}/examples/robots.json",
         c.RobotList(robots=[c.RobotSummary(robot_id=snap.robot_id, model="DEEP Robotics Lite3 Venture",
                                            online=True, last_seen=now)]).model_dump(mode="json"))
    dump(f"{OUT}/examples/streams.json",
         c.VideoStreamList(streams=[
             c.VideoStream(name="normal_cam", path="video/normal_cam", whep_url="/video/normal_cam/whep"),
             c.VideoStream(name="IR_cam", path="video/IR_cam", whep_url="/video/IR_cam/whep"),
         ]).model_dump(mode="json"))

    # round-trip: przyklady musza przechodzic przez modele (extra=forbid)
    for name, model in [("telemetry_latest", c.TelemetrySnapshot), ("ws_snapshot_event", c.TelemetryEventEnvelope),
                        ("ws_imu_event", c.TelemetryEventEnvelope), ("ws_gps_event", c.TelemetryEventEnvelope),
                        ("ws_status_event", c.TelemetryEventEnvelope),
                        ("robots", c.RobotList), ("streams", c.VideoStreamList)]:
        with open(f"{OUT}/examples/{name}.json", encoding="utf-8") as f:
            model.model_validate_json(f.read())
    print(f"OK: {OUT}/openapi.json, {len(MODELS)} schem, 7 przykladow")


if __name__ == "__main__":
    main()
