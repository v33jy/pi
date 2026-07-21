import yaml
import os
import sys
import json
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import requests

_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOADED_LOG = os.path.join(_DIR, "uploaded_images.json")

def _load_config():
    for name in ("config.local.yaml", "config.yaml"):
        path = os.path.join(_DIR, name)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return yaml.safe_load(f)
    raise FileNotFoundError("No config file found in raspberry_pi/")

try:
    cfg = _load_config()
    station_id = cfg["station"]["id"]
except Exception as e:
    print(f"[Config] Failed to load configuration file: {e}")
    raise SystemExit(1)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from uploader      import upload
from queue_manager import start_retry_thread
import queue_manager

def load_uploaded():
    if os.path.exists(UPLOADED_LOG):
        try:
            with open(UPLOADED_LOG) as f:
                raw = json.load(f)
            return {cam: set(names) for cam, names in raw.items()}
        except Exception:
            return {}
    return {}

def save_uploaded(uploaded):
    with open(UPLOADED_LOG, "w") as f:
        json.dump({cam: sorted(names) for cam, names in uploaded.items()}, f, indent=2)

def sensor_job():
    today = datetime.now().strftime("%Y-%m-%d")
    for data_dir in cfg["sensor"]["data_dirs"]:
        if not os.path.exists(data_dir):
            continue
        dir_name = os.path.basename(data_dir)
        for filename in os.listdir(data_dir):
            if not filename.endswith(".csv"):
                continue
            if not filename.startswith(today):
                continue
            filepath = os.path.join(data_dir, filename)
            ftp_path = f"{station_id}/sensor/{dir_name}/{filename}"
            upload(filepath, ftp_path)

def image_job():
    uploaded = load_uploaded()
    for img_dir in cfg["image"]["dirs"]:
        cam_name = os.path.basename(img_dir)
        if not os.path.exists(img_dir):
            continue
        cam_uploaded = uploaded.setdefault(cam_name, set())
        for filename in os.listdir(img_dir):
            if not filename.endswith((".jpg", ".png")):
                continue
            if filename in cam_uploaded:
                continue
            filepath = os.path.join(img_dir, filename)
            month = filename[:7]
            ftp_path = f"{station_id}/{cam_name}/{month}/{filename}"
            if upload(filepath, ftp_path):
                cam_uploaded.add(filename)
                save_uploaded(uploaded)

def heartbeat_job():
    api_url = cfg.get("dashboard", {}).get("api_url")
    if not api_url:
        return
    try:
        requests.post(
            f"{api_url}/stations/{station_id}/heartbeat",
            json={"queue_size": queue_manager.size()},
            timeout=5,
        )
    except Exception as e:
        print(f"[Heartbeat] Failed: {e}")

if __name__ == "__main__":
    start_retry_thread(upload, cfg["retry_queue"]["retry_interval_seconds"])

    now = datetime.now()
    scheduler = BlockingScheduler()
    scheduler.add_job(sensor_job, "interval", seconds=cfg["sensor"]["interval_seconds"],
                      next_run_time=now + timedelta(seconds=1))
    scheduler.add_job(image_job,  "interval", seconds=cfg["image"]["interval_seconds"],
                      next_run_time=now + timedelta(seconds=11))
    if cfg.get("dashboard", {}).get("api_url"):
        scheduler.add_job(heartbeat_job, "interval",
                          seconds=cfg["dashboard"].get("heartbeat_interval_seconds", 300),
                          next_run_time=now + timedelta(seconds=21))

    print(f"[Scheduler] {station_id} | FTP {cfg['ftp']['host']}:{cfg['ftp']['port']}")
    print(f"  Sensor : {cfg['sensor']['interval_seconds']}s interval")
    print(f"  Image  : {cfg['image']['interval_seconds']}s interval")

    scheduler.start()
