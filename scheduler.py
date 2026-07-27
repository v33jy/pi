import yaml
import os
import sys
import json
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

_DIR = os.path.dirname(os.path.abspath(__file__))
IMAGE_LOG = os.path.join(_DIR, "uploaded_images.json")
SENSOR_LOG = os.path.join(_DIR, "uploaded_sensors.json")

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

def _load_tracking(path):
    # {group_name: {filenames}} instead of one flat list — grouped (by
    # camera, or by sensor folder) so the file is actually readable rather
    # than one giant blob.
    if os.path.exists(path):
        try:
            with open(path) as f:
                raw = json.load(f)
            return {group: set(names) for group, names in raw.items()}
        except Exception:
            return {}
    return {}

def _save_tracking(path, tracking):
    with open(path, "w") as f:
        json.dump({group: sorted(names) for group, names in tracking.items()}, f, indent=2)

def sensor_job():
    # Today's file keeps growing, so it's always re-attempted (the resume
    # logic in upload() only sends the newly appended rows). Past days'
    # files are closed and never change again, so once one is confirmed
    # fully uploaded it's tracked and skipped from then on — previously this
    # only ever looked at *today's* file, so any backlog of past days never
    # got uploaded at all, even after weeks of the scheduler running fine.
    # (A big one-off backlog is handled by running backfill.py once, not by
    # this regular job — see that file for why.)
    today = datetime.now().strftime("%Y-%m-%d")
    uploaded = _load_tracking(SENSOR_LOG)
    for data_dir in cfg["sensor"]["data_dirs"]:
        if not os.path.exists(data_dir):
            continue
        dir_name = os.path.basename(data_dir)
        dir_uploaded = uploaded.setdefault(dir_name, set())
        for filename in os.listdir(data_dir):
            if not filename.endswith(".csv"):
                continue
            is_today = filename.startswith(today)
            if not is_today and filename in dir_uploaded:
                continue
            filepath = os.path.join(data_dir, filename)
            ftp_path = f"{station_id}/sensor/{dir_name}/{filename}"
            if upload(filepath, ftp_path) and not is_today:
                dir_uploaded.add(filename)
                _save_tracking(SENSOR_LOG, uploaded)

def image_job():
    uploaded = _load_tracking(IMAGE_LOG)
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
            # Filenames are "YYYY-MM-DD_HH-MM-camN.jpg" — nesting by that
            # month keeps any one folder from accumulating years of files
            # (a flat folder crashed Explorer on the server once already).
            month = filename[:7]
            ftp_path = f"{station_id}/{cam_name}/{month}/{filename}"
            if upload(filepath, ftp_path):
                cam_uploaded.add(filename)
                # Saved after every file (not just once at the end) so a big
                # backlog on one camera can't stall the others forever —
                # progress survives even if this job is slow or interrupted.
                _save_tracking(IMAGE_LOG, uploaded)

if __name__ == "__main__":
    start_retry_thread(upload, cfg["retry_queue"]["retry_interval_seconds"])

    now = datetime.now()
    scheduler = BlockingScheduler()
    scheduler.add_job(sensor_job, "interval", seconds=cfg["sensor"]["interval_seconds"],
                      next_run_time=now + timedelta(seconds=1))
    scheduler.add_job(image_job,  "interval", seconds=cfg["image"]["interval_seconds"],
                      next_run_time=now + timedelta(seconds=11))

    print(f"[Scheduler] {station_id} | FTP {cfg['ftp']['host']}:{cfg['ftp']['port']}")
    print(f"  Sensor : {cfg['sensor']['interval_seconds']}s interval")
    print(f"  Image  : {cfg['image']['interval_seconds']}s interval")

    scheduler.start()
