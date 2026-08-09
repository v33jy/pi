import fcntl
import itertools
import yaml
import os
import sys
import json
from datetime import datetime, timedelta
from apscheduler.schedulers.blocking import BlockingScheduler

_DIR = os.path.dirname(os.path.abspath(__file__))
IMAGE_LOG = os.path.join(_DIR, "uploaded_images.json")
SENSOR_LOG = os.path.join(_DIR, "uploaded_sensors.json")
_LOCK_PATH = os.path.join(_DIR, ".upload.lock")

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
from http_uploader import upload_batch

def _load_tracking(path):
    if os.path.exists(path):
        try:
            with open(path) as f:
                raw = json.load(f)
            return {group: set(names) for group, names in raw.items()}
        except Exception as e:
            # Safe to treat as empty: upload_batch's resume-from-server-size
            # check will skip files already complete rather than re-send them.
            print(f"[Tracking] {path} unreadable, treating as empty this run | {e}")
            return {}
    return {}

def _save_tracking(path, tracking):
    # Write-then-rename: os.replace is atomic, so a crash mid-write can't
    # leave a corrupt JSON file behind.
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w") as f:
        json.dump({group: sorted(names) for group, names in tracking.items()}, f, indent=2)
    os.replace(tmp_path, path)

def _pending_sensor_items():
    # Today's file keeps growing, so it's always re-attempted (upload_batch
    # resends only the newly appended rows). Past days are closed and never
    # change, so once tracked as uploaded they're skipped.
    today = datetime.now().strftime("%Y-%m-%d")
    uploaded = _load_tracking(SENSOR_LOG)
    items = []
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
            remote_path = f"{station_id}/sensor/{dir_name}/{filename}"
            items.append((filepath, remote_path, ("sensor", dir_name, filename, is_today)))
    return uploaded, items

def _image_month(root, img_dir, filename):
    # Files can live flat in img_dir ("YYYY-MM-DD_HH-MM-camN.jpg", month from
    # the filename prefix) or nested one level under a YYYYMMDD date folder
    # (month from the folder name instead, since the filename itself may not
    # follow the dashed format).
    subdir = os.path.basename(root) if root != img_dir else None
    if subdir and len(subdir) == 8 and subdir.isdigit():
        return f"{subdir[:4]}-{subdir[4:6]}"
    return filename[:7]

def _pending_image_items():
    uploaded = _load_tracking(IMAGE_LOG)
    per_camera = []
    for img_dir in cfg["image"]["dirs"]:
        cam_name = os.path.basename(img_dir)
        if not os.path.exists(img_dir):
            continue
        cam_uploaded = uploaded.setdefault(cam_name, set())
        cam_items = []
        for root, _dirs, filenames in os.walk(img_dir):
            for filename in filenames:
                if not filename.endswith((".jpg", ".png")):
                    continue
                filepath = os.path.join(root, filename)
                # Relative-path key so files with the same basename under
                # different date folders can't collide with each other or
                # with an unrelated flat file.
                rel_key = os.path.relpath(filepath, img_dir).replace(os.sep, "/")
                if rel_key in cam_uploaded:
                    continue
                month = _image_month(root, img_dir, filename)
                remote_path = f"{station_id}/{cam_name}/{month}/{filename}"
                cam_items.append((filepath, remote_path, ("image", cam_name, rel_key)))
        per_camera.append(cam_items)

    # Round-robin across cameras (one from cam0, one from cam1, ... repeat)
    # so a large backlog on one camera can't starve the others of a turn.
    items = [
        item for group in itertools.zip_longest(*per_camera)
        for item in group if item is not None
    ]
    return uploaded, items

def upload_job(always_check=False):
    """Single upload pass (sensor CSVs + images) used by both the scheduled
    cycle and backfill.py. Guarded by a file lock so the two can never run
    at the same moment and interleave writes to the tracking files."""
    with open(_LOCK_PATH, "w") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)

        sensor_uploaded, sensor_items = _pending_sensor_items()
        image_uploaded, image_items = _pending_image_items()
        items = sensor_items + image_items
        if not items:
            return 0, 0, 0

        def _on_success(tag):
            kind = tag[0]
            if kind == "sensor":
                _, dir_name, filename, is_today = tag
                if not is_today:
                    sensor_uploaded[dir_name].add(filename)
                    _save_tracking(SENSOR_LOG, sensor_uploaded)
            else:
                _, cam_name, rel_key = tag
                image_uploaded[cam_name].add(rel_key)
                _save_tracking(IMAGE_LOG, image_uploaded)

        processed, permanently_failed = upload_batch(items, on_success=_on_success, always_check=always_check)
        uploaded = processed - permanently_failed
        msg = f"[Upload] {uploaded}/{len(items)} uploaded this cycle"
        if permanently_failed:
            msg += f", {permanently_failed} skipped after repeated failures (check log)"
        print(msg)
        return processed, len(items), permanently_failed

if __name__ == "__main__":
    now = datetime.now()
    scheduler = BlockingScheduler()
    scheduler.add_job(upload_job, "interval", seconds=cfg["sensor"]["interval_seconds"],
                      next_run_time=now + timedelta(seconds=1))

    print(f"[Scheduler] {station_id} | HTTP {cfg['http']['base_url']}")
    print(f"  Upload check interval: {cfg['sensor']['interval_seconds']}s")

    scheduler.start()
