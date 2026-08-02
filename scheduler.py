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

def _pending_sensor_items():
    # Today's file keeps growing, so it's always re-attempted (upload_batch's
    # resume-from-server-size logic only sends the newly appended rows).
    # Past days' files are closed and never change again, so once one is
    # confirmed fully uploaded it's tracked and skipped from then on.
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

def _pending_image_items():
    uploaded = _load_tracking(IMAGE_LOG)
    per_camera = []
    for img_dir in cfg["image"]["dirs"]:
        cam_name = os.path.basename(img_dir)
        if not os.path.exists(img_dir):
            continue
        cam_uploaded = uploaded.setdefault(cam_name, set())
        cam_items = []
        for filename in os.listdir(img_dir):
            if not filename.endswith((".jpg", ".png")):
                continue
            if filename in cam_uploaded:
                continue
            filepath = os.path.join(img_dir, filename)
            # Filenames are "YYYY-MM-DD_HH-MM-camN.jpg" — nesting by that
            # month keeps any one folder from accumulating years of files.
            month = filename[:7]
            remote_path = f"{station_id}/{cam_name}/{month}/{filename}"
            cam_items.append((filepath, remote_path, ("image", cam_name, filename)))
        per_camera.append(cam_items)

    # Round-robin across cameras (one from cam0, one from cam1, ... repeat)
    # instead of draining one camera's entire backlog before starting the
    # next. A huge pile on one camera used to mean every other camera got
    # no turn at all within a run — this is the actual reason a camera could
    # go months without uploading anything even though pending counts looked
    # fine in isolation.
    items = [
        item for group in itertools.zip_longest(*per_camera)
        for item in group if item is not None
    ]
    return uploaded, items

def upload_job(always_check=False):
    """The one and only upload pass — uploads everything not yet confirmed
    on the server (sensor CSVs and images together). The regular scheduled
    cycle calls this every interval, and backfill.py calls it once on
    demand for an immediate catch-up —
    there is no separate "bulk" code path anymore, so there's nothing that
    can disagree with this function about what's already uploaded.

    Guarded by a file lock so that a manual `python3 backfill.py` run and
    the scheduled cycle can never execute this at the same moment and
    interleave writes to the tracking files — one simply waits for the
    other instead of the two silently corrupting each other's progress
    (the actual cause of a past incident where files looked "uploaded" in
    the tracking file but were never actually on the server). This is also
    why the scheduler service no longer needs to be stopped before running
    backfill.py, or restarted after — there's nothing to coordinate by hand.
    """
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
                _, cam_name, filename = tag
                image_uploaded[cam_name].add(filename)
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
