"""One-time backlog catch-up: uploads every pending sensor CSV and image
over a single shared FTP connection (uploader.upload_batch) instead of the
regular scheduler's one-connection-per-file loop. Per-file connect+login
overhead barely matters for a normal cycle (a file or two), but it adds up
fast for a big backlog (e.g. after being offline a while, or after wiping
old data and needing everything re-sent) — this is only for that situation.

Run manually, with the scheduler stopped first (both would otherwise fight
over the same tracking files):
    sudo systemctl stop barable-scheduler
    python3 backfill.py
    sudo systemctl start barable-scheduler
"""
import os
import subprocess
from datetime import datetime

from scheduler import cfg, station_id, IMAGE_LOG, SENSOR_LOG, _load_tracking, _save_tracking
from uploader import upload_batch

SCHEDULER_SERVICE = "barable-scheduler"


def _scheduler_is_running():
    # backfill.py and scheduler.py both read-modify-write the same tracking
    # JSON files with no locking — running them at once can silently drop
    # one process's progress update. systemctl is the source of truth on the
    # actual Pi; if it's unavailable (e.g. testing off-Pi) fail open rather
    # than blocking a legitimate run.
    try:
        result = subprocess.run(
            ["systemctl", "is-active", SCHEDULER_SERVICE],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout.strip() == "active"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _pending_sensor_items():
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
            ftp_path = f"{station_id}/sensor/{dir_name}/{filename}"
            items.append((filepath, ftp_path, ("sensor", dir_name, filename, is_today)))
    return uploaded, items


def _pending_image_items():
    uploaded = _load_tracking(IMAGE_LOG)
    items = []
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
            items.append((filepath, ftp_path, ("image", cam_name, filename)))
    return uploaded, items


def main():
    if _scheduler_is_running():
        print(f"[Backfill] {SCHEDULER_SERVICE} is currently running — stop it first, both")
        print("processes write the same tracking files and can clobber each other:")
        print(f"    sudo systemctl stop {SCHEDULER_SERVICE}")
        raise SystemExit(1)

    sensor_uploaded, sensor_items = _pending_sensor_items()
    image_uploaded, image_items = _pending_image_items()
    items = sensor_items + image_items
    print(f"{len(sensor_items)} sensor file(s), {len(image_items)} image(s) pending — {len(items)} total")

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

    done = upload_batch(items, on_success=_on_success)
    print(f"Done: {done}/{len(items)} uploaded")


if __name__ == "__main__":
    main()
