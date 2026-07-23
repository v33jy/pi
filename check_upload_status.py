"""Read-only report of what this Pi has uploaded vs. what's still pending —
no uploads happen here, it just reuses backfill.py's own pending-file scan
(same logic the actual upload path uses, so the report can't drift out of
sync with reality) plus the retry queue.

Run from raspberry_pi/:
    python3 check_upload_status.py
"""
from backfill import _pending_sensor_items, _pending_image_items
import queue_manager


def _report(label, uploaded, pending_items):
    pending_by_group = {}
    for filepath, ftp_path, tag in pending_items:
        group = tag[1]  # dir_name or cam_name — see backfill.py's tag shape
        pending_by_group.setdefault(group, []).append(tag[2])  # filename

    groups = sorted(set(uploaded) | set(pending_by_group))
    print(f"--- {label} ---")
    if not groups:
        print("  (no directories found)")
        return

    for group in groups:
        done = len(uploaded.get(group, set()))
        pending = sorted(pending_by_group.get(group, []))
        print(f"  {group}: {done} uploaded, {len(pending)} pending")
        for name in pending[:10]:
            print(f"      pending: {name}")
        if len(pending) > 10:
            print(f"      ... +{len(pending) - 10} more")


def main():
    sensor_uploaded, sensor_pending = _pending_sensor_items()
    image_uploaded, image_pending = _pending_image_items()

    _report("Sensors", sensor_uploaded, sensor_pending)
    print()
    _report("Images", image_uploaded, image_pending)

    print()
    q_size = queue_manager.size()
    print(f"--- Retry queue ---\n  {q_size} item(s) waiting to retry (failed uploads)")
    if q_size:
        for item in queue_manager._load()[:10]:
            print(f"      {item['ftp_path']}")

    total_pending = len(sensor_pending) + len(image_pending)
    print()
    if total_pending == 0 and q_size == 0:
        print("OK — nothing pending, nothing stuck in the retry queue.")
    else:
        print(f"{total_pending} file(s) not yet uploaded, {q_size} stuck in retry queue.")
        print("(Note: today's still-growing sensor CSVs always show as 'pending' here —")
        print(" that's expected, the scheduler re-sends just the new rows each cycle.)")


if __name__ == "__main__":
    main()
