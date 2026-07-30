import os
import time

import requests
import yaml

_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_config():
    for name in ("config.local.yaml", "config.yaml"):
        path = os.path.join(_DIR, name)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return yaml.safe_load(f)
    raise FileNotFoundError("No config file found in raspberry_pi/")


cfg = _load_config()

BASE_URL = cfg["http"]["base_url"].rstrip("/")
API_KEY = cfg["http"]["shared_key"]

# Same threshold/backoff shape as uploader.py's FTP path, for the same
# reason: one bad file shouldn't be allowed to eat the whole run's retry
# budget and block everything queued behind it.
ITEM_FAILURE_THRESHOLD = 3


def _remote_size(remote_path):
    """Bytes already on the server for remote_path, or 0 if it doesn't exist
    (or the check itself fails — treated the same as "not there yet")."""
    try:
        resp = requests.head(f"{BASE_URL}/{remote_path}", headers={"X-Api-Key": API_KEY}, timeout=15)
        if resp.status_code == 200:
            return int(resp.headers.get("X-File-Size", 0))
    except requests.RequestException:
        pass
    return 0


def upload_batch(items, on_success=None, max_reconnects=20, always_check=False):
    """HTTP equivalent of uploader.py's upload_batch — same signature and
    same return value, so scheduler.py can pick either transport without
    changing anything else about how items are built or tracked.

    There's no persistent connection to drop/reconnect the way FTP has, so
    max_reconnects here just counts consecutive failed requests (server
    unreachable, 5xx, timeout, ...) and backs off between retries the same
    way. A single item that keeps failing on its own is skipped after
    ITEM_FAILURE_THRESHOLD attempts, same as the FTP path — it'll simply
    show up as pending again next cycle.

    Unlike FTP, a failed/retried upload always resends the whole file (no
    byte-range resume) — sensor CSVs and single images are small enough that
    this isn't worth the extra complexity.

    always_check: skip re-uploading a file the server already has in full
    (checked via HEAD), same purpose as the FTP version — real LTE data, not
    just wasted time, when re-running after resetting a tracking file you no
    longer trust.
    """
    idx = 0
    reconnects = 0
    permanently_failed = 0
    item_failures = {}

    while idx < len(items) and reconnects <= max_reconnects:
        filepath, remote_path, tag = items[idx]
        try:
            if always_check:
                local_size = os.path.getsize(filepath)
                if _remote_size(remote_path) >= local_size:
                    print(f"[HTTP] Already complete on server, skipping → {remote_path}")
                    if on_success:
                        on_success(tag)
                    idx += 1
                    continue

            with open(filepath, "rb") as f:
                resp = requests.put(
                    f"{BASE_URL}/{remote_path}", data=f,
                    headers={"X-Api-Key": API_KEY}, timeout=60,
                )

            if resp.status_code in (200, 204):
                print(f"[HTTP] Complete → {remote_path}")
                if on_success:
                    on_success(tag)
                idx += 1
            elif resp.status_code == 401:
                print("[HTTP] Rejected (401) — wrong API key, not a dropped connection. Giving up rather than retrying something retrying can't fix.")
                return idx, permanently_failed
            else:
                raise requests.RequestException(f"HTTP {resp.status_code}: {resp.text[:200]}")
        except requests.RequestException as e:
            item_failures[idx] = item_failures.get(idx, 0) + 1
            if item_failures[idx] >= ITEM_FAILURE_THRESHOLD:
                print(f"[HTTP] {remote_path} failed {item_failures[idx]} times on its own — skipping it, not the connection's fault | {e}")
                idx += 1
                permanently_failed += 1
                continue
            reconnects += 1
            wait = min(60, 15 * reconnects)
            print(f"[HTTP] Request failed at {idx + 1}/{len(items)}, retrying ({reconnects}/{max_reconnects}) in {wait}s | {e}")
            time.sleep(wait)

    return idx, permanently_failed
