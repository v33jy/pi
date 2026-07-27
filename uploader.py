import ftplib
import time
import yaml
import os

_DIR = os.path.dirname(os.path.abspath(__file__))

def _load_config():
    for name in ("config.local.yaml", "config.yaml"):
        path = os.path.join(_DIR, name)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as f:
                return yaml.safe_load(f)
    raise FileNotFoundError("No config file found in raspberry_pi/")

cfg = _load_config()

FTP_HOST = cfg["ftp"]["host"]
FTP_PORT = cfg["ftp"]["port"]
FTP_USER = cfg["ftp"]["user"]
FTP_PASS = cfg["ftp"]["password"]

# A single file failing this many times in a row (not the connection itself
# dropping — see ITEM_FAILURE_THRESHOLD's use below) gets skipped rather than
# left to eat the whole batch's reconnect budget and block every file behind
# it. It'll simply show up as pending again next cycle.
ITEM_FAILURE_THRESHOLD = 3

def ensure_ftp_dirs(ftp, dir_path):
    parts = dir_path.split("/")
    for i in range(1, len(parts) + 1):
        partial = "/".join(parts[:i])
        if not partial:
            continue
        try:
            ftp.mkd(partial)
        except ftplib.error_perm:
            pass

def _remote_size(ftp, ftp_path):
    """Bytes already on the server for ftp_path, or 0 if it doesn't exist yet.
    SIZE is only reliable in binary mode (RFC 3659), so TYPE I is set first."""
    try:
        ftp.voidcmd("TYPE I")
        return ftp.size(ftp_path)
    except ftplib.all_errors:
        return 0

def upload_batch(items, on_success=None, max_reconnects=20, always_check=False):
    """Uploads many files over ONE shared FTP connection instead of
    reconnecting per file — this is the only upload path in the project now;
    both the regular scheduled cycle and a manual on-demand catch-up
    (backfill.py) call this same function.

    items: list of (filepath, ftp_path, tag) — tag is opaque, passed back to
    on_success as-is so callers can update their own tracking without having
    to re-derive anything from the ftp_path.
    on_success: called right after each file's STOR completes (or is found
    already complete on the server), so callers can persist progress
    incrementally — a crash partway through the batch shouldn't lose track
    of what already made it.

    If the shared connection drops partway through, reconnects (with
    backoff, up to max_reconnects times) and resumes from the same item
    (already-completed ones aren't retried) rather than restarting the whole
    batch. max_reconnects defaults high — this is meant to be run
    unattended over a flaky LTE link, so it should keep trying through
    ordinary connection drops rather than giving up early. A rejected login
    (wrong FTP credentials) is different — that's not going to fix itself on
    retry, so it gives up immediately instead of burning the whole
    reconnect budget on something no amount of retrying can fix.

    A single item that keeps failing on its own (not the shared connection
    dropping — e.g. a corrupted local file the server rejects every time)
    is skipped after ITEM_FAILURE_THRESHOLD attempts instead of being
    allowed to consume the entire reconnect budget and block every file
    queued behind it in this run.

    always_check: normally the SIZE round trip is skipped on a file's first
    attempt (it's almost always genuinely new, so the check is pure
    overhead) and only done on a retry after a dropped connection. Pass
    True when `items` might include files the server actually already has
    in full — e.g. re-running after resetting a local tracking file you no
    longer trust — so a file that's already fully uploaded gets *skipped*
    instead of blindly re-sent, which for images/point clouds is real LTE
    data, not just wasted time.

    Returns (processed, permanently_failed): `processed` is how many items
    reached a terminal state (uploaded, found already on the server, or
    given up on individually) — it reaching len(items) means the run went
    through the whole list, as opposed to stopping early because
    max_reconnects was exhausted. `permanently_failed` is how many of those
    were the latter (skipped after repeated failures of their own) — worth
    reporting separately so "processed everything" doesn't read as "all
    genuinely uploaded" when some weren't.
    """
    ensured_dirs = set()
    idx = 0
    reconnects = 0
    permanently_failed = 0
    # Indices already attempted at least once in this run — only these need
    # the SIZE round trip to resume mid-file after a dropped connection. The
    # common case (a file's first and only attempt) skips it entirely, which
    # for a multi-thousand-file backlog removes one full extra round trip per
    # file — the dominant cost over high-latency LTE, not the transfer itself.
    attempted = set()
    item_failures = {}

    while idx < len(items) and reconnects <= max_reconnects:
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
                try:
                    ftp.login(FTP_USER, FTP_PASS)
                except ftplib.error_perm as e:
                    print(f"[Batch] Login rejected ({e}) — wrong FTP username/password, not a dropped connection. Giving up rather than retrying something retrying can't fix.")
                    return idx, permanently_failed
                while idx < len(items):
                    filepath, ftp_path, tag = items[idx]
                    ftp_dir = "/".join(ftp_path.split("/")[:-1])
                    if ftp_dir not in ensured_dirs:
                        ensure_ftp_dirs(ftp, ftp_dir)
                        ensured_dirs.add(ftp_dir)

                    local_size = os.path.getsize(filepath)
                    offset = 0
                    try:
                        if always_check or idx in attempted:
                            offset = _remote_size(ftp, ftp_path)
                            if offset >= local_size:
                                print(f"[Batch] Already complete on server, skipping → {ftp_path}")
                                if on_success:
                                    on_success(tag)
                                idx += 1
                                continue
                        attempted.add(idx)

                        with open(filepath, "rb") as f:
                            if offset:
                                f.seek(offset)
                            ftp.storbinary(f"STOR {ftp_path}", f, rest=offset or None)
                        print(f"[Batch] Complete → {ftp_path}")
                        if on_success:
                            on_success(tag)
                        idx += 1
                    except ftplib.all_errors as e:
                        item_failures[idx] = item_failures.get(idx, 0) + 1
                        if item_failures[idx] >= ITEM_FAILURE_THRESHOLD:
                            print(f"[Batch] {ftp_path} failed {item_failures[idx]} times on its own — skipping it, not the connection's fault | {e}")
                            idx += 1
                            permanently_failed += 1
                            continue
                        raise
        except ftplib.all_errors as e:
            reconnects += 1
            wait = min(60, 15 * reconnects)  # ramps 15s->60s cap — a dead server shouldn't get hammered every few seconds
            print(f"[Batch] Connection dropped at {idx + 1}/{len(items)}, reconnecting ({reconnects}/{max_reconnects}) in {wait}s | {e}")
            time.sleep(wait)

    return idx, permanently_failed
