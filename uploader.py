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
MAX_RETRY = 3

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

def upload(filepath, ftp_path, from_queue=False):
    ftp_dir = "/".join(ftp_path.split("/")[:-1])
    tag = "[Retry Queue]" if from_queue else "[Upload]"
    local_size = os.path.getsize(filepath)

    for attempt in range(1, MAX_RETRY + 1):
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
                ftp.login(FTP_USER, FTP_PASS)
                ensure_ftp_dirs(ftp, ftp_dir)

                # Resume from whatever the server already has instead of
                # re-sending the whole file — matters both for retrying a
                # connection that dropped mid-transfer (large .ply files) and
                # for the sensor CSVs, which get re-uploaded every cycle as
                # they grow but never rewrite earlier rows, so most of the
                # file is usually already there.
                offset = _remote_size(ftp, ftp_path)
                if offset >= local_size:
                    offset = 0

                with open(filepath, "rb") as f:
                    if offset:
                        f.seek(offset)
                    ftp.storbinary(f"STOR {ftp_path}", f, rest=offset or None)
            print(f"{tag} Complete → {ftp_path}")
            return True
        except ftplib.all_errors as e:
            print(f"{tag} Fail {attempt}/{MAX_RETRY} | {e}")
            if attempt < MAX_RETRY:
                time.sleep(5)

    if not from_queue:
        from queue_manager import enqueue
        enqueue(filepath, ftp_path)
    return False

def upload_batch(items, on_success=None, max_reconnects=200):
    """Uploads many files over ONE shared FTP connection instead of
    reconnecting per file — for backlog situations (hundreds/thousands of
    pending files) where per-file connect+login overhead dominates the
    actual transfer time.

    items: list of (filepath, ftp_path, tag) — tag is opaque, passed back to
    on_success as-is so callers can update their own tracking without having
    to re-derive anything from the ftp_path.
    on_success: called right after each file's STOR completes, so callers
    can persist progress incrementally (a crash partway through the batch
    shouldn't lose track of what already made it).

    If the shared connection drops partway through, reconnects (with
    backoff, up to max_reconnects times) and resumes from the same item
    (already-completed ones aren't retried) rather than restarting the whole
    batch. max_reconnects defaults high — this is meant to be run once,
    unattended, over a flaky LTE link, so it should keep trying through
    ordinary connection drops rather than giving up early and leaving a
    caller that chains "backfill.py && start the scheduler" stuck never
    starting it. A rejected login (wrong FTP credentials) is different —
    that's not going to fix itself on retry, so it gives up immediately
    instead of burning the whole reconnect budget on something no amount of
    retrying can fix.
    """
    ensured_dirs = set()
    idx = 0
    reconnects = 0
    # Indices already attempted at least once in this run — only these need
    # the SIZE round trip to resume mid-file after a dropped connection. The
    # common case (a file's first and only attempt) skips it entirely, which
    # for a multi-thousand-file backlog removes one full extra round trip per
    # file — the dominant cost over high-latency LTE, not the transfer itself.
    attempted = set()

    while idx < len(items) and reconnects <= max_reconnects:
        try:
            with ftplib.FTP() as ftp:
                ftp.connect(FTP_HOST, FTP_PORT, timeout=30)
                try:
                    ftp.login(FTP_USER, FTP_PASS)
                except ftplib.error_perm as e:
                    print(f"[Batch] Login rejected ({e}) — wrong FTP username/password, not a dropped connection. Giving up rather than retrying something retrying can't fix.")
                    return idx
                while idx < len(items):
                    filepath, ftp_path, tag = items[idx]
                    ftp_dir = "/".join(ftp_path.split("/")[:-1])
                    if ftp_dir not in ensured_dirs:
                        ensure_ftp_dirs(ftp, ftp_dir)
                        ensured_dirs.add(ftp_dir)

                    local_size = os.path.getsize(filepath)
                    offset = 0
                    if idx in attempted:
                        offset = _remote_size(ftp, ftp_path)
                        if offset >= local_size:
                            offset = 0
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
            reconnects += 1
            wait = min(60, 15 * reconnects)  # ramps 15s->60s cap — a dead server shouldn't get hammered every few seconds
            print(f"[Batch] Connection dropped at {idx + 1}/{len(items)}, reconnecting ({reconnects}/{max_reconnects}) in {wait}s | {e}")
            time.sleep(wait)

    return idx  # number of items successfully completed
