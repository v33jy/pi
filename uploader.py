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
