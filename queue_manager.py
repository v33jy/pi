import json
import os
import threading

QUEUE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raspi_queue.json")
_lock = threading.Lock()

def enqueue(filepath, ftp_path):
    with _lock:
        queue = _load()
        queue.append({"filepath": filepath, "ftp_path": ftp_path})
        _save(queue)
        print(f"[Enqueue] {ftp_path} (waiting {len(queue)} items)")

def dequeue_all():
    with _lock:
        queue = _load()
        _save([])
        return queue

def requeue(items):
    with _lock:
        existing = _load()
        _save(existing + items)

def size():
    return len(_load())

def _load():
    if os.path.exists(QUEUE_FILE):
        try:
            with open(QUEUE_FILE) as f:
                return json.load(f)
        except Exception:
            return []
    return []

def _save(queue):
    with open(QUEUE_FILE, "w") as f:
        json.dump(queue, f)

def start_retry_thread(upload_fn, interval_seconds):
    def _retry_loop():
        import time
        while True:
            time.sleep(interval_seconds)
            items = dequeue_all()
            if not items:
                continue
            print(f"[Retry Queue] {len(items)} items reuploading")
            failed = []
            for item in items:
                if not os.path.exists(item["filepath"]):
                    # local file already deleted, remove from queue
                    print(f"[Retry Queue] File not found, skipping → {item['ftp_path']}")
                    continue
                if not upload_fn(item["filepath"], item["ftp_path"], from_queue=True):
                    failed.append(item)
            if failed:
                requeue(failed)
                print(f"[Retry Queue] {len(failed)} item(s) carried over")

    t = threading.Thread(target=_retry_loop, daemon=True)
    t.start()
    print(f"[Retry Thread] {interval_seconds}s interval")
