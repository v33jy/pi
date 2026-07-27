"""Force one upload pass right now instead of waiting for the next
scheduled cycle — useful right after a local backlog builds up (Pi offline
for a while, etc.) so you don't have to wait out however many cycles it'd
take to drain naturally.

The scheduler service can be left running the whole time. There's only one
upload code path now (scheduler.upload_job), guarded by a file lock, so this
and the regular scheduled cycle can never corrupt each other's tracking
files by running at the same moment — no need to stop/restart anything.

Usage (from raspberry_pi/):
    python3 backfill.py            # one pass, right now
    python3 backfill.py --verify   # also check-before-send on every file's first
                                    # attempt, not just retries — use this if you
                                    # don't trust the local tracking file (e.g. you
                                    # just reset an entry after suspecting it was
                                    # marked uploaded incorrectly)
"""
import sys

from scheduler import upload_job


def main():
    verify = "--verify" in sys.argv
    processed, total, permanently_failed = upload_job(always_check=verify)
    uploaded = processed - permanently_failed
    print(f"Done: {uploaded}/{total} uploaded" + (
        f", {permanently_failed} skipped after repeated failures (check the log above)" if permanently_failed else ""
    ))


if __name__ == "__main__":
    main()
