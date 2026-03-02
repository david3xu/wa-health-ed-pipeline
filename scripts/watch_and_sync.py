"""
watch_and_sync.py
Watch the notebooks/ directory and auto-sync to Fabric on every save.

Usage:
    python3 scripts/watch_and_sync.py

Press Ctrl+C to stop.
"""

import time
import subprocess
import sys
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

NOTEBOOKS_DIR = Path(__file__).parent.parent / "notebooks"
SYNC_SCRIPT   = Path(__file__).parent / "sync_to_fabric.py"

# Debounce: ignore repeated events within this window (seconds)
DEBOUNCE_SECS = 2.0

class NotebookHandler(FileSystemEventHandler):
    def __init__(self):
        self._last_synced: dict[str, float] = {}

    def on_modified(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix != ".ipynb":
            return
        self._sync(path)

    def on_created(self, event):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if path.suffix != ".ipynb":
            return
        self._sync(path)

    def _sync(self, path: Path):
        now = time.time()
        last = self._last_synced.get(str(path), 0)
        if now - last < DEBOUNCE_SECS:
            return  # Skip duplicate events from a single save

        self._last_synced[str(path)] = now
        name = path.stem
        print(f"\n[{time.strftime('%H:%M:%S')}] Change detected: {path.name}")

        result = subprocess.run(
            [sys.executable, str(SYNC_SCRIPT), name],
            capture_output=False
        )

        if result.returncode == 0:
            print(f"[{time.strftime('%H:%M:%S')}] Synced OK — refresh notebook in Fabric browser")
        else:
            print(f"[{time.strftime('%H:%M:%S')}] Sync failed — check output above")

if __name__ == "__main__":
    print(f"Watching {NOTEBOOKS_DIR} for changes ...")
    print("Save any .ipynb file to auto-sync to Fabric.")
    print("Press Ctrl+C to stop.\n")

    handler = NotebookHandler()
    observer = Observer()
    observer.schedule(handler, str(NOTEBOOKS_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
        print("\nWatcher stopped.")
    observer.join()
