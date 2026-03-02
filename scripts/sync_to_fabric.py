"""
sync_to_fabric.py
Sync local notebooks to Fabric workspace via REST API.

Strategy: delete existing notebook, then create fresh.
This avoids silent failures from updateDefinition.

Usage:
    python3 scripts/sync_to_fabric.py                  # sync all notebooks
    python3 scripts/sync_to_fabric.py 01               # sync notebook starting with "01"
    python3 scripts/sync_to_fabric.py 01_silver_ed_performance  # sync by name
"""

import sys
import time
import base64
import subprocess
import json
import re
from pathlib import Path
import requests

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
WORKSPACE_ID  = "e53f915a-de32-40a9-9b16-af4486796bbe"
FABRIC_API    = "https://api.fabric.microsoft.com/v1"
NOTEBOOKS_DIR = Path(__file__).parent.parent / "notebooks"

# Notebook name -> Fabric item ID (updated automatically after each sync)
NOTEBOOK_IDS = {
    "00_eda_bronze":  "f9735b0c-eb80-473a-b96f-ff0ceeaf7f2f",
    "01_silver_ed_performance": "bc2b94ce-32a7-41b1-93c5-3d97162dd384",
    "02_silver_dim_hospitals": "c139d854-cca5-4a36-8615-1c4d108c2700",
    "03_gold_ed_trends":  "6b2bd5ef-b2c7-4f4e-a3f0-b2dfb660123a",
    "04_graph_hospital_network": "78aa8bc8-86a3-4682-a16f-7fec4830444d",
}

# ----------------------------------------------------------------
# Auth
# ----------------------------------------------------------------
def get_token() -> str:
    result = subprocess.run(
        ["az", "account", "get-access-token",
         "--resource", "https://api.fabric.microsoft.com",
         "--query", "accessToken", "-o", "tsv"],
        capture_output=True, text=True
    )
    token = result.stdout.strip()
    if not token:
        raise RuntimeError("No token -- run `az login` first")
    return token

def headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

# ----------------------------------------------------------------
# Delete
# ----------------------------------------------------------------
def delete_notebook(nb_id: str, token: str) -> bool:
    """Delete a notebook by ID. Returns True if deleted (or already gone)."""
    url  = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/notebooks/{nb_id}"
    resp = requests.delete(url, headers=headers(token), timeout=30)
    if resp.status_code in (200, 204):
        return True
    if resp.status_code == 404:
        print(f"    (already gone - 404)")
        return True
    print(f"    [WARN] delete returned {resp.status_code}: {resp.text[:200]}")
    return False

# ----------------------------------------------------------------
# Create
# ----------------------------------------------------------------
def create_notebook(name: str, path: Path, token: str) -> str | None:
    """
    Create a new notebook in Fabric.
    Returns the new item ID, or None on failure.
    """
    payload = base64.b64encode(path.read_bytes()).decode("utf-8")
    url     = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/notebooks"

    resp = requests.post(
        url,
        headers=headers(token),
        json={
            "displayName": name,
            "definition": {
                "format": "ipynb",
                "parts": [{
                    "path": "artifact.content.ipynb",
                    "payload": payload,
                    "payloadType": "InlineBase64",
                }]
            },
        },
        timeout=30,
    )

    # 201 = created synchronously; 202 = accepted (long-running op)
    if resp.status_code == 201:
        new_id = resp.json().get("id")
        return new_id

    if resp.status_code == 202:
        # Poll the operation URL until complete
        op_url = resp.headers.get("Location") or resp.headers.get("x-ms-operation-id")
        if op_url:
            return _poll_operation(op_url, name, token)
        # No operation URL - fall through to list lookup
        print(f"    202 with no operation URL, waiting 5s then searching by name...")
        time.sleep(5)
        return _find_by_name(name, token)

    # Fabric name propagation delay after delete - retry with backoff
    if resp.status_code == 400:
        err = resp.json()
        if err.get("errorCode") == "ItemDisplayNameNotAvailableYet":
            return None   # caller will retry

    print(f"    [ERR] create returned {resp.status_code}: {resp.text[:300]}")
    return None

def _poll_operation(op_url: str, name: str, token: str, max_wait: int = 60) -> str | None:
    """Poll a Fabric long-running operation and return the new item ID."""
    # op_url may be a relative path or full URL
    if not op_url.startswith("http"):
        op_url = f"https://api.fabric.microsoft.com{op_url}"

    deadline = time.time() + max_wait
    while time.time() < deadline:
        time.sleep(3)
        r = requests.get(op_url, headers=headers(token), timeout=30)
        if r.status_code != 200:
            break
        body   = r.json()
        status = body.get("status", "").lower()
        if status in ("succeeded", "completed"):
            # Response may include the created item
            item_id = (body.get("createdItemId")
                       or (body.get("result") or {}).get("id"))
            if item_id:
                return item_id
            # Otherwise search by display name
            return _find_by_name(name, token)
        if status in ("failed", "canceled"):
            print(f"    [ERR] operation {status}: {body}")
            return None
        # still running - loop

    # Timed out - try name lookup
    print(f"    Operation polling timed out, searching by name...")
    return _find_by_name(name, token)

def _find_by_name(name: str, token: str) -> str | None:
    """List workspace notebooks and find the one matching display name."""
    url  = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/notebooks"
    resp = requests.get(url, headers=headers(token), timeout=30)
    if resp.status_code != 200:
        print(f"    [ERR] list notebooks: {resp.status_code}: {resp.text[:200]}")
        return None
    items = resp.json().get("value", [])
    for item in items:
        if item.get("displayName") == name:
            return item.get("id")
    print(f"    [ERR] could not find '{name}' in workspace listing")
    return None

# ----------------------------------------------------------------
# Persist updated IDs back to this script file
# ----------------------------------------------------------------
def _persist_ids(updated: dict[str, str]) -> None:
    """Rewrite the NOTEBOOK_IDS dict in this script with updated IDs."""
    script = Path(__file__)
    text   = script.read_text()

    # Build replacement block
    lines = ["NOTEBOOK_IDS = {\n"]
    for nb_name, nb_id in updated.items():
        lines.append(f'    "{nb_name}":{"  " if len(nb_name) < 22 else " "}"{nb_id}",\n')
    lines.append("}\n")
    new_block = "".join(lines)

    # Replace existing block
    new_text = re.sub(
        r"NOTEBOOK_IDS\s*=\s*\{[^}]*\}\n",
        new_block,
        text,
        flags=re.DOTALL,
    )
    script.write_text(new_text)

# ----------------------------------------------------------------
# Main sync
# ----------------------------------------------------------------
def sync_notebook(name: str, token: str) -> str | None:
    """
    Delete then re-create a notebook in Fabric.
    Returns the new item ID on success, or None on failure.
    """
    path = NOTEBOOKS_DIR / f"{name}.ipynb"
    if not path.exists():
        print(f"  [SKIP] {name} -- file not found at {path}")
        return None

    old_id = NOTEBOOK_IDS.get(name)

    # Step 1: delete existing (if we know its ID)
    if old_id:
        print(f"  [DEL]  {name} (id={old_id[:8]}...)")
        delete_notebook(old_id, token)
        print(f"    waiting 20s for Fabric name propagation...")
        time.sleep(20)
    else:
        print(f"  [NEW]  {name} -- no existing ID, creating fresh")

    # Step 2: create fresh (retry up to 4x if name not yet available)
    for attempt in range(1, 5):
        new_id = create_notebook(name, path, token)
        if new_id:
            print(f"  [OK]   {name} -> {new_id}")
            return new_id
        if attempt < 4:
            wait = attempt * 15
            print(f"    name not available yet, retrying in {wait}s (attempt {attempt}/4)...")
            time.sleep(wait)

    print(f"  [ERR]  {name} -- create failed after retries")
    return None

def resolve_notebooks(arg: str | None) -> list[str]:
    """Return list of notebook names matching the arg, or all if None."""
    if arg is None:
        return list(NOTEBOOK_IDS.keys())
    return [name for name in NOTEBOOK_IDS if name.startswith(arg)]

# ----------------------------------------------------------------
# Entry point
# ----------------------------------------------------------------
if __name__ == "__main__":
    arg     = sys.argv[1] if len(sys.argv) > 1 else None
    targets = resolve_notebooks(arg)

    if not targets:
        print(f"No notebooks match '{arg}'. Available:")
        for n in NOTEBOOK_IDS:
            print(f"  {n}")
        sys.exit(1)

    print(f"Syncing {len(targets)} notebook(s) to Fabric (delete + create) ...")
    token = get_token()

    updated_ids = dict(NOTEBOOK_IDS)   # start with current IDs
    ok = 0
    for name in targets:
        new_id = sync_notebook(name, token)
        if new_id:
            updated_ids[name] = new_id
            ok += 1

    # Persist any new IDs back into this script
    if updated_ids != NOTEBOOK_IDS:
        _persist_ids(updated_ids)
        print("\nNOTEBOOK_IDS updated in sync_to_fabric.py")

    print(f"\n{ok}/{len(targets)} synced successfully.")
    sys.exit(0 if ok == len(targets) else 1)
