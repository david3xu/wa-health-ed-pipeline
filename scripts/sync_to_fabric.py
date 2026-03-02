"""
sync_to_fabric.py
Sync local notebooks to Fabric workspace via REST API.

Usage:
    python3 scripts/sync_to_fabric.py                  # sync all notebooks
    python3 scripts/sync_to_fabric.py 01               # sync notebook starting with "01"
    python3 scripts/sync_to_fabric.py 01_silver_ed_performance  # sync by name
"""

import sys
import base64
import subprocess
import json
from pathlib import Path
import requests

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
WORKSPACE_ID = "e53f915a-de32-40a9-9b16-af4486796bbe"
FABRIC_API   = "https://api.fabric.microsoft.com/v1"
NOTEBOOKS_DIR = Path(__file__).parent.parent / "notebooks"

# Notebook name → Fabric item ID
NOTEBOOK_IDS = {
    "00_eda_bronze":             "5e1f3cd8-7311-4670-92a6-20f7a0b440a2",
    "01_silver_ed_performance":  "f68a29ba-0d8a-450a-86cc-abd4e89552f5",
    "02_silver_dim_hospitals":   "07634915-c084-4131-ba34-12f26bd51b40",
    "03_gold_ed_trends":         "3d533a1e-3621-473d-b2d3-f1dceac59d37",
    "04_graph_hospital_network": "00c288c7-860f-43ae-b424-24925239182a",
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
        raise RuntimeError("No token — run `az login` first")
    return token

# ----------------------------------------------------------------
# Sync
# ----------------------------------------------------------------
def sync_notebook(name: str, token: str) -> bool:
    nb_id = NOTEBOOK_IDS.get(name)
    if not nb_id:
        print(f"  [SKIP] {name} — not in NOTEBOOK_IDS")
        return False

    path = NOTEBOOKS_DIR / f"{name}.ipynb"
    if not path.exists():
        print(f"  [SKIP] {name} — file not found at {path}")
        return False

    payload = base64.b64encode(path.read_bytes()).decode("utf-8")
    url = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/notebooks/{nb_id}/updateDefinition"

    resp = requests.post(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        },
        json={
            "definition": {
                "format": "ipynb",
                "parts": [{
                    "path": "artifact.content.ipynb",
                    "payload": payload,
                    "payloadType": "InlineBase64"
                }]
            }
        },
        timeout=30
    )

    if resp.status_code in (200, 202):
        print(f"  [OK]   {name} → Fabric ({resp.status_code})")
        return True
    else:
        print(f"  [ERR]  {name} → {resp.status_code}: {resp.text[:200]}")
        return False

def resolve_notebooks(arg: str | None) -> list[str]:
    """Return list of notebook names matching the arg, or all if None."""
    if arg is None:
        return list(NOTEBOOK_IDS.keys())
    return [name for name in NOTEBOOK_IDS if name.startswith(arg)]

# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
if __name__ == "__main__":
    arg = sys.argv[1] if len(sys.argv) > 1 else None
    targets = resolve_notebooks(arg)

    if not targets:
        print(f"No notebooks match '{arg}'. Available:")
        for n in NOTEBOOK_IDS:
            print(f"  {n}")
        sys.exit(1)

    print(f"Syncing {len(targets)} notebook(s) to Fabric ...")
    token = get_token()
    results = [sync_notebook(name, token) for name in targets]
    ok = sum(results)
    print(f"\n{ok}/{len(targets)} synced successfully.")
    sys.exit(0 if ok == len(targets) else 1)
