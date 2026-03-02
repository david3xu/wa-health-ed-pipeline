"""
ingest_bronze.py
Fetch AIHW ED performance data and WA hospital reference data,
upload directly to OneLake bronze layer.

Sources:
- AIHW MyHospitals API: measures/data-items for ED performance
- AIHW MyHospitals API: reporting-units for WA hospital locations + LHN mapping

Usage:
    python3 scripts/ingest_bronze.py
"""

import json
import io
import sys
import requests
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------
WORKSPACE_ID  = "e53f915a-de32-40a9-9b16-af4486796bbe"
LAKEHOUSE_ID  = "6383e12e-91b9-4df2-99c5-06c9597bc27e"
ONELAKE_URL   = "https://onelake.dfs.fabric.microsoft.com"

AIHW_BASE     = "https://myhospitalsapi.aihw.gov.au/api/v1"
MEASURE_CODES = ["MYH0005", "MYH0010", "MYH0011", "MYH0013"]

# Headers to avoid 403 — some AIHW endpoints block default Python UA
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; wa-health-ed-pipeline/1.0)"
}

# ----------------------------------------------------------------
# Auth — uses existing `az login` session
# ----------------------------------------------------------------
credential = AzureCliCredential()

def get_datalake_client():
    return DataLakeServiceClient(
        account_url=ONELAKE_URL,
        credential=credential
    )

def upload_to_onelake(client: DataLakeServiceClient, path: str, data: bytes):
    """Upload bytes to OneLake at Files/{path}."""
    # OneLake filesystem = workspace_id, path root = lakehouse_id/Files/
    fs = client.get_file_system_client(WORKSPACE_ID)
    full_path = f"{LAKEHOUSE_ID}/Files/{path}"

    # Create directories
    dir_path = "/".join(full_path.split("/")[:-1])
    try:
        fs.create_directory(dir_path)
    except Exception:
        pass  # Directory may already exist

    file_client = fs.get_file_client(full_path)
    file_client.upload_data(data, overwrite=True, length=len(data))
    print(f"  Uploaded {len(data):,} bytes → {full_path}")

# ----------------------------------------------------------------
# Ingest AIHW measures
# ----------------------------------------------------------------
def ingest_aihw_measures(client):
    print("\n=== AIHW Measures ===")
    for code in MEASURE_CODES:
        url = f"{AIHW_BASE}/measures/{code}/data-items"
        print(f"Fetching {code} from {url} ...")
        try:
            resp = requests.get(url, timeout=180, headers=HEADERS)
            resp.raise_for_status()
            data = resp.content
            print(f"  {code}: {len(data):,} bytes, "
                  f"{len(resp.json().get('result', []))} records")
            upload_to_onelake(
                client,
                f"bronze/aihw/measures/{code}/raw.json",
                data
            )
        except Exception as e:
            print(f"  ERROR fetching {code}: {e}", file=sys.stderr)

# ----------------------------------------------------------------
# Ingest WA hospital reference data from AIHW reporting-units
# Includes: lat/lon, LHN (health service), state mapping
# ----------------------------------------------------------------
def ingest_wa_hospitals(client):
    print("\n=== WA Hospital Reference Data (AIHW reporting-units) ===")
    url = f"{AIHW_BASE}/reporting-units"
    print(f"Fetching all reporting units from {url} ...")
    try:
        resp = requests.get(url, timeout=180, headers=HEADERS)
        resp.raise_for_status()
        all_units = resp.json().get("result", [])

        # Filter to WA hospitals only
        wa_hospitals = [
            u for u in all_units
            if u.get("reporting_unit_type", {}).get("reporting_unit_type_code") == "H"
            and any(
                m.get("mapped_reporting_unit", {}).get("reporting_unit_code") == "WA"
                for m in u.get("mapped_reporting_units", [])
            )
        ]

        data = json.dumps({
            "result": wa_hospitals,
            "version_information": resp.json().get("version_information", {})
        }, indent=2).encode("utf-8")

        print(f"  Found {len(wa_hospitals)} WA hospitals out of {len(all_units)} total units")
        upload_to_onelake(
            client,
            "bronze/aihw/reporting_units/wa_hospitals.json",
            data
        )
    except Exception as e:
        print(f"  ERROR fetching reporting units: {e}", file=sys.stderr)

# ----------------------------------------------------------------
# Ingest datasets metadata — time period lookup table
# data_set_id → reporting_start_date, reporting_end_date
# ----------------------------------------------------------------
def ingest_datasets(client):
    print("\n=== Datasets Metadata (time period lookup) ===")
    url = f"{AIHW_BASE}/datasets"
    print(f"Fetching all datasets from {url} ...")
    try:
        resp = requests.get(url, timeout=180, headers=HEADERS)
        resp.raise_for_status()
        datasets = resp.json().get("result", [])

        # Keep only fields needed for time period lookup
        slim = [
            {
                "data_set_id":          d["data_set_id"],
                "reporting_start_date": d["reporting_start_date"],
                "reporting_end_date":   d["reporting_end_date"],
                "measure_code":         d.get("reported_measure_summary", {})
                                         .get("measure_summary", {})
                                         .get("measure_code")
            }
            for d in datasets
        ]

        data = json.dumps({"result": slim}, indent=2).encode("utf-8")
        print(f"  {len(slim)} datasets, {len(data):,} bytes")
        upload_to_onelake(client, "bronze/aihw/datasets/datasets.json", data)
    except Exception as e:
        print(f"  ERROR fetching datasets: {e}", file=sys.stderr)

# ----------------------------------------------------------------
# Main
# ----------------------------------------------------------------
if __name__ == "__main__":
    print("Connecting to OneLake ...")
    client = get_datalake_client()
    print(f"Workspace: {WORKSPACE_ID}")
    print(f"Lakehouse: {LAKEHOUSE_ID}")

    ingest_aihw_measures(client)
    ingest_wa_hospitals(client)
    ingest_datasets(client)

    print("\nBronze ingestion complete.")
    print("Files uploaded to OneLake:")
    for code in MEASURE_CODES:
        print(f"  bronze/aihw/measures/{code}/raw.json")
    print(f"  bronze/aihw/reporting_units/wa_hospitals.json")
    print(f"  bronze/aihw/datasets/datasets.json")
