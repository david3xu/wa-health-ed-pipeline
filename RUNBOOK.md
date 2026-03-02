# WA Health ED Pipeline — Runbook

End-to-end reproducibility guide. Follow these steps in order to run the pipeline from scratch on a new machine or after a fresh clone.

---

## Prerequisites

| Requirement | Version | Check |
|---|---|---|
| Python | 3.10+ | `python3 --version` |
| Azure CLI | 2.50+ | `az --version` |
| Azure account | Fabric-enabled | `az login` |
| Git | any | `git --version` |
| Internet access | AIHW API | `curl https://myhospitalsapi.aihw.gov.au/api/v1` |

---

## Step 0 — Clone & Install

```bash
git clone https://github.com/david3xu/wa-health-ed-pipeline.git
cd wa-health-ed-pipeline
pip3 install -r requirements.txt
```

---

## Step 1 — Authenticate to Azure

```bash
az login
```

Select the **Azure for Students / Curtin** subscription when prompted.

Verify:
```bash
az account show --query "{name:name, id:id}"
```

Expected output:
```json
{ "name": "Azure for Students", "id": "52758373-b269-439c-8ba0-976397a796cf" }
```

---

## Step 2 — Verify Fabric Resources

The Fabric workspace and lakehouse were created once and are permanent. Confirm they exist:

```bash
TOKEN=$(az account get-access-token --resource https://api.fabric.microsoft.com --query accessToken -o tsv)
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://api.fabric.microsoft.com/v1/workspaces/e53f915a-de32-40a9-9b16-af4486796bbe/items" \
  | python3 -c "import sys,json; [print(f'  [{i[\"type\"]}] {i[\"displayName\"]}') for i in json.load(sys.stdin)['value']]"
```

Expected output:
```
  [Lakehouse] wa_health_ed
  [Notebook]  00_eda_bronze
  [Notebook]  01_silver_ed_performance
  [Notebook]  02_silver_dim_hospitals
  [Notebook]  03_gold_ed_trends
  [Notebook]  04_graph_hospital_network
  [SQLEndpoint] wa_health_ed
```

If items are missing, re-sync notebooks:
```bash
python3 scripts/sync_to_fabric.py
```

---

## Step 3 — Ingest Bronze Data

Fetches from AIHW API and uploads directly to OneLake. Safe to re-run (overwrites existing files).

```bash
python3 scripts/ingest_bronze.py
```

Expected output:
```
=== AIHW Measures ===
MYH0005: 37,185 records  → uploaded
MYH0010: 22,532 records  → uploaded
MYH0011: 22,678 records  → uploaded
MYH0013: 13,947 records  → uploaded

=== WA Hospital Reference Data ===
Found 147 WA hospitals → uploaded

=== Datasets Metadata ===
7,107 datasets → uploaded
```

**What gets uploaded to OneLake:**

| File | Description |
|---|---|
| `bronze/aihw/measures/MYH0005/raw.json` | 4-hour departure rate, all hospitals |
| `bronze/aihw/measures/MYH0010/raw.json` | Treatment commencement rate |
| `bronze/aihw/measures/MYH0011/raw.json` | ED presentation count |
| `bronze/aihw/measures/MYH0013/raw.json` | 90th percentile departure time |
| `bronze/aihw/reporting_units/wa_hospitals.json` | 147 WA hospitals with lat/lon and LHN mapping |
| `bronze/aihw/datasets/datasets.json` | 7,107 dataset records with reporting date ranges |

---

## Step 4 — Run Notebooks in Fabric

Open `app.fabric.microsoft.com` → workspace **wa-health-ed-pipeline**.

Run notebooks **in this order**, clicking **Run All** in each:

| Order | Notebook | Output | Runtime |
|---|---|---|---|
| 1 | `00_eda_bronze` | EDA report (no writes) | ~3 min |
| 2 | `01_silver_ed_performance` | `silver.fact_ed_performance` Delta table | ~5 min |
| 3 | `02_silver_dim_hospitals` | `silver.dim_hospitals` Delta table | ~2 min |
| 4 | `03_gold_ed_trends` | `gold.ed_waittime_trends`, `gold.hospital_network_edges` | ~3 min |
| 5 | `04_graph_hospital_network` | `gold.hospital_network_nodes` | ~2 min |

**All notebooks use absolute `abfss://` paths — no lakehouse attachment required.**

---

## Step 5 — Create Synapse SQL Views

In Fabric → open **wa_health_ed** lakehouse → click **SQL analytics endpoint** tab → open a new query.

Run the contents of `sql/views.sql`. This creates:

| View | Description |
|---|---|
| `vw_underperforming_hospitals` | Hospitals below 67% 4-hour target |
| `vw_wa_performance_summary` | WA-wide summary for latest period |
| `vw_health_service_ranking` | Health services ranked by average rate |

Verify:
```sql
SELECT TOP 5 * FROM vw_underperforming_hospitals;
SELECT * FROM vw_wa_performance_summary;
```

---

## Step 6 — Run Tests

```bash
pytest tests/ -v
```

Tests validate the silver layer data quality. They require a local Spark session (configured in `tests/conftest.py`).

---

## Development Workflow

### Auto-sync notebooks to Fabric on save

```bash
# Start watcher (keep running in a terminal)
python3 scripts/watch_and_sync.py
```

Save any `.ipynb` file in VS Code → automatically pushed to Fabric within seconds.

### Manual sync

```bash
python3 scripts/sync_to_fabric.py          # all notebooks
python3 scripts/sync_to_fabric.py 01       # notebook 01 only
```

### Re-ingest fresh data

```bash
python3 scripts/ingest_bronze.py
```

Then re-run affected notebooks in Fabric.

---

## Key IDs (Fabric Resources)

| Resource | ID |
|---|---|
| Subscription | `52758373-b269-439c-8ba0-976397a796cf` |
| Workspace | `e53f915a-de32-40a9-9b16-af4486796bbe` |
| Lakehouse | `6383e12e-91b9-4df2-99c5-06c9597bc27e` |
| Capacity (AU SE) | `09cd640d-f351-4b45-a4a2-03e72161b322` |

---

## Data Sources

| Source | URL | Auth |
|---|---|---|
| AIHW MyHospitals API | `https://myhospitalsapi.aihw.gov.au/api/v1` | None (public) |
| AIHW API Docs | `https://myhospitalsapi.aihw.gov.au/swagger/index.html` | None |

---

## Troubleshooting

**`az login` token expired during notebook run**
```bash
az account get-access-token --resource https://api.fabric.microsoft.com
# If expired, run: az login
```

**Notebook can't find bronze files**
- Re-run `python3 scripts/ingest_bronze.py`
- Confirm files appear in OneLake: Fabric UI → wa_health_ed lakehouse → Files → bronze

**`Operation failed: Bad Request` on `spark.read.json(path)`**
- The path is wrong or the lakehouse is not accessible
- All notebooks use full `abfss://` paths — this should not require lakehouse attachment
- Check `WORKSPACE_ID` and `LAKEHOUSE_ID` constants in the notebook match the values in this runbook

**Sync script fails with `No token`**
```bash
az login
python3 scripts/sync_to_fabric.py
```
