# Runtime selection — pick the right place to run the generator

The Spark BigQuery generator works on three runtimes; pick the one that fits your target row count and how you want to interact with the form.

## Quick decision matrix

| Runtime | Approx ceiling | Form UI | Cost | When |
|---|---|---|---|---|
| **BQ Studio default** (Colab Enterprise) | ~150–250K rows | Yes (notebook form) | Free with BQ Studio | Quick demos, smoke-checks, the smaller presets |
| **Vertex AI Workbench** `n1-standard-8` | ~1–2M rows | Yes (same notebook form) | ~$0.40/hr running, $0 stopped | Real-volume runs, all 6 presets, exploratory custom configs |
| **Laptop / local Spark** | Unbounded* | No (CLI) | Free | The biggest runs (multi-million rows), CI-driven generation, scripted batches |

\* Laptop ceiling is your local JVM heap. With `SPARK_DRIVER_MEMORY=8g` we've validated up to ~1M rows; beyond that, switch to Dataproc Serverless.

If the form crashes with `ConnectionRefusedError` / `Py4JNetworkError` / `EOFError`, the JVM ran out of heap. Either drop the row count or move to a beefier runtime.

---

## Path A — BQ Studio default (smallest runs)

Open `notebooks/02_generate_acdoca_bq_studio.ipynb` in BigQuery Studio, click *Run all*. Works for `quick_smoke` and `globe_lite` (≤30K rows) reliably; `tp_workshop` and `supply_chain_demo` are borderline; `ml_features` and any custom run above ~200K will JVM-crash.

Nothing to provision — this is the default Colab Enterprise runtime.

---

## Path B — Vertex AI Workbench (recommended for real-volume runs)

A user-managed Vertex AI Workbench notebook gives you the same notebook form on a 30 GB / 8 vCPU VM. It runs every preset reliably and most custom configs up to ~1M rows.

### One-time provisioning

Use the helper script (idempotent — skips if the instance already exists):

```bash
./scripts/provision_vertex_workbench.sh
```

Or run the underlying `gcloud` command directly (Vertex AI Workbench Instances — the unified product that replaced the deprecated user-managed notebooks API):

```bash
PROJECT=acdoca-synthetic-greg
ZONE=us-central1-a
INSTANCE=acdoca-form
gcloud workbench instances create "$INSTANCE" \
  --location="$ZONE" \
  --project="$PROJECT" \
  --machine-type=n1-standard-8 \
  --vm-image-project=cloud-notebooks-managed \
  --vm-image-family=workbench-instances \
  --metadata=idle-timeout-seconds=3600
```

The `idle-timeout-seconds` metadata auto-stops the VM after 60 min of inactivity so you don't bleed money when you forget to turn it off.

### Using it

1. After `gcloud workbench instances create` finishes, the JupyterLab URL appears in the GCP console at *Vertex AI → Workbench → Instances*: <https://console.cloud.google.com/vertex-ai/workbench/instances>.
2. Click *Open JupyterLab*. The instance has the same Python + Spark stack as BQ Studio.
3. Open `notebooks/02_generate_acdoca_bq_studio.ipynb` (clone the repo first via *File → New → Terminal* and `git clone https://github.com/GDAVIDREEVES/bigquery-sap-synthetic-data.git`).
4. Click *Run all*. Same form, same widgets, ~10× the headroom.

### Cost

`n1-standard-8` in `us-central1` runs ~$0.38/hr while the VM is on. With idle-shutdown enabled, a typical demo session costs <$1. Always *Stop* the instance from the GCP console when you're done — running unused costs $9/day.

### Deprovisioning

```bash
gcloud workbench instances delete acdoca-form --location=us-central1-a --project=acdoca-synthetic-greg
```

---

## Path C — Laptop / local Spark (largest runs, no UI)

For runs above ~1M rows, or when you want scripted/CI generation, run from your laptop using `scripts/run_generate_bq.py`. No form — pass everything as CLI flags. Writes to BigQuery the same way the notebook does.

### One-time setup

Same as the laptop instructions in the main README:
- Java 17 (`brew install --cask temurin@17`)
- `pip install -e ".[dev,fast]"`
- `gcloud auth application-default login`
- `~/.spark-jars/gcs-connector-hadoop3-2.2.21-shaded.jar` downloaded (the script checks for it and prints the curl command if missing)
- `export ACDOCA_GCS_TEMP_BUCKET=acdoca-synthetic-greg-bq-staging`

### Run a preset

```bash
SPARK_DRIVER_MEMORY=8g python3 scripts/run_generate_bq.py \
  --preset ml_features \
  --full-table-name acdoca-synthetic-greg.synthetic_acdoca.journal_entries
```

### Run a custom config

```bash
SPARK_DRIVER_MEMORY=8g python3 scripts/run_generate_bq.py \
  --preset custom \
  --industry-key pharmaceutical \
  --country-isos US,DE,CH,IE,IN,FR,GB,JP \
  --complexity very_high \
  --txn-per-cc-per-period 3000 \
  --ic-pct 0.35 \
  --sc-chains 100 \
  --full-table-name acdoca-synthetic-greg.synthetic_acdoca.journal_entries
```

That config produces ~580K rows. For ~1M rows: bump `--country-isos` to ~12 and `--txn-per-cc-per-period` to 3500.

Use `--help` for the full flag list.

---

## Choosing between Vertex and laptop

- **Need the form?** → Vertex Workbench
- **Generating from a script / CI / batch?** → laptop or a Dataproc Serverless job
- **Above ~1M rows?** → laptop with `SPARK_DRIVER_MEMORY=8g` (or larger if your machine has it), or graduate to Dataproc Serverless
- **Working with someone non-technical?** → Vertex Workbench so they get the form UI
