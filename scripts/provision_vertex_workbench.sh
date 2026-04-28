#!/usr/bin/env bash
# Provision a Vertex AI Workbench user-managed notebook with enough heap
# headroom to run the BQ Studio form on every preset. Idempotent: prints
# "already exists" and exits 0 if the instance is already there.
#
# Cost: n1-standard-8 in us-central1 ≈ $0.38/hr running, $0 stopped.
# Idle-shutdown is enabled so the VM auto-stops after 1 hr of inactivity.
#
# Usage:
#   ./scripts/provision_vertex_workbench.sh                        # defaults
#   PROJECT=other-project INSTANCE=my-form ./scripts/provision_vertex_workbench.sh
#
# After provisioning, open *Vertex AI → Workbench → User-managed notebooks*
# in the GCP console and click *Open JupyterLab* on the instance.

set -euo pipefail

PROJECT="${PROJECT:-acdoca-synthetic-greg}"
ZONE="${ZONE:-us-central1-a}"
INSTANCE="${INSTANCE:-acdoca-form}"
MACHINE_TYPE="${MACHINE_TYPE:-n1-standard-8}"
IDLE_SHUTDOWN_SECONDS="${IDLE_SHUTDOWN_SECONDS:-3600}"

if ! command -v gcloud >/dev/null 2>&1; then
    echo "ERROR: gcloud CLI not on PATH. Install via https://cloud.google.com/sdk/docs/install"
    exit 1
fi

echo "Provisioning Vertex AI Workbench instance:"
echo "  project       = $PROJECT"
echo "  zone          = $ZONE"
echo "  instance      = $INSTANCE"
echo "  machine type  = $MACHINE_TYPE"
echo "  idle shutdown = ${IDLE_SHUTDOWN_SECONDS}s"

if gcloud notebooks instances describe "$INSTANCE" \
        --location="$ZONE" \
        --project="$PROJECT" \
        >/dev/null 2>&1; then
    echo "Instance '$INSTANCE' already exists in $ZONE. Skipping create."
    echo "Open: https://console.cloud.google.com/vertex-ai/workbench/user-managed?project=$PROJECT"
    exit 0
fi

gcloud notebooks instances create "$INSTANCE" \
    --location="$ZONE" \
    --project="$PROJECT" \
    --machine-type="$MACHINE_TYPE" \
    --vm-image-project=deeplearning-platform-release \
    --vm-image-family=common-cpu-notebooks \
    --metadata="idle-shutdown=true,idle-shutdown-timeout=${IDLE_SHUTDOWN_SECONDS}"

echo
echo "Created. Open JupyterLab via:"
echo "  https://console.cloud.google.com/vertex-ai/workbench/user-managed?project=$PROJECT"
echo
echo "Inside JupyterLab, open a Terminal and clone the repo:"
echo "  git clone https://github.com/GDAVIDREEVES/bigquery-sap-synthetic-data.git"
echo "  cd bigquery-sap-synthetic-data"
echo "Then open notebooks/02_generate_acdoca_bq_studio.ipynb and click Run all."
echo
echo "When done: stop the instance to avoid running costs:"
echo "  gcloud notebooks instances stop $INSTANCE --location=$ZONE --project=$PROJECT"
