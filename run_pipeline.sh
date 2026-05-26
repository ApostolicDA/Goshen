#!/bin/bash
# ─────────────────────────────────────────────────────────────────
#  Goshen Analytics Pipeline — run_pipeline.sh
#  Replaces run_pipeline.bat for Docker/WSL2
#
#  Usage:
#    chmod +x run_pipeline.sh   (first time only)
#    ./run_pipeline.sh
# ─────────────────────────────────────────────────────────────────

set -e  # Exit immediately if any command fails

TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
LOG_FILE="./logs/pipeline_$(date '+%Y%m%d_%H%M%S').log"

mkdir -p ./logs

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"
echo "  Goshen Pipeline Started: $TIMESTAMP"              | tee -a "$LOG_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"

# ── Step 1: Build image (cached after first run) ──────────────────
echo ""
echo "▶ Building Docker image..." | tee -a "$LOG_FILE"
docker-compose build 2>&1 | tee -a "$LOG_FILE"

# ── Step 2: Run ingestion ─────────────────────────────────────────
echo ""
echo "▶ Step 1/3 — Running Python ingestion..." | tee -a "$LOG_FILE"
docker-compose run --rm ingestion 2>&1 | tee -a "$LOG_FILE"

# ── Step 3: dbt run ───────────────────────────────────────────────
echo ""
echo "▶ Step 2/3 — Running dbt models..." | tee -a "$LOG_FILE"
docker-compose run --rm dbt_run 2>&1 | tee -a "$LOG_FILE"

# ── Step 4: dbt test ──────────────────────────────────────────────
echo ""
echo "▶ Step 3/3 — Running dbt tests..." | tee -a "$LOG_FILE"
docker-compose run --rm dbt_test 2>&1 | tee -a "$LOG_FILE"

# ── Done ──────────────────────────────────────────────────────────
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"
echo "  ✅ Pipeline Complete: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$LOG_FILE"
echo "  📄 Log saved to: $LOG_FILE"                         | tee -a "$LOG_FILE"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━" | tee -a "$LOG_FILE"
