import subprocess
import sys
import os

scripts = [
    "ingestion/facebook_ingestion.py",
    "ingestion/youtube_ingestion.py",
    "ingestion/facebook_csv_ingestion.py",
    "ingestion/tiktok_ingestion.py",
]

# ── Step 1: Run ingestion scripts ─────────────────────────────
for script in scripts:
    print(f"\n🚀 Running {script}...")
    result = subprocess.run([sys.executable, script])
    if result.returncode == 0:
        print(f"✅ {script} completed")
    else:
        print(f"⚠️  {script} failed")

# ── Step 2: Run dbt deps ──────────────────────────────────────
print("\n🚀 Running dbt deps...")
dbt_deps = subprocess.run(
    ["dbt", "deps", "--profiles-dir", "/app", "--project-dir", "/app"],
    capture_output=False
)
if dbt_deps.returncode == 0:
    print("✅ dbt deps completed")
else:
    print("⚠️  dbt deps failed")

# ── Step 3: Run dbt run ───────────────────────────────────────
print("\n🚀 Running dbt run...")
dbt_run = subprocess.run(
    ["dbt", "run", "--profiles-dir", "/app", "--project-dir", "/app"],
    capture_output=False
)
if dbt_run.returncode == 0:
    print("✅ dbt run completed")
else:
    print("⚠️  dbt run failed")

# ── Step 4: Run dbt test ──────────────────────────────────────
print("\n🚀 Running dbt test...")
dbt_test = subprocess.run(
    ["dbt", "test", "--profiles-dir", "/app", "--project-dir", "/app"],
    capture_output=False
)
if dbt_test.returncode == 0:
    print("✅ dbt test completed")
else:
    print("⚠️  dbt test failed")