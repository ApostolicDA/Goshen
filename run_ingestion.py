import subprocess
import sys

scripts = [
    "ingestion/facebook_ingestion.py",
    "ingestion/youtube_ingestion.py",
    "ingestion/facebook_csv_ingestion.py",
    "ingestion/tiktok_ingestion.py",
]

for script in scripts:
    print(f"\n🚀 Running {script}...")
    result = subprocess.run([sys.executable, script])
    if result.returncode == 0:
        print(f"✅ {script} completed")
    else:
        print(f"❌ {script} failed")