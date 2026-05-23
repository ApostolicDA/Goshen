@echo off
cd C:\Users\gadis\goshen
call C:\Users\gadis\goshen-dbt-env\Scripts\activate.bat
python run_ingestion.py >> pipeline_log.txt 2>&1
dbt run >> pipeline_log.txt 2>&1
dbt test >> pipeline_log.txt 2>&1
echo Pipeline completed %date% %time% >> pipeline_log.txt