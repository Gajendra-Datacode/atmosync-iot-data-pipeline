import subprocess
import sys
import datetime
import os

def run_pipeline():
    # Targets the directory where this script sits (atmosync_dbt)
    project_path = os.path.dirname(os.path.abspath(__file__))
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{start_time}] Starting automated AtmoSync dbt pipeline execution...")

    # Execute all dbt models (staging, hourly aggregates, spoilage arbitrage)
    cmd = [sys.executable, "-m", "dbt.cli.main", "run", "--project-dir", project_path]
    process = subprocess.run(cmd, capture_output=True, text=True)

    end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if process.returncode == 0:
        print(f"[{end_time}] Pipeline executed successfully.")
        print(process.stdout)
    else:
        print(f"[{end_time}] Pipeline encountered errors:")
        print(process.stderr)
        print(process.stdout)

if __name__ == "__main__":
    run_pipeline()