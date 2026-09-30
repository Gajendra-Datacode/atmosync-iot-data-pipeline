import os
import subprocess
import sys

def schedule_task():
    task_name = "AtmoSync_dbt_Hourly_Orchestrator"
    python_path = sys.executable
    script_path = os.path.abspath(os.path.join("atmosync_dbt", "orchestrate_dbt.py"))
    
    bat_path = os.path.abspath("run_dbt_task.bat")
    with open(bat_path, "w") as f:
        f.write(f'@echo off\ncd /d "{os.path.dirname(script_path)}"\n"{python_path}" "{script_path}"\n')
    
    print(f"Created batch runner: {bat_path}")

    cmd = [
        "schtasks", "/Create",
        "/SC", "HOURLY",
        "/MO", "1",
        "/TN", task_name,
        "/TR", bat_path,
        "/F"
    ]
    
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode == 0:
        print("Scheduled task registered successfully!")
        print(res.stdout)
    else:
        print("Task registration output:")
        print(res.stdout)
        print(res.stderr)

if __name__ == "__main__":
    schedule_task()
