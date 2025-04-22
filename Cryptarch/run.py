import os
import subprocess
import sys
import signal
import time
from contextlib import ExitStack

# Resolve project paths
project_root = os.path.dirname(os.path.abspath(__file__))
app_path = os.path.join(project_root, "app")
backend_path = os.path.join(app_path, "backend")

# Prepare environment with updated PYTHONPATH
env = os.environ.copy()
env["PYTHONPATH"] = f"{app_path}:{backend_path}:" + env.get("PYTHONPATH", "")

# Commands
backend_cmd = ["python", "-m", "uvicorn", "app.backend.main:app", "--reload", "--port", "8000"]
frontend_cmd = ["streamlit", "run", "app/frontend/main.py"]

# Clean shutdown handler
def shutdown_handler(processes):
    print("\nShutting down processes...")
    for proc in processes:
        proc.terminate()

def main():
    processes = []

    signal.signal(signal.SIGINT, lambda sig, frame: shutdown_handler(processes) or sys.exit(0))

    with ExitStack() as stack:
        print("Starting FastAPI backend...")
        backend_proc = subprocess.Popen(backend_cmd, cwd=project_root, env=env)
        processes.append(backend_proc)
        stack.callback(backend_proc.terminate)

        time.sleep(2)  # Give backend time to start

        print("Starting Streamlit frontend...")
        frontend_proc = subprocess.Popen(frontend_cmd, cwd=project_root, env=env)
        processes.append(frontend_proc)
        stack.callback(frontend_proc.terminate)

        # Keep running until one process exits
        while all(proc.poll() is None for proc in processes):
            time.sleep(1)

if __name__ == "__main__":
    main()
