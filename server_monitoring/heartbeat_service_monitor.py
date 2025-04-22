import asyncio
import pandas as pd
from datetime import datetime
import json

# CONFIG
BATCH_SIZE = 200
POWERSHELL_SCRIPT = r"C:\Scripts\monitor_agent.ps1"
SERVICE_NAME = "HealthService"

# Example servers (replace with your actual list)
all_servers = ["server001", "server002", "server003", "..."]

# 🧠 Run a PowerShell script asynchronously for one server
async def check_service(server):
    try:
        process = await asyncio.create_subprocess_exec(
            "powershell",
            "-ExecutionPolicy", "Bypass",
            "-File", POWERSHELL_SCRIPT,
            "-ServerName", server,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)
        except asyncio.TimeoutError:
            process.kill()
            await process.communicate()
            return {
                "server_name": server,
                "status": "Timeout",
                "action": "Script execution timed out",
                "service_name": SERVICE_NAME,
                "log_time": datetime.now()
            }

        stdout_decoded = stdout.decode().strip()
        stderr_decoded = stderr.decode().strip()

        # If PowerShell wrote errors to stderr
        if process.returncode != 0 or stderr_decoded:
            return {
                "server_name": server,
                "status": "Error",
                "action": stderr_decoded or f"Exited with code {process.returncode}",
                "service_name": SERVICE_NAME,
                "log_time": datetime.now()
            }

        # Try parsing JSON from stdout
        try:
            data = json.loads(stdout_decoded)
            return {
                "server_name": data.get("Server", server),
                "status": data.get("Status", "Unknown"),
                "action": data.get("Action", "Unknown"),
                "service_name": data.get("ServiceName", SERVICE_NAME),
                "log_time": datetime.now()
            }
        except json.JSONDecodeError as e:
            return {
                "server_name": server,
                "status": "Error",
                "action": f"Invalid JSON output: {e} | Raw Output: {stdout_decoded}",
                "service_name": SERVICE_NAME,
                "log_time": datetime.now()
            }

    except Exception as e:
        return {
            "server_name": server,
            "status": "Error",
            "action": f"Exception: {str(e)}",
            "service_name": SERVICE_NAME,
            "log_time": datetime.now()
        }

# 📦 Batch processor
async def run_in_batches(servers):
    all_results = []
    for i in range(0, len(servers), BATCH_SIZE):
        batch = servers[i:i + BATCH_SIZE]
        print(f"🚀 Processing batch {i // BATCH_SIZE + 1} with {len(batch)} servers...")
        tasks = [check_service(server) for server in batch]
        results = await asyncio.gather(*tasks, return_exceptions=False)
        all_results.extend(results)
    return all_results

# 🚀 Main runner
def main():
    results = asyncio.run(run_in_batches(all_servers))

    df = pd.DataFrame(results)
    print(df.head())

    # Example: Save to CSV or push to DB
    df.to_csv("agent_status_log.csv", index=False)

if __name__ == "__main__":
    main()
