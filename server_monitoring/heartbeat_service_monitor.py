import asyncio
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import json

# CONFIG
BATCH_SIZE = 200
POWERSHELL_SCRIPT = r"C:\Scripts\monitor_agent.ps1"
SERVICE_NAME = "HealthService"
#MYSQL_URI = "mysql+pymysql://user:password@host:port/db_name"

# Dummy server list for test
all_servers = ["server001", "server002", "server003", "..."]  # Add your real list

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

        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=30)

        if stderr:
            return {
                "server_name": server,
                "status": "Error",
                "action": stderr.decode().strip(),
                "service_name": SERVICE_NAME,
                "log_time": datetime.now()
            }

        try:
            output = stdout.decode().strip()
            data = json.loads(output)
        except Exception as e:
            return {
                "server_name": server,
                "status": "Error",
                "action": f"Invalid JSON output: {str(e)}",
                "service_name": SERVICE_NAME,
                "log_time": datetime.now()
            }

        return {
            "server_name": data.get("Server", server),
            "status": data.get("Status", "Unknown"),
            "action": data.get("Action", "Unknown"),
            "service_name": data.get("ServiceName", SERVICE_NAME),
            "log_time": datetime.now()
        }

    except asyncio.TimeoutError:
        return {
            "server_name": server,
            "status": "Timeout",
            "action": "Script execution timed out",
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

async def run_in_batches(servers):
    all_results = []
    for i in range(0, len(servers), BATCH_SIZE):
        batch = servers[i:i + BATCH_SIZE]
        print(f"Processing batch {i // BATCH_SIZE + 1} with {len(batch)} servers...")
        tasks = [check_service(server) for server in batch]
        results = await asyncio.gather(*tasks)
        all_results.extend(results)
    return all_results

def main():
    results = asyncio.run(run_in_batches(all_servers))

if __name__ == "__main__":
    main()
