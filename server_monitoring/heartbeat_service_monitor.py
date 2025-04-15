import asyncio
import json
import pandas as pd
from asyncio.subprocess import PIPE
from sqlalchemy import create_engine

# ---- Config ----

ps_script_path = r"C:\Scripts\monitor_agent.ps1"  # Update if needed
servers = [...]  # Full list of server names (strings)
db_uri = "mysql+pymysql://your_user:your_pass@your_db_host/your_database"
engine = create_engine(db_uri)
BATCH_SIZE = 200
TIMEOUT = 20  # seconds

# ---- Async function to run PowerShell ----

async def monitor_server(server):
    try:
        process = await asyncio.create_subprocess_exec(
            "powershell", "-File", ps_script_path, "-ServerName", server,
            stdout=PIPE, stderr=PIPE
        )

        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=TIMEOUT)
        except asyncio.TimeoutError:
            process.kill()
            return {
                "server_name": server,
                "status": "Error",
                "action": "Timeout while connecting"
            }

        if stderr:
            return {
                "server_name": server,
                "status": "Error",
                "action": stderr.decode().strip()
            }

        try:
            data = json.loads(stdout.decode().strip())
            return {
                "server_name": data.get("Server", server),
                "status": data.get("Status", "Unknown"),
                "action": data.get("Action", "Unknown")
            }
        except json.JSONDecodeError:
            return {
                "server_name": server,
                "status": "Error",
                "action": "Invalid JSON from PowerShell"
            }

    except Exception as e:
        return {
            "server_name": server,
            "status": "Error",
            "action": f"Unexpected error: {str(e)}"
        }

# ---- Utilities ----

async def process_batch(server_batch):
    tasks = [monitor_server(s) for s in server_batch]
    return await asyncio.gather(*tasks)

def chunk_list(data, chunk_size):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]

def run_batches(servers):
    all_results = []

    for i, batch in enumerate(chunk_list(servers, BATCH_SIZE), 1):
        print(f"Processing batch {i} of {len(servers)//BATCH_SIZE + 1}")
        results = asyncio.run(process_batch(batch))
        all_results.extend(results)

    return all_results

# ---- Main Entry ----

if __name__ == "__main__":
    results = run_batches(servers)
    df = pd.DataFrame(results)
    df['log_time'] = pd.Timestamp.now()

    try:
        df.to_sql("agent_status_log", con=engine, if_exists='append', index=False)
        print(f"Inserted {len(df)} rows into MySQL.")
    except Exception as e:
        print(f"DB insert failed: {e}")
