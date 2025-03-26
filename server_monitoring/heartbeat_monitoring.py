import asyncio
import aioping
import time

# List of 5000 servers to check (Replace with actual IPs/Hostnames)
SERVERS = ["192.168.1.1", "192.168.1.2", "example.com", "..."]  # Add all 5000 servers

async def ping_server(ip):
    """Ping a server and return 'UP' if reachable, otherwise 'RETRY'."""
    try:
        await aioping.ping(ip, timeout=1)  # Ping with 1s timeout
        return "UP"
    except TimeoutError:
        return "RETRY"

async def monitor_servers():
    """Ping all servers and retry unreachable ones after 5 minutes."""
    start_time = time.time()

    # Step 1: Initial Ping Check
    results = await asyncio.gather(*[ping_server(ip) for ip in SERVERS])

    # Step 2: Identify Unreachable Servers
    failed_servers = [ip for ip, status in zip(SERVERS, results) if status == "RETRY"]

    if failed_servers:
        print(f"\nRetrying {len(failed_servers)} unreachable servers after 5 minutes...\n")
        await asyncio.sleep(300)  # Wait for 5 minutes before retrying

        # Step 3: Retry Ping
        retry_results = await asyncio.gather(*[ping_server(ip) for ip in failed_servers])

        # Step 4: Final Status Update (Mark "DOWN" if still unreachable)
        final_status = {ip: status if status != "RETRY" else "DOWN" for ip, status in zip(failed_servers, retry_results)}
        print("\nFinal Status:", final_status)
    else:
        final_status = {}

    # Step 5: Store Results in Dictionary
    initial_status = {ip: status for ip, status in zip(SERVERS, results)}
    
    # Merge initial + final retry results
    final_results = {**initial_status, **final_status}

    end_time = time.time()
    print(f"\nMonitoring Completed in {round(end_time - start_time, 2)} seconds!\n")
    print("Final Server Status:")
    for ip, status in final_results.items():
        print(f"{ip}: {status}")

    return final_results

if __name__ == "__main__":
    asyncio.run(monitor_servers())
