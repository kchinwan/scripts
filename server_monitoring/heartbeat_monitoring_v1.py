import platform
import pandas as pd
import re
from datetime import datetime
import sys
import pytz
import requests
import json
import os
import logging as logger
import time
import numpy as np
import asyncio
import aioping


serverlist = [
    "google.com", "cloudflare.com", "openai.com", "github.com", "amazon.com", "mirrors.kernel.org",
    "ftp.heanet.ie", "debian.org", "ubuntu.com", "archlinux.org"
] * 100  # Repeat 10 times to make 100 elements

IPAddresslist = [
    "8.8.8.8", "1.1.1.1", "8.8.4.4", "208.67.222.222", "64.233.187.99", "204.152.191.39",
    "193.1.193.64", "149.20.4.15", "91.189.91.38", "95.217.163.246"
] * 100  

ServerTypelist = [
    "DNS Server", "DNS Server", "DNS Server", "DNS Server", "Web Server", "Linux Mirror",
    "Linux FTP Server", "Linux Distribution", "Linux Distribution", "Linux Distribution"
] * 100  

ClassTypelist = ["Public"] * 1000  # All are public servers


async def is_pingable(host):
    try:
        # Send a ping request (timeout = 3 seconds)
        await aioping.ping(host, timeout=3)
        return True
    except asyncio.TimeoutError:  # Fix: Use asyncio.TimeoutError
        return False
    except Exception as e:
        print(f"Error pinging {host}: {e}")
        return False
        
    
async def check_servers(serverlist):
    """Ping all servers and return a dictionary of results"""
    results = await asyncio.gather(*(is_pingable(server) for server in serverlist))
    return dict(zip(serverlist, results))
    
    
def get_current_time():
    """Get the current timestamp in CST timezone."""
    central = pytz.timezone('US/Central')
    return datetime.now().astimezone(central).strftime('%Y-%m-%d %H:%M:%S')
    
    
def create_dataframe(results, timestamp):
    """Create a DataFrame from ping results."""
    df = pd.DataFrame({
        'HostName': list(results.keys()),
        'StatusColor': ["green" if status else "red" for status in results.values()],
        'PingTime': [timestamp] * len(results),
        'ServerStatus': ["ServerUp" if status else "ServerDown" for status in results.values()],
        'GreenCount': [1 if status else 0 for status in results.values()],
        'RedCount': [1 if not status else 0 for status in results.values()],
        'IPAddress': IPAddresslist[:len(results)], 
        'ServerType': ServerTypelist[:len(results)],
        'Class': ClassTypelist[:len(results)]
    })
    return df
    
async def main():
    global df  
    print("\n**Pinging all servers initially...**")
    
    # Initial Ping
    first_results = await check_servers(serverlist)
    cst_time = get_current_time()
    df = create_dataframe(first_results, cst_time)
    
    print("\n**Initial results stored**\n", df.head())

    # Find down servers
    down_servers = [server for server, status in first_results.items() if not status]

    if down_servers:
        print(f"\n**Waiting 5 minutes before rechecking {len(down_servers)} down servers...**\n")
        await asyncio.sleep(30)  # Wait 5 minutes

        # Recheck only down servers
        print("\n**Rechecking down servers...**")
        second_results = await check_servers(down_servers)
        new_cst_time = get_current_time()

        # Create new DataFrame with second attempt results and append it
        new_df = create_dataframe(second_results, new_cst_time)
        df = pd.concat([df, new_df], ignore_index=True)

        print("\n**Updated DataFrame with Rechecked Servers:**\n", df.head())
        return df
        
if __name__ == "__main__":
    df = asyncio.run(main())
    print(df)
