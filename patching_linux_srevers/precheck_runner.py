import os
import paramiko
import pandas as pd
import mysql.connector
from datetime import datetime

# ========== CONFIGURATION ==========
DB_CONFIG = {
    'host': 'localhost',
    'user': 'your_username',
    'password': 'your_password',
    'database': 'patching_db'
}

LOG_DIR = "logs"
SSH_USER = "your_ssh_user"
SSH_KEY_PATH = "/path/to/your/private_key.pem"  # or use password
PRECHECK_COMMANDS = ["df -h", "uptime"]

# ========== STEP 1: Connect to DB ==========
def get_today_batches():
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor(dictionary=True)
    
    query = """
        SELECT * FROM patch_schedule
        WHERE DATE(patch_schedule_time) = CURDATE()
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows)
    cursor.close()
    conn.close()
    return df

# ========== STEP 2: Run Precheck via SSH ==========
def run_precheck_on_server(hostname, ip_address, batch_id):
    log_folder = os.path.join(LOG_DIR, f"batch_{batch_id}")
    os.makedirs(log_folder, exist_ok=True)
    log_file = os.path.join(log_folder, f"{hostname}.log")

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(ip_address, username=SSH_USER, key_filename=SSH_KEY_PATH)

        with open(log_file, 'w') as f:
            for cmd in PRECHECK_COMMANDS:
                f.write(f"\n\n=== Running: {cmd} ===\n")
                stdin, stdout, stderr = ssh.exec_command(cmd)
                f.write(stdout.read().decode())
                f.write(stderr.read().decode())
        ssh.close()
        return "success"
    except Exception as e:
        with open(log_file, 'a') as f:
            f.write(f"\n\n=== ERROR: {str(e)} ===\n")
        return "fail"

# ========== STEP 3: Update Precheck Status in DB ==========
def update_precheck_status(batch_id, hostname, status):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    query = """
        UPDATE patch_schedule
        SET precheck_status = %s
        WHERE batch_id = %s AND hostname = %s
    """
    cursor.execute(query, (status, batch_id, hostname))
    conn.commit()
    cursor.close()
    conn.close()

# ========== MAIN ==========
def main():
    df = get_today_batches()
    if df.empty:
        print("No batches scheduled for today.")
        return

    for _, row in df.iterrows():
        batch_id = row['batch_id']
        hostname = row['hostname']
        ip_address = row['ip_address']
        print(f"Running precheck on {hostname} ({ip_address}) in batch {batch_id}...")
        status = run_precheck_on_server(hostname, ip_address, batch_id)
        update_precheck_status(batch_id, hostname, status)
        print(f"  ➤ Precheck {status.upper()}\n")

if __name__ == "__main__":
    main()
