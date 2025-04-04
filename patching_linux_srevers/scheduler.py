import pandas as pd
import datetime
from sqlalchemy import create_engine

# -------------------------
# 1. DB Configuration
# -------------------------
DB_CONFIG = {
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': 3306,
    'database': 'patching_db'
}

def get_engine():
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(url)

# -------------------------
# 2. Load Inventory
# -------------------------
def load_inventory(csv_path="inventory.csv"):
    df = pd.read_csv(csv_path)
    df['patch_status'] = 'Pending'
    return df


# -------------------------
# 3. Schedule Batches by Application, Day-wise
# -------------------------
def schedule_batches(df):
    all_batches = []
    unique_apps = sorted(df['application_name'].unique())  # Keep it sorted for predictability
    base_date = datetime.date.today()

    for i, app in enumerate(unique_apps):
        # Non-prod batch → Day i
        nonprod = df[(df['application_name'] == app) & (df['environment'] == 'non-prod')].copy()
        if not nonprod.empty:
            batch_id = f"{app}_NP_B1"
            schedule_time = base_date + datetime.timedelta(days=i)
            nonprod['batch_id'] = batch_id
            nonprod['patch_schedule_time'] = schedule_time
            nonprod['approval_status'] = 'Pending'
            all_batches.append(nonprod)

        # Prod batch → Day i + 9
        prod = df[(df['application_name'] == app) & (df['environment'] == 'prod')].copy()
        if not prod.empty:
            batch_id = f"{app}_PR_B1"
            schedule_time = base_date + datetime.timedelta(days=i + 9)
            prod['batch_id'] = batch_id
            prod['patch_schedule_time'] = schedule_time
            prod['approval_status'] = 'Pending'
            all_batches.append(prod)

    return pd.concat(all_batches) if all_batches else pd.DataFrame()


# -------------------------
# 4. Save to MySQL
# -------------------------
def save_to_mysql(df):
    engine = get_engine()
    df.to_sql("patch_schedule", con=engine, if_exists="replace", index=False)
    print("Patch schedule saved to MySQL database.")


# -------------------------
# 5. Main Execution
# -------------------------
def main():
    print("Loading server inventory...")
    inventory_df = load_inventory()

    print("Scheduling patches application-wise day-by-day...")
    scheduled_df = schedule_batches(inventory_df)

    print("Saving patch schedule to MySQL...")
    save_to_mysql(scheduled_df)

    print("Scheduler complete. Total applications:", scheduled_df['application_name'].nunique())

if __name__ == "__main__":
    main()
