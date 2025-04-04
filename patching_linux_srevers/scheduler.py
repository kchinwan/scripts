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
# 3. Schedule Batches by App + Env (1 batch per combo)
# -------------------------
def schedule_batches(df):
    all_batches = []
    base_date = datetime.date.today()

    # Separate non-prod and prod groups
    nonprod_groups = df[df['environment'] == 'non-prod'].groupby('application_name')
    prod_groups = df[df['environment'] == 'prod'].groupby('application_name')

    # Schedule non-prod batches: Day 0, 1, 2...
    for i, (app, group_df) in enumerate(nonprod_groups):
        batch_id = f"{app}_NP_B1"
        schedule_time = base_date + datetime.timedelta(days=i)
        group_df = group_df.copy()
        group_df['batch_id'] = batch_id
        group_df['patch_schedule_time'] = schedule_time
        group_df['approval_status'] = 'Pending'
        all_batches.append(group_df)

    # Schedule prod batches: Day 10, 11, 12...
    for i, (app, group_df) in enumerate(prod_groups):
        batch_id = f"{app}_PR_B1"
        schedule_time = base_date + datetime.timedelta(days=i + 9)
        group_df = group_df.copy()
        group_df['batch_id'] = batch_id
        group_df['patch_schedule_time'] = schedule_time
        group_df['approval_status'] = 'Pending'
        all_batches.append(group_df)

    return pd.concat(all_batches) if all_batches else pd.DataFrame()


# -------------------------
# 4. Save to MySQL
# -------------------------
def save_to_mysql(df):
    engine = get_engine()
    df.to_sql("patch_schedule", con=engine, if_exists="replace", index=False)
    print("Patch schedule saved to MySQL database.")



# -------------------------
# 6. Main
# -------------------------
def main():
    print("Loading server inventory...")
    inventory_df = load_inventory()

    print("Scheduling batches (App + Env groups)...")
    scheduled_df = schedule_batches(inventory_df)

    print(" Saving patch schedule to MySQL...")
    save_to_mysql(scheduled_df)



if __name__ == "__main__":
    main()
