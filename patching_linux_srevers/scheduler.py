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
    import datetime

    all_batches = []
    base_date = datetime.date.today()

    # Split environments
    nonprod_df = df[df['environment'] == 'non-prod'].copy()
    prod_df = df[df['environment'] == 'prod'].copy()

    # Get all (app, db_status) combinations
    combos = df[['application_name', 'DB_status']].drop_duplicates().values.tolist()

    np_day_offset = 0
    pr_day_offset = 10
    np_schedule_map = {}

    batch_counter = 1  # Unique batch ID counter

    # Helper to chunk groups into batches of ~10 servers/day
    def chunk_into_daily_batches(grouped_df):
        batches = []
        day = 0
        temp_group = []
        count = 0

        for group_df in grouped_df:
            temp_group.append(group_df)
            count += len(group_df)

            if count >= 10:
                batches.append((day, temp_group))
                temp_group = []
                count = 0
                day += 1

        if temp_group:
            batches.append((day, temp_group))
        return batches

    # Step 1: Group non-prod by (app, db_status)
    np_groups = []
    for app, db_status in combos:
        group = nonprod_df[
            (nonprod_df['application_name'] == app) &
            (nonprod_df['DB_status'] == db_status)
        ]
        if not group.empty:
            np_groups.append(group)

    # Step 2: Create daily non-prod batches (~10 servers per day)
    daily_np_batches = chunk_into_daily_batches(np_groups)

    for day_offset, group_list in daily_np_batches:
        schedule_time = base_date + datetime.timedelta(days=day_offset)
        for group_df in group_list:
            app = group_df['application_name'].iloc[0]
            db_status = group_df['DB_status'].iloc[0]
            np_schedule_map[(app, db_status)] = schedule_time

            group_df = group_df.copy()
            group_df['batch_id'] = f"NP_BATCH_{batch_counter}"
            group_df['patch_schedule_time'] = schedule_time
            group_df['approval_status'] = 'Pending'
            all_batches.append(group_df)
            batch_counter += 1

    # Step 3: Group prod by (app, db_status)
    pr_groups = []
    for app, db_status in combos:
        group = prod_df[
            (prod_df['application_name'] == app) &
            (prod_df['DB_status'] == db_status)
        ]
        if not group.empty:
            pr_groups.append((app, db_status, group))

    # Step 4: Schedule prod batches using map or pr_day_offset
    pr_batches_by_day = {}

    for app, db_status, group_df in pr_groups:
        if (app, db_status) in np_schedule_map:
            schedule_time = np_schedule_map[(app, db_status)] + datetime.timedelta(days=10)
        else:
            schedule_time = base_date + datetime.timedelta(days=pr_day_offset)
            pr_day_offset += 1

        schedule_day = (schedule_time - base_date).days
        if schedule_day not in pr_batches_by_day:
            pr_batches_by_day[schedule_day] = []
        pr_batches_by_day[schedule_day].append(group_df)

    # Chunk prod batches into daily buckets (~10 servers per day)
    for day_offset in sorted(pr_batches_by_day.keys()):
        batch_groups = pr_batches_by_day[day_offset]
        daily_pr_batches = chunk_into_daily_batches(batch_groups)
        for offset, group_list in daily_pr_batches:
            schedule_time = base_date + datetime.timedelta(days=day_offset + offset)
            for group_df in group_list:
                group_df = group_df.copy()
                group_df['batch_id'] = f"PR_BATCH_{batch_counter}"
                group_df['patch_schedule_time'] = schedule_time
                group_df['approval_status'] = 'Pending'
                all_batches.append(group_df)
                batch_counter += 1

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
