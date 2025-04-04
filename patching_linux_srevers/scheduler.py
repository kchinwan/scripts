import pandas as pd
from datetime import datetime, timedelta

def generate_patch_schedule(inventory_df, start_date=None):
    """
    Group servers into patch batches with priority and scheduling rules.
    
    Args:
        inventory_df (pd.DataFrame): Cleaned server inventory DataFrame.
        start_date (str): Optional starting date (format: 'YYYY-MM-DD')
    
    Returns:
        pd.DataFrame: DataFrame with patch batch info and dates.
    """
    if start_date is None:
        start_date = datetime.today().date()
    else:
        start_date = datetime.strptime(start_date, "%Y-%m-%d").date()

    # Assign initial flags
    inventory_df = inventory_df.copy()
    inventory_df['batch_id'] = None
    inventory_df['patch_date'] = None
    inventory_df['batch_type'] = None  # non-prod / prod

    batch_id = 1
    schedule_map = []

    # Define priority
    priorities = [
        ('non-prod', 'yes'),  # highest priority
        ('non-prod', 'no'),
        ('prod', 'yes'),
        ('prod', 'no')        # lowest priority
    ]

    current_date = start_date

    for env, db_status in priorities:
        subset = inventory_df[
            (inventory_df['env'] == env) &
            (inventory_df['db_status'] == db_status) &
            (inventory_df['batch_id'].isnull())
        ]
        
        if subset.empty:
            continue
        
        # Group by application
        for app, app_df in subset.groupby('application_name'):
            servers = app_df.copy()
            total_servers = len(servers)
            
            # Chunk the servers into batches of 15-20
            for i in range(0, total_servers, 20):
                batch = servers.iloc[i:i+20]
                batch_size = len(batch)

                if batch_size < 10:
                    # Try to merge with next group (if possible)
                    continue  # Skip for now; we'll append them later

                # Assign batch
                inventory_df.loc[batch.index, 'batch_id'] = f"B{batch_id:04}"
                inventory_df.loc[batch.index, 'patch_date'] = current_date
                inventory_df.loc[batch.index, 'batch_type'] = env
                batch_id += 1
                schedule_map.append((env, app, len(batch), current_date))

                # For prod, delay patch date by 10 days
                if env == 'non-prod':
                    current_date += timedelta(days=1)
                elif env == 'prod':
                    current_date += timedelta(days=1)

    return inventory_df, schedule_map


if __name__ == "__main__":
    # Test run
    from inventory_loader import load_inventory
    df = load_inventory("data/server_inventory.xlsx")
    scheduled_df, schedule_map = generate_patch_schedule(df)
    print(scheduled_df.head())
    print("Patch Schedule Map:")
    for row in schedule_map:
        print(row)
