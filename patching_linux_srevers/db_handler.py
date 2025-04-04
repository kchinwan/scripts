import pandas as pd
from sqlalchemy import create_engine
import os

# ----------------------
# CONFIGURATION
# ----------------------
DB_CONFIG = {
    'user': 'your_user',
    'password': 'your_password',
    'host': 'localhost',
    'port': 3306,
    'database': 'patching_db'
}

# ----------------------
# CONNECTION
# ----------------------
def get_engine():
    """Create SQLAlchemy engine."""
    url = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    return create_engine(url)

# ----------------------
# TABLE CREATION (Optional, for first-time setup)
# ----------------------
def create_tables():
    """Create tables in MySQL if they don't exist."""
    engine = get_engine()
    with engine.connect() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS patch_schedule (
            id INT AUTO_INCREMENT PRIMARY KEY,
            batch_id VARCHAR(20),
            hostname VARCHAR(100),
            ip_address VARCHAR(50),
            env VARCHAR(20),
            db_status VARCHAR(10),
            application_name VARCHAR(100),
            patch_date DATE,
            batch_type VARCHAR(20),
            approval_status VARCHAR(20) DEFAULT 'Pending',
            proposed_time VARCHAR(50) DEFAULT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
    print("✅ Tables created or verified.")

# ----------------------
# SAVE TO DATABASE
# ----------------------
def save_patch_schedule(df: pd.DataFrame):
    """
    Save the patch schedule DataFrame to MySQL table.
    
    Args:
        df (pd.DataFrame): The final scheduled DataFrame from scheduler.
    """
    engine = get_engine()
    df_to_save = df.copy()

    # Only keep necessary columns
    required_cols = [
        'batch_id', 'hostname', 'ip_address', 'env', 'db_status',
        'application_name', 'patch_date', 'batch_type'
    ]
    df_to_save = df_to_save[required_cols]
    
    # Save to MySQL
    df_to_save.to_sql('patch_schedule', con=engine, if_exists='append', index=False)
    print(f"✅ Saved {len(df_to_save)} records to MySQL.")


if __name__ == "__main__":
    from scheduler import generate_patch_schedule
    from inventory_loader import load_inventory

    # Load inventory and generate batches
    inventory_df = load_inventory("data/server_inventory.xlsx")
    scheduled_df, _ = generate_patch_schedule(inventory_df)

    # Optional: Create tables
    create_tables()

    # Save to DB
    save_patch_schedule(scheduled_df)
