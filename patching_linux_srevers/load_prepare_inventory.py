import pandas as pd

# Define required columns for validation
REQUIRED_COLUMNS = ['hostname', 'ip_address', 'env', 'db_status', 'application_name']

def load_inventory(filepath):
    """
    Load and validate the server inventory Excel or CSV file.
    
    Args:
        filepath (str): Path to the input Excel or CSV file.
        
    Returns:
        pd.DataFrame: Cleaned and structured inventory DataFrame.
    """
    # Load file
    if filepath.endswith('.xlsx') or filepath.endswith('.xls'):
        df = pd.read_excel(filepath)
    elif filepath.endswith('.csv'):
        df = pd.read_csv(filepath)
    else:
        raise ValueError("Unsupported file type. Please provide an Excel or CSV file.")

    # Lowercase columns and standardize
    df.columns = df.columns.str.strip().str.lower()

    # Validate required columns
    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing columns in input file: {missing_cols}")

    # Clean values
    df['env'] = df['env'].str.strip().str.lower()  # prod / non-prod
    df['db_status'] = df['db_status'].str.strip().str.lower()  # yes / no
    df['application_name'] = df['application_name'].str.strip()

    # Optional: drop duplicates and nulls
    df = df.dropna(subset=REQUIRED_COLUMNS).drop_duplicates()

    return df


if __name__ == "__main__":
    # Example usage
    file_path = "data/server_inventory.xlsx"
    inventory_df = load_inventory(file_path)
    print(f"Loaded {len(inventory_df)} servers")
    print(inventory_df.head())
