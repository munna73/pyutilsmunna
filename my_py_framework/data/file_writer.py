import pandas as pd
from datetime import datetime

def write_to_excel(dfs: dict, filename_prefix: str):
    # Format current timestamp as mmddyyyy_hhmiss
    timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
    # Construct full filepath with timestamp
    filepath = f"{filename_prefix}_{timestamp}.xlsx"
    
    with pd.ExcelWriter(filepath) as writer:
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

def write_to_csv(df, filename_prefix: str):
    # Create timestamp string
    timestamp = datetime.now().strftime("%m%d%Y_%H%M%S")
    # Construct the full filename with timestamp
    filename = f"{filename_prefix}_{timestamp}.csv"
    # Write DataFrame to CSV
    df.to_csv(filename, index=False)
