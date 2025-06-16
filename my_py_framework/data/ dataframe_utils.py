import pandas as pd

def compare_dataframes_on_key(df1, df2, key_columns):
    """
    Compare two DataFrames on given key(s) and return column-level differences.
    All column names are converted to lowercase for comparison.
    
    Args:
        df1 (pd.DataFrame): First DataFrame.
        df2 (pd.DataFrame): Second DataFrame.
        key_columns (list or str): Column name(s) to use as the primary key.

    Returns:
        pd.DataFrame: A DataFrame showing where values differ, including source and target values.
    """
    # Convert column names to lowercase
    df1.columns = df1.columns.str.lower()
    df2.columns = df2.columns.str.lower()

    # Normalize key column names to lowercase
    if isinstance(key_columns, str):
        key_columns = [key_columns]
    key_columns = [k.lower() for k in key_columns]

    # Set keys as index
    df1_keyed = df1.set_index(key_columns).sort_index()
    df2_keyed = df2.set_index(key_columns).sort_index()

    # Get common columns for comparison
    common_columns = df1_keyed.columns.intersection(df2_keyed.columns)

    df1_aligned = df1_keyed[common_columns]
    df2_aligned = df2_keyed[common_columns]

    # Compare the two aligned DataFrames
    comparison = df1_aligned.compare(df2_aligned, keep_shape=True, keep_equal=False)

    # Reset index for output readability
    comparison.reset_index(inplace=True)

    return comparison
