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

#Compare DataFrames Records & Columns
def compare_records_cols (srce_df, trgt_df, primary_key, srce_table, trgt_table):
    #Convert all column names to lower case
    srce_df.columns = srce_df.columns.str.lower()
    trgt_df.columns = trgt_df.columns.str.lower()

    # Convert all columns to string type
    srce_df = srce_df.astype(str)
    trgt_df = trgt_df.astype(str)

    if set(srce_df.columns) != set(trgt_df.columns):
        raise ValueError ("The Data Frames must have the same columns to Compare")

    # Ensure primary key exists
    if (
        primary_key not in srce_df.columns
        or primary_key not in trgt_df.columns
    ):
        raise ValueError(
            f"Primary key '{primary_key}' not found in one or both DataFrames."
        )

    #Identify missing rows
    missing_in_target = srce_df [
        ~srce_df[primary_key].isin(trgt_df[primary_key])
    ].copy()
    missing_in_target["missing_in"] = "Target"
    missing_in_target["table_name"] = trgt_table

    missing_in_srce = trgt_df[
        ~trgt_df[primary_key].isin(srce_df[primary_key])
    ].copy()
    missing_in_srce["missing_in"] = "Srce"
    missing_in_srce["table_name"] = srce_table

    missing_data_df = pd.concat([
        missing_in_target[[primary_key, "missing_in", "table_name"]],
        missing_in_srce[[primary_key, "missing_in", "table_name"]],
    ])

    if missing_data_df.empty:
        logging.debug("There are no mismatched Records in Oracle and Postgres")
        missing_data_df = pd.DataFrame({"message": ["There are no mimatched Records Oracle and Postgres"]})

    # Set primary key as index for both dataframes to ensure alignment
    srce_df_indexed = srce_df.set_index(primary_key)
    pg_df_indexed = trgt_df.set_index(primary_key)

    # Find common indices (primary key values)
    common_indices = srce_df_indexed.index.intersection(pg_df_indexed.index)

    # Initialize results
    differences = []

    # Compare each row
    for idx in common_indices:
        rowl = srce_df_indexed.loc[idx]
        row2 = pg_df_indexed.loc[idx]

        # Compare each column value
        for col in srce_df_indexed.columns:
            # Check if values are different (handle NaN values properly).
            if pd.isna(rowl[col]) and pd.isna(row2[col]):
                continue # Both are NaN, considered equal
            elif pd.isna(rowl[col]) or pd.isna(row2[col]):
                # One is NaN, the other isn't
                differences.append(
                    {
                        primary_key: idx,
                        "column": col,
                        "srce_df_value": rowl[col],
                        "trgt_df_value": row2[col],
                    }
                )
            elif rowl[col] != row2[col]:
                differences.append(
                    {
                        primary_key: idx,
                        "column": col,
                        "srce_df_value": rowl[col],
                        "trgt_df_value": row2[col],
                    }
                )

    # Convert results to DataFrame
    if differences:
        logging.debug(f"Found {len(differences)} differences")
        mismatch_columns_df = pd.DataFrame(differences)
    else:
        # Return a simple message in a DataFrame for Excel compatibility
        logging.debug("No differences found")
        mismatch_columns_df = pd.DataFrame({"message": ["No differences found"]})

    return missing_data_df, mismatch_columns_df