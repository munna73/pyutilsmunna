import pandas as pd
import logging

# Define constants for clarity
MISSING_IN_TARGET = "Target"
MISSING_IN_SOURCE = "Source"
MESSAGE_NO_MISMATCHED_RECORDS = "There are no mismatched Records in Oracle and Postgres"
MESSAGE_NO_DIFFERENCES_FOUND = "No differences found"

def audit_dataframe_differences(
    srce_df: pd.DataFrame,
    trgt_df: pd.DataFrame,
    primary_key: str,
    srce_table: str,
    trgt_table: str,
    omit_columns: list[str] = None # New parameter: List of columns to omit from value comparison
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compares two Pandas DataFrames (source and target) based on a primary key
    to identify missing records and differing column values, with an option
    to omit specific columns from value comparison.

    Args:
        srce_df (pd.DataFrame): The source DataFrame.
        trgt_df (pd.DataFrame): The target DataFrame.
        primary_key (str): The name of the primary key column to use for comparison.
        srce_table (str): The logical name of the source table (for reporting missing data).
        trgt_table (str): The logical name of the target table (for reporting missing data).
        omit_columns (list[str], optional): A list of column names to exclude from
                                            the column-wise value comparison. These will
                                            be converted to lowercase for comparison.
                                            Defaults to None.

    Returns:
        tuple: A tuple containing two pandas.DataFrames:
            - missing_data_df: Contains records that are missing in either
                               the source or target DataFrame, identified by primary key.
                               Includes 'missing_in' (Target/Source) and 'table_name' columns.
                               Returns a message DataFrame if no missing records are found.
            - mismatch_columns_df: Contains details of column value differences
                                   for records present in both DataFrames.
                                   Includes primary_key, 'column', 'srce_df_value',
                                   and 'trgt_df_value'. Returns a message DataFrame
                                   if no column differences are found.
    Raises:
        ValueError: If DataFrames do not have the same columns or if the
                    primary key is not found in one or both DataFrames.
    """
    # Convert all column names to lower case for consistent processing
    srce_df.columns = srce_df.columns.str.lower()
    trgt_df.columns = trgt_df.columns.str.lower()

    logging.info(f"Target DataFrame columns: {trgt_df.columns.tolist()}")

    # Convert all columns to string type for consistent comparison,
    # especially important for handling mixed types and NaN values during comparison.
    srce_df = srce_df.astype(str)
    trgt_df = trgt_df.astype(str)

    # Validate that both DataFrames have the same set of columns
    if set(srce_df.columns) != set(trgt_df.columns):
        raise ValueError("The DataFrames must have the same columns to compare.")

    # Ensure the primary key exists in both DataFrames
    if primary_key not in srce_df.columns or primary_key not in trgt_df.columns:
        raise ValueError(
            f"Primary key '{primary_key}' not found in one or both DataFrames."
        )

    # --- Identify missing records ---
    # Find records present in source but missing in target
    missing_in_target = srce_df[
        ~srce_df[primary_key].isin(trgt_df[primary_key])
    ].copy()
    missing_in_target["missing_in"] = MISSING_IN_TARGET
    missing_in_target["table_name"] = trgt_table

    # Find records present in target but missing in source
    missing_in_srce = trgt_df[
        ~trgt_df[primary_key].isin(srce_df[primary_key])
    ].copy()
    missing_in_srce["missing_in"] = MISSING_IN_SOURCE
    missing_in_srce["table_name"] = srce_table

    # Concatenate the missing records into a single DataFrame
    missing_data_df = pd.concat([
        missing_in_target[[primary_key, "missing_in", "table_name"]],
        missing_in_srce[[primary_key, "missing_in", "table_name"]],
    ])

    # If no records are missing, return a message DataFrame
    if missing_data_df.empty:
        logging.debug(MESSAGE_NO_MISMATCHED_RECORDS)
        missing_data_df = pd.DataFrame({"message": [MESSAGE_NO_MISMATCHED_RECORDS]})

    # --- Prepare for column value comparison ---
    # Set primary key as index for both dataframes to ensure alignment during iteration
    srce_df_indexed = srce_df.set_index(primary_key)
    trgt_df_indexed = trgt_df.set_index(primary_key)

    # Find common primary key values, as we only compare column values for these records
    common_indices = srce_df_indexed.index.intersection(trgt_df_indexed.index)

    # Determine which columns to actually compare for values
    # Start with all columns except the primary key
    columns_to_compare = [col for col in srce_df_indexed.columns if col != primary_key]

    # If omit_columns is provided, filter them out (case-insensitive)
    if omit_columns:
        omit_columns_lower = {col.lower() for col in omit_columns} # Use a set for efficient lookup
        columns_to_compare = [
            col for col in columns_to_compare if col not in omit_columns_lower
        ]
    
    # Log the columns that will be compared for clarity in debugging
    logging.debug(f"Columns selected for value comparison: {columns_to_compare}")

    # Initialize a list to store the differences found in column values
    differences = []

    # Iterate through common records and compare column values
    for idx in common_indices:
        source_row = srce_df_indexed.loc[idx]
        target_row = trgt_df_indexed.loc[idx]

        for col in columns_to_compare:
            is_source_na = pd.isna(source_row[col])
            is_target_na = pd.isna(target_row[col])

            # Case 1: Both are NaN, considered equal (continue to next column)
            if is_source_na and is_target_na:
                continue
            # Case 2: One is NaN and the other is not, they are different
            elif is_source_na or is_target_na:
                differences.append(
                    {
                        primary_key: idx,
                        "column": col,
                        "srce_df_value": source_row[col],
                        "trgt_df_value": target_row[col],
                    }
                )
            # Case 3: Neither are NaN, check if their values are different
            elif source_row[col] != target_row[col]:
                differences.append(
                    {
                        primary_key: idx,
                        "column": col,
                        "srce_df_value": source_row[col],
                        "trgt_df_value": target_row[col],
                    }
                )

    # Convert the list of differences into a DataFrame
    if differences:
        logging.debug(f"Found {len(differences)} column differences.")
        mismatch_columns_df = pd.DataFrame(differences)
    else:
        # If no differences found in columns, return a message DataFrame
        logging.debug(MESSAGE_NO_DIFFERENCES_FOUND)
        mismatch_columns_df = pd.DataFrame({"message": [MESSAGE_NO_DIFFERENCES_FOUND]})

    return missing_data_df, mismatch_columns_df