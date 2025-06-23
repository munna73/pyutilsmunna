import pandas as pd
import logging

# Assuming the audit_dataframe_differences function is defined in your environment

# Example DataFrames
data_source = {
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'age': [30, 24, 35, 29, 22],
    'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Miami'],
    'updtd': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04', '2023-01-05'],
    'updt_usr': ['user1', 'user2', 'user1', 'user3', 'user2'],
    'etl_dttm': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00', '2023-01-04 13:00:00', '2023-01-05 14:00:00']
}
srce_df = pd.DataFrame(data_source)

data_target = {
    'id': [1, 2, 3, 5, 6], # ID 4 is missing, ID 6 is new
    'name': ['Alice', 'Bobby', 'Charlie', 'Eve', 'Frank'], # Bob vs Bobby
    'age': [30, 24, 36, 22, 40], # Charlie's age changed
    'city': ['New York', 'Los Angeles', 'Chicago', 'Miami', 'Dallas'],
    'updtd': ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-05', '2023-01-06'],
    'updt_usr': ['user1', 'user2', 'user4', 'user2', 'user5'],
    'etl_dttm': ['2023-01-01 10:00:00', '2023-01-02 11:00:00', '2023-01-03 12:00:00', '2023-01-05 14:00:00', '2023-01-06 15:00:00']
}
trgt_df = pd.DataFrame(data_target)

# Configure logging (optional, but good practice to see debug messages)
logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')

# Sample usage line of code to call the function
# Omit 'updtd', 'updt_usr', and 'etl_dttm' from column value comparison
missing_data, column_differences = audit_dataframe_differences(
    srce_df=srce_df.copy(), # Pass copies to avoid modifying original DFs
    trgt_df=trgt_df.copy(),
    primary_key='id',
    srce_table='Source_Customers',
    trgt_table='Target_Customers',
    omit_columns=['updtd', 'updt_usr', 'etl_dttm']
)

print("\n--- Missing Data Report ---")
print(missing_data)

print("\n--- Column Differences Report ---")
print(column_differences)