from configparser import ConfigParser
from db.oracle_connector import OracleConnector
from db.postgres_connector import PostgresConnector
from data.dataframe_utils import compare_dataframes
from data.file_writer import write_to_excel
from messaging.sqs_client import SQSClient
from messaging.s3_client import S3Client

# Load config
config = ConfigParser()
config.read("config.ini")

# Initialize DB connectors
oracle = OracleConnector(config)
pg = PostgresConnector(config)

# Read queries from config
oracle_query = config['ORACLE']['query']
pg_query = config['POSTGRES']['query']

# Execute queries
df_oracle = oracle.run_query(oracle_query)
df_pg = pg.run_query(pg_query)

# Compare DataFrames
df_delta = compare_dataframes(df_oracle, df_pg)

# Write to Excel
write_to_excel({
    'Oracle_Result': df_oracle,
    'Postgres_Result': df_pg,
    'Delta': df_delta
}, 'comparison_report.xlsx')

# SQS operations
sqs = SQSClient(config)
sqs.send_messages_from_file('input.txt')
sqs.receive_and_save_messages('sqs_output.txt')

# S3 operation
s3 = S3Client(config)
s3.download_file('folder/mydata.csv', 'downloaded_data.csv')
