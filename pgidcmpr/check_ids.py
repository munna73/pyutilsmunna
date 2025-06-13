import os
import logging
import psycopg2
import pandas as pd
import configparser
from psycopg2.extras import RealDictCursor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("check_ids.log"),
        logging.StreamHandler()
    ]
)

def read_ids_from_file(file_path):
    logging.info(f"Reading IDs from file: {file_path}")
    try:
        df = pd.read_csv(file_path, header=None, names=['id'])
        df['id'] = df['id'].astype(str).str.strip()
        return df
    except Exception as e:
        logging.error(f"Failed to read file {file_path}: {e}")
        raise

def read_config(config_path='config.ini'):
    logging.info(f"Loading configuration from {config_path}")
    config = configparser.ConfigParser()
    config.read(config_path)
    return config['postgres']

def connect_to_postgres(config, password):
    logging.info("Connecting to PostgreSQL")
    try:
        conn = psycopg2.connect(
            host=config.get('host'),
            port=config.get('port'),
            dbname=config.get('database'),
            user=config.get('user'),
            password=password
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        raise

def fetch_ids_from_db(conn, query):
    logging.info("Fetching IDs from PostgreSQL")
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            rows = cur.fetchall()
            db_df = pd.DataFrame(rows)
            if 'id' not in db_df.columns:
                raise KeyError("The query must return a column named 'id'")
            db_df['id'] = db_df['id'].astype(str).str.strip()
            return db_df
    except Exception as e:
        logging.error(f"Failed to execute query: {e}")
        raise

def write_to_excel(file_df, db_df, missing_df, output_file='id_check_result.xlsx'):
    logging.info(f"Writing results to Excel file: {output_file}")
    try:
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            db_df.to_excel(writer, sheet_name='db_ids', index=False)
            file_df.to_excel(writer, sheet_name='file_ids', index=False)
            missing_df.to_excel(writer, sheet_name='missing_ids', index=False)
        logging.info(f"Successfully written to {output_file}")
    except Exception as e:
        logging.error(f"Failed to write Excel file: {e}")
        raise

def main():
    try:
        # Read input IDs
        file_df = read_ids_from_file('ids.txt')

        # Load config and get password
        config = read_config()
        password = os.getenv('POSTGRES_PASSWORD')
        if not password:
            raise EnvironmentError("POSTGRES_PASSWORD environment variable not set.")

        # Connect to DB and fetch IDs
        conn = connect_to_postgres(config, password)
        db_df = fetch_ids_from_db(conn, config.get('query'))
        conn.close()

        # Identify missing IDs
        logging.info("Comparing IDs...")
        missing_ids = file_df[~file_df['id'].isin(db_df['id'])]

        # Write to Excel
        write_to_excel(file_df, db_df, missing_ids)

    except Exception as e:
        logging.error(f"Script failed: {e}")

if __name__ == "__main__":
    main()
