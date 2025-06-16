import os
import pandas as pd
import psycopg2

class PostgresConnector:
    def __init__(self, config):
        self.config = config['POSTGRES']

    def run_query(self, sql):
        conn = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            user=self.config['user'],
            password=os.environ['PG_PASSWORD'],
            dbname=self.config['database']
        )
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df
