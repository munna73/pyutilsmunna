import os
import cx_Oracle
import pandas as pd

class OracleConnector:
    def __init__(self, config):
        self.config = config['ORACLE']

    def run_query(self, sql):
        conn = cx_Oracle.connect(
            user=self.config['user'],
            password=os.environ['ORACLE_PASSWORD'],
            dsn=f"{self.config['host']}:{self.config['port']}/{self.config['service_name']}"
        )
        df = pd.read_sql(sql, con=conn)
        conn.close()
        return df
