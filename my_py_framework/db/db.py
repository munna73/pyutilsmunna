import os
import sys
import logging
import traceback
import cx_Oracle
import pandas as pd
from configparser import ConfigParser


class Database:
    def __init__(self, config_path='config.ini'):
        self.config = ConfigParser()
        self.config.read(config_path)
        self.setup_logging()
        self.logger = logging.getLogger(__name__)
        self.logger.info("Initialized Database class")

    def setup_logging(self):
        """Set up logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        logging.info("Logging has been configured.")

    def read_config_value(self, section, key, fallback=None):
        """Read a value from the config, with fallback support."""
        try:
            value = self.config.get(section, key, fallback=fallback)
            self.logger.info(f"Read config value: [{section}] {key} = {value}")
            return value
        except Exception as e:
            self.logger.error(f"Error reading config value for [{section}] {key}: {e}")
            return fallback

    def get_query(self, section, key='query'):
        """Reads a SQL query from a specified section and key in config.ini."""
        try:
            query = self.config.get(section, key)
            self.logger.info(f"Retrieved query from section [{section}], key '{key}'.")
            return query
        except Exception as e:
            self.logger.error(f"Failed to retrieve query from [{section}] '{key}': {e}")
            return None

    def run_query(self, query):
        """Execute SQL query and return a DataFrame."""
        pd_year = os.environ.get('pd_year')
        if not pd_year:
            self.logger.warning("Environment variable 'pd_year' not set. Falling back to config.")
            pd_year = self.read_config_value('pd_year', 'pd_year')
            if not pd_year:
                raise ValueError("Missing 'pd_year' in both environment and config.")
            os.environ['pd_year'] = pd_year

        try:
            self.logger.info("Connecting to Oracle database...")
            conn = cx_Oracle.connect(
                user=self.read_config_value('base', 'user'),
                password=self.read_config_value('base', 'password'),
                dsn=f"{self.read_config_value('base', 'host')}:{self.read_config_value('base', 'port')}/{self.read_config_value('base', 'service_name')}"
            )
            self.logger.info("Connection established.")

            cursor = conn.cursor()
            self.logger.info("Executing SQL query...")
            cursor.execute(query)
            self.logger.info("Query executed successfully.")

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            df = pd.DataFrame(rows, columns=columns)
            self.logger.info(f"Query returned {len(df)} rows.")
            return df

        except Exception as e:
            self.logger.error(f"An error occurred during query execution:\n{traceback.format_exc()}")
            return pd.DataFrame()
        finally:
            try:
                cursor.close()
                conn.close()
                self.logger.info("Database connection closed.")
            except Exception as e:
                self.logger.warning("Failed to close cursor/connection cleanly.")
