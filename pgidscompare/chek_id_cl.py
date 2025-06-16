import os
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from contextlib import contextmanager
import psycopg2
import pandas as pd
import configparser
from psycopg2.extras import RealDictCursor


class IDChecker:
    """A class to handle ID checking operations between files and PostgreSQL database."""
    
    def __init__(self, config_path: str = 'config.ini', log_file: str = 'check_ids.log'):
        self.config_path = Path(config_path)
        self.log_file = log_file
        self._setup_logging()
        self.config = self._load_config()
    
    def _setup_logging(self) -> None:
        """Configure logging with proper formatting and handlers."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.log_file, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def _load_config(self) -> Dict[str, Any]:
        """Load configuration from INI file with validation."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        self.logger.info(f"Loading configuration from {self.config_path}")
        config = configparser.ConfigParser()
        config.read(self.config_path)
        
        if 'postgres' not in config:
            raise ValueError("Configuration file must contain [postgres] section")
        
        required_keys = ['host', 'port', 'database', 'user', 'query']
        postgres_config = config['postgres']
        
        for key in required_keys:
            if key not in postgres_config:
                raise ValueError(f"Missing required configuration key: {key}")
        
        return dict(postgres_config)
    
    def read_ids_from_file(self, file_path: str) -> pd.DataFrame:
        """Read IDs from a file and return as DataFrame."""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Input file not found: {file_path}")
        
        self.logger.info(f"Reading IDs from file: {file_path}")
        
        try:
            # Handle different file formats
            if file_path.suffix.lower() == '.csv':
                df = pd.read_csv(file_path, header=None, names=['id'])
            else:
                # Assume text file with one ID per line
                with open(file_path, 'r', encoding='utf-8') as f:
                    ids = [line.strip() for line in f if line.strip()]
                df = pd.DataFrame({'id': ids})
            
            # Clean and validate data
            df = df.dropna()  # Remove NaN values
            df['id'] = df['id'].astype(str).str.strip()
            df = df[df['id'] != '']  # Remove empty strings
            df = df.drop_duplicates(subset=['id'])  # Remove duplicates
            
            self.logger.info(f"Successfully read {len(df)} unique IDs from file")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read file {file_path}: {e}")
            raise
    
    @contextmanager
    def _get_db_connection(self):
        """Context manager for database connections."""
        password = os.getenv('POSTGRES_PASSWORD')
        if not password:
            raise EnvironmentError("POSTGRES_PASSWORD environment variable not set")
        
        self.logger.info("Connecting to PostgreSQL")
        conn = None
        
        try:
            conn = psycopg2.connect(
                host=self.config['host'],
                port=int(self.config['port']),
                dbname=self.config['database'],
                user=self.config['user'],
                password=password,
                connect_timeout=30
            )
            yield conn
            
        except psycopg2.Error as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
                self.logger.info("Database connection closed")
    
    def fetch_ids_from_db(self) -> pd.DataFrame:
        """Fetch IDs from PostgreSQL database."""
        self.logger.info("Fetching IDs from PostgreSQL")
        
        with self._get_db_connection() as conn:
            try:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(self.config['query'])
                    rows = cur.fetchall()
                    
                    if not rows:
                        self.logger.warning("No data returned from database query")
                        return pd.DataFrame(columns=['id'])
                    
                    db_df = pd.DataFrame(rows)
                    
                    if 'id' not in db_df.columns:
                        raise KeyError("The query must return a column named 'id'")
                    
                    # Clean and validate data
                    db_df['id'] = db_df['id'].astype(str).str.strip()
                    db_df = db_df[db_df['id'] != '']  # Remove empty strings
                    db_df = db_df.drop_duplicates(subset=['id'])  # Remove duplicates
                    
                    self.logger.info(f"Successfully fetched {len(db_df)} unique IDs from database")
                    return db_df
                    
            except Exception as e:
                self.logger.error(f"Failed to execute query: {e}")
                raise
    
    def compare_ids(self, file_df: pd.DataFrame, db_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """Compare IDs and return analysis results."""
        self.logger.info("Comparing IDs...")
        
        # IDs in file but not in database
        missing_in_db = file_df[~file_df['id'].isin(db_df['id'])].copy()
        
        # IDs in database but not in file
        extra_in_db = db_df[~db_df['id'].isin(file_df['id'])].copy()
        
        # Common IDs
        common_ids = file_df[file_df['id'].isin(db_df['id'])].copy()
        
        results = {
            'file_ids': file_df,
            'db_ids': db_df,
            'missing_in_db': missing_in_db,
            'extra_in_db': extra_in_db,
            'common_ids': common_ids
        }
        
        # Log summary
        self.logger.info(f"Analysis complete:")
        self.logger.info(f"  - File IDs: {len(file_df)}")
        self.logger.info(f"  - Database IDs: {len(db_df)}")
        self.logger.info(f"  - Common IDs: {len(common_ids)}")
        self.logger.info(f"  - Missing in DB: {len(missing_in_db)}")
        self.logger.info(f"  - Extra in DB: {len(extra_in_db)}")
        
        return results
    
    def write_to_excel(self, results: Dict[str, pd.DataFrame], output_file: str = 'id_check_result.xlsx') -> None:
        """Write analysis results to Excel file."""
        output_path = Path(output_file)
        self.logger.info(f"Writing results to Excel file: {output_path}")
        
        try:
            with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
                # Write each result to a separate sheet
                for sheet_name, df in results.items():
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                
                # Add summary sheet
                summary_data = {
                    'Category': ['File IDs', 'Database IDs', 'Common IDs', 'Missing in DB', 'Extra in DB'],
                    'Count': [len(results['file_ids']), len(results['db_ids']), 
                             len(results['common_ids']), len(results['missing_in_db']), 
                             len(results['extra_in_db'])]
                }
                summary_df = pd.DataFrame(summary_data)
                summary_df.to_excel(writer, sheet_name='summary', index=False)
            
            self.logger.info(f"Successfully written results to {output_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to write Excel file: {e}")
            raise
    
    def run(self, input_file: str = 'ids.txt', output_file: str = 'id_check_result.xlsx') -> None:
        """Run the complete ID checking process."""
        try:
            # Validate input file
            if not Path(input_file).exists():
                raise FileNotFoundError(f"Input file not found: {input_file}")
            
            # Read input IDs
            file_df = self.read_ids_from_file(input_file)
            
            if file_df.empty:
                raise ValueError("No valid IDs found in input file")
            
            # Fetch IDs from database
            db_df = self.fetch_ids_from_db()
            
            # Compare IDs
            results = self.compare_ids(file_df, db_df)
            
            # Write results to Excel
            self.write_to_excel(results, output_file)
            
            self.logger.info("ID checking process completed successfully")
            
        except Exception as e:
            self.logger.error(f"ID checking process failed: {e}")
            raise


def main():
    """Main entry point."""
    try:
        checker = IDChecker()
        checker.run()
        
    except Exception as e:
        logging.error(f"Application failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()