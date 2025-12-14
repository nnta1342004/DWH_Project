import psycopg2
import pandas as pd
import time
from contextlib import contextmanager
from typing import Optional, Dict, Any
import logging

class DatabaseConnection:
    def __init__(self, host: str = "localhost", port: int = 5432, 
                 database: str = "crm_erp", user: str = "postgres", password: str = "6666"):
        self.connection_params = {
            'host': host,
            'port': port,
            'database': database,
            'user': user,
            'password': password
        }
        self.logger = logging.getLogger(__name__)
    
    @contextmanager
    def get_connection(self):
        conn = None
        try:
            conn = psycopg2.connect(**self.connection_params)
            conn.autocommit = True
            yield conn
        except Exception as e:
            self.logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: Optional[tuple] = None) -> pd.DataFrame:
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def execute_command(self, command: str, params: Optional[tuple] = None) -> None:
        with self.get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(command, params)
    
    def create_table_from_csv(self, table_name: str, csv_path: str, 
                             columns: Dict[str, str]) -> None:
        df = pd.read_csv(csv_path)
        df.to_sql(table_name, self.get_connection(), if_exists='replace', index=False)

