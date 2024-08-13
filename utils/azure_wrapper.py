import pyodbc
import time
import os

from utils.utils import exponential_backoff_retries, batch


class AzureDBWrapper:
    def __init__(self):
        self.server = os.environ.get('DB_SERVER')
        self.database = os.environ.get('DB_NAME')
        self.port = os.environ.get('DB_PORT')
        self.username = os.environ.get('DB_USERNAME')
        self.password = os.environ.get('DB_PASSWORD')
        self.conn = None
        self.cursor = None

    def connect(self):
        for sleep_time in exponential_backoff_retries():
            try:
                conn_str = f'DRIVER=ODBC Driver 17 for SQL Server;SERVER={self.server};DATABASE={self.database};UID={self.username};PWD={self.password}'
                self.conn = pyodbc.connect(conn_str)
                self.cursor = self.conn.cursor()
            except pyodbc.Error as e:
                print(f'Error connecting to Azure SQL Database: {str(e)}')
                time.sleep(sleep_time)

    def close(self):
        if self.conn:
            self.conn.close()

    def select(self, query):
        try:
            self.connect()
            self.cursor.execute(query)
            columns = [column[0] for column in self.cursor.description]
            data = [dict(zip(columns, data_row)) for data_row in self.cursor.fetchall()]
            self.close()
            return data
        except pyodbc.Error as e:
            print(f'Error executing query: {str(e)}')
            return None

    def insert(self, table_name, data):
        try:
            self.connect()
            columns = [column[3] for column in self.cursor.columns(table=table_name) if 'identity' not in column[5]]
            placeholders = ','.join(['?'] * len(columns))

            query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})"
            prepared_data = list()
            for obj in data:
                values = [getattr(obj, col) for col in columns]
                prepared_data.append(tuple(values))
            affected_rows = 0

            for data_batch in batch(prepared_data):
                self.cursor.executemany(query, data_batch)
                self.conn.commit()
                affected_rows += sum(abs(self.cursor.rowcount) for _ in data)
            self.close()

            return affected_rows
        except pyodbc.Error as e:
            print(f'Error executing batch insert: {str(e)}')
