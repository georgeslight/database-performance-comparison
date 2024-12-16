import duckdb
import psycopg2
from cassandra.cluster import Cluster


class Database:
    def __init__(self, config, db_type):
        self.config = config
        self.db_type = db_type
        self.connection = None
        self.session = None

    def connect(self):
        if self.db_type == 'postgres':
            return self._connect_postgres()
        if self.db_type == 'duckdb':
            return self._connect_duckdb()
        if self.db_type == 'cassandra':
            return self._connect_cassandra()

    def _connect_postgres(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            print(f"Connected to Postgres database: {self.config.get('dbname')}")
            return self.connection
        except Exception as e:
            print(f'Error connecting to Postgres: {e}')

    def _connect_duckdb(self):
        try:
            filepath = self.config.get("filepath")
            self.connection = duckdb.connect(filepath)
            print(f'Connected to DuckDB at {filepath}')
            return self.connection
        except Exception as e:
            print(f'Error connecting to DuckDB: {e}')

    def _connect_cassandra(self):
        try:
            cluster = Cluster([self.config.get("host")], port=self.config.get("port"))
            self.session = cluster.connect()
            self.session.execute(f"""CREATE KEYSPACE IF NOT EXISTS {self.config.get('keyspace')} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}};""")
            self.session.set_keyspace(self.config.get("keyspace"))
            print('Connected to Cassandra')
            return self.session
        except Exception as e:
            print(f'Error connecting to Cassandra: {e}')
        
    