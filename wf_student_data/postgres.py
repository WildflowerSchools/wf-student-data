import psycopg2
import os

class PostgresClient:
    def __init__(
        self,
        dbname=None,
        user=None,
        password=None,
        host=None,
        port=None
    ):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        if self.dbname is None:
            self.dbname = os.getenv('STUDENT_DATABASE_DBNAME')
        if self.user is None:
            self.user = os.getenv('STUDENT_DATABASE_USER')
        if self.password is None:
            self.password = os.getenv('STUDENT_DATABASE_PASSWORD')
        if self.host is None:
            self.host = os.getenv('STUDENT_DATABASE_HOST')
        if self.port is None:
            self.port = os.getenv('STUDENT_DATABASE_PORT')
        self.connect_kwargs = dict()
        if self.dbname is not None:
            self.connect_kwargs['dbname'] = self.dbname
        if self.user is not None:
            self.connect_kwargs['user'] = self.user
        if self.password is not None:
            self.connect_kwargs['password'] = self.password
        if self.host is not None:
            self.connect_kwargs['host'] = self.host
        if self.port is not None:
            self.connect_kwargs['port'] = self.port
    
    def connect(self):
        conn = psycopg2.connect(**self.connect_kwargs)
        return conn
        


