import pandas as pd
import psycopg2
import tqdm
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
        # Read connection specifications from environment variables if not explicitly specified
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
        # Populate connection arguments, leaving out any not specified (so psycopg2 reverts to default)
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
        # Connect to student database
        conn = psycopg2.connect(**self.connect_kwargs)
        return conn

    def fetch_dataframe(
        self,
        schema_name,
        table_name,
        index_columns=None
    ):
        ## TODO: Should we add option of using existing connection?
        # Read data from student database
        sql_object = psycopg2.sql.SQL("SELECT * FROM {schema_name}.{table_name}").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name)
        )
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute(sql_object)
                column_names = [descriptor.name for descriptor in cur.description]
                data_list = cur.fetchall()
        # Convert to dataframe
        dataframe = pd.DataFrame(
            data_list,
            columns=column_names
        )
        # If index columns are specified, set index of dataframe
        ## TODO: Automate this by inspecting primary key of table?
        if index_columns is not None:
            dataframe = (
                dataframe
                .set_index(index_columns)
                .sort_index()
            )
        return dataframe

    def insert_dataframe(
        self,
        dataframe,
        schema_name,
        table_name,
        conn,
        progress_bar=False,
        notebook=False
    ):
        ## TODO: Should we add option of *not* using existing connection (create connection within method)?
        dataframe_noindex = dataframe.reset_index()
        column_names = dataframe_noindex.columns.tolist()
        # Build SQL string which we will use to insert each row of data
        sql_object = psycopg2.sql.SQL("INSERT INTO {schema_name}.{table_name} ({field_names}) VALUES ({value_placeholders})").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            field_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(column_name) for column_name in column_names]),
            value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(column_names))
        )
        # Use the appropriate progress bar if indicated
        if progress_bar:
            if notebook:
                dataframe_iterator = tqdm.notebook.tqdm(dataframe_noindex.iterrows(), total=len(dataframe_noindex))
            else:
                dataframe_iterator = tqdm.tqdm(dataframe_noindex.iterrows(), total=len(dataframe_noindex))
        else:
            dataframe_iterator = dataframe_noindex.iterrows()
        # Iterate through dataframe, inserting each row
        ## TODO: Implement bulk insert?
        with conn.cursor() as cur:
            for index, row in dataframe_iterator:
                cur.execute(sql_object, row.tolist())

