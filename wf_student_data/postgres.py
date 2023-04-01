import pandas as pd
import psycopg2
import psycopg2.sql
import tqdm
import tqdm.notebook
import os
import logging

logger = logging.getLogger(__name__)

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
        logger.info('Connecting to student database with connection specifications {}'.format(self.connect_kwargs))
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
        logger.info('Fetching \'{}\' table from \'{}\' schema'.format(
           table_name,
           schema_name 
        ))
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
        logger.info('Converting to Pandas dataframe')
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
        drop_index=False
    ):
        ## TODO: Should we add option of *not* using existing connection (create connection within method)?
        dataframe_noindex = dataframe.reset_index(drop=drop_index)
        column_names = dataframe_noindex.columns.tolist()
        column_values_list = dataframe_noindex.to_dict(orient='tight')['data']
        logger.info('Inserting data into \'{}\' table in \'{}\' schema'.format(
           table_name,
           schema_name 
        ))
        self.insert_rows(
            schema_name=schema_name,
            table_name=table_name,
            column_names=column_names,
            column_values_list=column_values_list,
            conn=conn
        )
    
    def insert_rows(
        self,
        schema_name,
        table_name,
        column_names,
        column_values_list,
        conn
    ):
        sql_object, parameters = self.compose_insert_rows_sql(
            schema_name=schema_name,
            table_name=table_name,
            column_names=column_names,
            column_values_list=column_values_list
        )
        self.executemany(
            sql_object=sql_object,
            parameters=parameters,
            conn=conn
        )

    def insert_row(
        self,
        schema_name,
        table_name,
        column_names,
        column_values,
        conn,
        return_names=None
    ):
        sql_object, parameters = self.compose_insert_row_sql(
            schema_name=schema_name,
            table_name=table_name,
            column_names=column_names,
            column_values=column_values,
            return_names=return_names
        )
        if return_names is not None:
            data = self.execute(
                sql_object=sql_object,
                parameters=parameters,
                conn=conn,
                return_data=True
            )
            return data
        else:
            self.execute(
                sql_object=sql_object,
                parameters=parameters,
                conn=conn,
                return_data=False
            )

    def update_rows(
        self,
        schema_name,
        table_name,
        index_names,
        index_values_list,
        column_names,
        column_values_list,
        conn
    ):
        sql_object, parameters = self.compose_update_rows_sql(
            schema_name=schema_name,
            table_name=table_name,
            index_names=index_names,
            index_values_list=index_values_list,
            column_names=column_names,
            column_values_list=column_values_list
        )
        self.executemany(
            sql_object=sql_object,
            parameters=parameters,
            conn=conn
        )

    def update_row(
        self,
        schema_name,
        table_name,
        index_names,
        index_values,
        column_names,
        column_values,
        conn
    ):
        sql_object, parameters = self.compose_update_row_sql(
            schema_name=schema_name,
            table_name=table_name,
            index_names=index_names,
            index_values=index_values,
            column_names=column_names,
            column_values=column_values
        )
        self.execute(
            sql_object=sql_object,
            parameters=parameters,
            conn=conn,
            return_data=False
        )

    def compose_insert_rows_sql(
        self,
        schema_name,
        table_name,
        column_names,
        column_values_list
    ):
        sql_object = psycopg2.sql.SQL("INSERT INTO {schema_name}.{table_name} ({column_names}) VALUES ({column_value_placeholders})").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(column_name) for column_name in column_names]),
            column_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(column_names))
        )
        parameters = list(column_values_list)
        return sql_object, parameters

    def compose_insert_row_sql(
        self,
        schema_name,
        table_name,
        column_names,
        column_values,
        return_names=None
    ):
        sql_object = psycopg2.sql.SQL("INSERT INTO {schema_name}.{table_name} ({column_names}) VALUES ({column_value_placeholders})").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(column_name) for column_name in column_names]),
            column_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(column_names))
        )
        if return_names is not None:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("RETURNING ({return_names})").format(
                    return_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(return_name) for return_name in return_names])
                )
            ])
        parameters = list(column_values)
        return sql_object, parameters

    def compose_update_rows_sql(
        self,
        schema_name,
        table_name,
        index_names,
        index_values_list,
        column_names,
        column_values_list
    ):
        if len(index_values_list) != len(column_values_list):
            raise ValueError('Column values list and index values list must be of same length')
        sql_object = psycopg2.sql.SQL("UPDATE {schema_name}.{table_name} SET {column_specifications} WHERE ({index_names}) = ({index_value_placeholders});").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            column_specifications=psycopg2.sql.SQL(', ').join([
                    psycopg2.sql.SQL('{column_name}={column_placeholder}').format(
                        column_name=psycopg2.sql.Identifier(column_name),
                        column_placeholder=psycopg2.sql.Placeholder()
                    )
                    for column_name in column_names
            ]),
            index_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(index_name) for index_name in index_names]),
            index_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(index_names))
        )
        parameters = [list(column_values) + list(index_values) for column_values, index_values in zip(column_values_list, index_values_list)]
        return sql_object, parameters

    def compose_update_row_sql(
        self,
        schema_name,
        table_name,
        index_names,
        index_values,
        column_names,
        column_values
    ):
        sql_object = psycopg2.sql.SQL("UPDATE {schema_name}.{table_name} SET {column_specifications} WHERE ({index_names}) = ({index_value_placeholders});").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            column_specifications=psycopg2.sql.SQL(', ').join([
                    psycopg2.sql.SQL('{column_name}={column_placeholder}').format(
                        column_name=psycopg2.sql.Identifier(column_name),
                        column_placeholder=psycopg2.sql.Placeholder()
                    )
                    for column_name in column_names
            ]),
            index_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(index_name) for index_name in index_names]),
            index_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(index_values))
        )
        parameters = list(column_values) + list(index_values)
        return sql_object, parameters

    def execute(
        self,
        sql_object,
        parameters,
        conn,
        return_data=False
    ):
        logger.debug(sql_object.as_string(conn))
        with conn.cursor() as cur:
            cur.execute(sql_object, parameters)
            if return_data:
                data = cur.fetchall()
                return data

    def executemany(
        self,
        sql_object,
        parameters,
        conn
    ):
        logger.debug(sql_object.as_string(conn))
        with conn.cursor() as cur:
            cur.executemany(sql_object, parameters)
