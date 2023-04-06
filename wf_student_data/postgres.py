import pandas as pd
import numpy as np
import psycopg2
import psycopg2.sql
import tqdm
import tqdm.notebook
import datetime
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
        connection = psycopg2.connect(**self.connect_kwargs)
        return connection

    def execute(
        self,
        sql_object,
        parameters,
        return_data=False,
        connection=None
    ):
        existing_connection = True if connection is not None else False
        if not existing_connection:
            connection = self.connect()
        logger.debug(sql_object.as_string(connection))
        with connection.cursor() as cur:
            cur.execute(sql_object, parameters)
            description = cur.description
            if return_data:
                data = cur.fetchall()
            else:
                data = None
        if not existing_connection:
            connection.commit()
            connection.close()
        return data, description

    def executemany(
        self,
        sql_object,
        parameters_list,
        connection=None
    ):
        existing_connection = True if connection is not None else False
        if not existing_connection:
            connection = self.connect()
        logger.debug(sql_object.as_string(connection))
        with connection.cursor() as cur:
            cur.executemany(sql_object, parameters_list)
        if not existing_connection:
            connection.commit()
            connection.close()

    def fetch_dataframe(
        self,
        schema_name,
        table_name,
        index_column_names=None,
        connection=None
    ):
        ## TODO: Should we add option of using existing connection?
        # Read data from student database
        logger.info('Fetching \'{}\' table from \'{}\' schema'.format(
           table_name,
           schema_name 
        ))
        sql_object = self.compose_select_sql(
            schema_name=schema_name,
            table_name=table_name
        )
        data, description = self.execute(
            sql_object=sql_object,
            parameters=None,
            return_data=True,
            connection=connection
        )
        column_names = [descriptor.name for descriptor in description]
        # Convert to dataframe
        logger.info('Converting to Pandas dataframe')
        dataframe = pd.DataFrame(
            data,
            columns=column_names
        )
        # If index columns are specified, set index of dataframe
        ## TODO: Automate this by inspecting primary key of table?
        if index_column_names is not None:
            dataframe = (
                dataframe
                .set_index(index_column_names)
                .sort_index()
            )
        return dataframe

    def insert_dataframe(
        self,
        dataframe,
        schema_name,
        table_name,
        drop_index=False,
        connection=None
    ):
        ## TODO: Should we add option of *not* using existing connection (create connection within method)?
        dataframe_noindex = dataframe.reset_index(drop=drop_index)
        insert_column_names = dataframe_noindex.columns.tolist()
        insert_values_list = dataframe_noindex.to_dict(orient='tight')['data']
        logger.info('Inserting data into \'{}\' table in \'{}\' schema'.format(
           table_name,
           schema_name 
        ))
        self.insert_rows(
            schema_name=schema_name,
            table_name=table_name,
            insert_column_names=insert_column_names,
            insert_values_list=insert_values_list,
            connection=connection
        )
    
    def update_assignments(
        self,
        schema_name,
        table_name,
        update_values,
        value_index_names,
        value_column_names,
        update_time=None,
        connection=None
    ):
        if update_time is None:
            update_time = datetime.datetime.now(tz=datetime.timezone.utc)
        assignments = self.fetch_dataframe(
            schema_name=schema_name,
            table_name=table_name,
            index_column_names=['assignment_id'],
            connection=connection
        )
        current_assignments = (
            assignments
            .loc[assignments['assignment_end'].isna()]
            .reset_index()
            .set_index(value_index_names)
            .reindex(columns=['assignment_id'] + value_column_names)
        )
        current_values=current_assignments.reindex(columns=value_column_names)
        deleted_values = current_values.loc[current_values.index.difference(update_values.index)]
        new_values = update_values.loc[update_values.index.difference(current_values.index)]
        changed_values = (
            update_values
            .loc[current_values.index.intersection(update_values.index)]
            .loc[
                (
                    current_values.loc[current_values.index.intersection(update_values.index)] !=
                    update_values.loc[current_values.index.intersection(update_values.index)]
                ).apply(np.all, axis=1)
            ]
        )
        end_assignment_ids = (
            current_assignments
            .loc[deleted_values.index.union(changed_values.index)]['assignment_id']
            .tolist()
        )
        new_assignments = pd.concat([changed_values, new_values])
        self.end_assignments(
            schema_name=schema_name,
            table_name=table_name,
            assignment_ids=end_assignment_ids,
            assignment_end=update_time,
            connection=connection
        )
        self.start_assignments(
            schema_name=schema_name,
            table_name=table_name,
            assignment_start=update_time,
            values=new_assignments,
            value_index_names=value_index_names,
            value_column_names=value_column_names,
            connection=connection
        )

    def start_assignments(
        self,
        schema_name,
        table_name,
        values,
        assignment_start=None,
        value_index_names=None,
        value_column_names=None,
        connection=None
    ):
        if len(values) == 0:
            logger.warning('Values dataframe is empty')
            return
        if assignment_start is None:
            assignment_start = datetime.datetime.now(tz=datetime.timezone.utc)
        values = values.copy()
        if value_index_names is None:
            value_index_names = list(values.index.names)
        else:
            values.index.names = value_index_names
        if value_column_names is None:
            value_column_names = values.columns.tolist()
        else:
            values.columns = value_column_names
        insert_df = (
            values
            .assign(assignment_start=assignment_start)
        )
        self.insert_dataframe(
            dataframe=insert_df,
            schema_name=schema_name,
            table_name=table_name,
            drop_index=False,
            connection=connection
        )

    def end_assignments(
        self,
        schema_name,
        table_name,
        assignment_ids,
        assignment_end=None,
        connection=None
    ):
        if len(assignment_ids) == 0:
            logger.warning('Assignment ID list is empty')
        if assignment_end is None:
            assignment_end = datetime.datetime.now(tz=datetime.timezone.utc)
        match_column_names = ['assignment_id']
        match_values_list = [[assignment_id] for assignment_id in assignment_ids]
        update_column_names = ['assignment_end']
        update_values_list=[[assignment_end] for _ in range(len(assignment_ids))]
        self.update_rows(
            schema_name=schema_name,
            table_name=table_name,
            match_column_names=match_column_names,
            match_values_list=match_values_list,
            update_column_names=update_column_names,
            update_values_list=update_values_list,
            connection=connection
        )

    def insert_rows(
        self,
        schema_name,
        table_name,
        insert_column_names,
        insert_values_list,
        connection=None
    ):
        sql_object = self.compose_insert_sql(
            schema_name=schema_name,
            table_name=table_name,
            insert_column_names=insert_column_names,
            return_column_names=None
        )
        parameters_list = insert_values_list
        self.executemany(
            sql_object=sql_object,
            parameters_list=parameters_list,
            connection=connection
        )

    def insert_row(
        self,
        schema_name,
        table_name,
        insert_column_names,
        insert_values,
        return_column_names=None,
        connection=None
    ):
        sql_object = self.compose_insert_sql(
            schema_name=schema_name,
            table_name=table_name,
            insert_column_names=insert_column_names,
            return_column_names=return_column_names
        )
        parameters = insert_values
        return_data = True if return_column_names is not None else False
        data, description = self.execute(
            sql_object=sql_object,
            parameters=parameters,
            return_data=return_data,
            connection=connection
        )
        return data, description

    def update_rows(
        self,
        schema_name,
        table_name,
        match_column_names,
        match_values_list,
        update_column_names,
        update_values_list,
        connection=None
    ):
        sql_object = self.compose_update_sql(
            schema_name=schema_name,
            table_name=table_name,
            match_column_names=match_column_names,
            update_column_names=update_column_names,
            return_column_names=None
        )
        parameters_list = [list(update_values) + list(match_values) for update_values, match_values in zip(update_values_list, match_values_list)]
        self.executemany(
            sql_object=sql_object,
            parameters_list=parameters_list,
            connection=connection
        )

    def update_row(
        self,
        schema_name,
        table_name,
        match_column_names,
        match_values,
        update_column_names,
        update_values,
        return_column_names=None,
        connection=None
    ):
        sql_object = self.compose_update_sql(
            schema_name=schema_name,
            table_name=table_name,
            match_column_names=match_column_names,
            update_column_names=update_column_names,
            return_column_names=return_column_names
        )
        parameters = list(update_values) + list(match_values)
        return_data = True if return_column_names is not None else False
        data, description = self.execute(
            sql_object=sql_object,
            parameters=parameters,
            return_data=return_data,
            connection=connection
        )
        return data, description

    def compose_select_sql(
        self,
        schema_name,
        table_name
    ):
        sql_object = psycopg2.sql.SQL("SELECT * FROM {schema_name}.{table_name}").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name)
        )
        return sql_object

    def compose_insert_sql(
        self,
        schema_name,
        table_name,
        insert_column_names,
        return_column_names=None
    ):
        sql_object = psycopg2.sql.SQL("INSERT INTO {schema_name}.{table_name} ({insert_column_names}) VALUES ({insert_value_placeholders})").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            insert_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(column_name) for column_name in insert_column_names]),
            insert_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(insert_column_names))
        )
        if return_column_names is not None:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("RETURNING ({return_column_names})").format(
                    return_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(return_name) for return_name in return_column_names])
                )
            ])
        return sql_object

    def compose_update_sql(
        self,
        schema_name,
        table_name,
        match_column_names,
        update_column_names,
        return_column_names=None
    ):
        sql_object = psycopg2.sql.SQL("UPDATE {schema_name}.{table_name} SET {update_specifications} WHERE ({match_column_names}) = ({match_value_placeholders});").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            update_specifications=psycopg2.sql.SQL(', ').join([
                    psycopg2.sql.SQL('{update_column_name}={update_value_placeholder}').format(
                        update_column_name=psycopg2.sql.Identifier(update_column_name),
                        update_value_placeholder=psycopg2.sql.Placeholder()
                    )
                    for update_column_name in update_column_names
            ]),
            match_column_names=psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(match_column_name) for match_column_name in match_column_names]),
            match_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(match_column_names))
        )
        if return_column_names is not None:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("RETURNING ({return_column_names})").format(
                    return_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(return_column_name) for return_column_name in return_column_names])
                )
            ])
        return sql_object
