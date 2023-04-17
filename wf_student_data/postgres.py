import wf_student_data.utils
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
                data_list = cur.fetchall()
            else:
                data_list = None
        if not existing_connection:
            connection.commit()
            connection.close()
        return data_list, description

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
        # Read data from student database
        logger.info('Fetching \'{}\' table from \'{}\' schema'.format(
           table_name,
           schema_name 
        ))
        data, description = self.select_rows(
            schema_name=schema_name,
            table_name=table_name,
            select_column_names=None,
            match_column_names=None,
            match_values_list=None,
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
            return_column_names=None,
            connection=connection
        )
    
    def update_records(
        self,
        schema_name,
        table_name,
        update_values,
        index_column_names,
        value_column_names,
        update_time=None,
        connection=None
    ):
        if update_time is None:
            update_time = datetime.datetime.now(tz=datetime.timezone.utc)
        records = self.fetch_dataframe(
            schema_name=schema_name,
            table_name=table_name,
            index_column_names=['record_id'],
            connection=connection
        )
        current_records = (
            records
            .loc[records['record_end'].isna()]
            .reset_index()
            .set_index(index_column_names)
            .reindex(columns=['record_id'] + value_column_names)
        )
        current_values=current_records.reindex(columns=value_column_names)
        new_rows, deleted_rows, changed_rows, unchanged_rows = wf_student_data.utils.compare_dataframes(
            current=current_values,
            updates=update_values
        )
        new_records = pd.concat([changed_rows, new_rows])
        end_records_value_indices = changed_rows.index.union(deleted_rows.index)
        end_record_ids = (
            current_records
            .loc[end_records_value_indices]
            .record_id
            .tolist()
        )
        self.end_records(
            schema_name=schema_name,
            table_name=table_name,
            record_ids=end_record_ids,
            record_end=update_time,
            connection=connection
        )
        self.start_records(
            schema_name=schema_name,
            table_name=table_name,
            record_start=update_time,
            index_column_names=index_column_names,
            value_column_names=value_column_names,
            values=new_records,
            connection=connection
        )

    def start_records(
        self,
        schema_name,
        table_name,
        values,
        index_column_names,
        value_column_names,
        record_start=None,
        connection=None
    ):
        if len(values) == 0:
            logger.warning('Values dataframe is empty')
            return
        if record_start is None:
            record_start = datetime.datetime.now(tz=datetime.timezone.utc)
        insert_df = (
            values
            .assign(record_start=record_start)
        )
        self.insert_dataframe(
            dataframe=insert_df,
            schema_name=schema_name,
            table_name=table_name,
            drop_index=False,
            connection=connection
        )

    def end_records(
        self,
        schema_name,
        table_name,
        record_ids,
        record_end=None,
        connection=None
    ):
        if len(record_ids) == 0:
            logger.warning('Assignment ID list is empty')
            return
        if record_end is None:
            record_end = datetime.datetime.now(tz=datetime.timezone.utc)
        match_column_names = ['record_id']
        match_values_list = [[record_id] for record_id in record_ids]
        update_column_names = ['record_end']
        update_values_list=[[record_end] for _ in range(len(record_ids))]
        self.update_rows(
            schema_name=schema_name,
            table_name=table_name,
            update_column_names=update_column_names,
            update_values_list=update_values_list,
            match_column_names=match_column_names,
            match_values_list=match_values_list,
            return_column_names=None,
            connection=connection
        )

    def select_rows(
        self,
        schema_name,
        table_name,
        select_column_names=None,
        match_column_names=None,
        match_values_list=None,
        connection=None
    ):
        if select_column_names is not None and len(select_column_names) == 0:
            select_column_names = None
        if match_column_names is not None and len(match_column_names) == 0:
            match_column_names = None
        if match_values_list is not None and len(match_values_list) == 0:
            match_values_list = None
        sql_object = self.compose_select_sql(
            schema_name=schema_name,
            table_name=table_name,
            select_column_names=select_column_names,
            match_column_names=match_column_names
        )
        if match_values_list is not None:
            parameters_list = match_values_list
            return_data = True
            data_items = list()
            description_items = list()
            for parameters in parameters_list:
                data_list, description = self.execute(
                    sql_object=sql_object,
                    parameters=parameters,
                    return_data=return_data,
                    connection=connection
                )
                data_items.append(data_list[0])
                description_items.append(description)
            data = data_items
            description = description_items[0]
        else:
            parameters=None
            return_data=True
            data_list, description = self.execute(
                sql_object=sql_object,
                parameters=parameters,
                return_data=return_data,
                connection=connection
            )
            data=data_list
        return data, description

    def select_row(
        self,
        schema_name,
        table_name,
        select_column_names=None,
        match_column_names=None,
        match_values=None,
        connection=None
    ):
        if select_column_names is not None and len(select_column_names) == 0:
            select_column_names = None
        if match_column_names is not None and len(match_column_names) == 0:
            match_column_names = None
        if match_values is not None and len(match_values) == 0:
            match_values = None
        if match_column_names is None or match_values is None:
            raise ValueError('Must specify columns and values to match when selecting single row')
        sql_object = self.compose_select_sql(
            schema_name=schema_name,
            table_name=table_name,
            select_column_names=select_column_names,
            match_column_names=match_column_names
        )
        parameters = match_values
        return_data = True
        data_list, description = self.execute(
            sql_object=sql_object,
            parameters=parameters,
            return_data=return_data,
            connection=connection
        )
        data = data_list[0]
        return data, description

    def insert_rows(
        self,
        schema_name,
        table_name,
        insert_column_names=None,
        insert_values_list=None,
        return_column_names=None,
        connection=None
    ):
        if insert_column_names is not None and len(insert_column_names) == 0:
            insert_column_names = None
        if insert_values_list is not None and len(insert_values_list) == 0:
            insert_values_list = None
        if return_column_names is not None and len(return_column_names) == 0:
            return_column_names = None
        sql_object = self.compose_insert_sql(
            schema_name=schema_name,
            table_name=table_name,
            insert_column_names=insert_column_names,
            return_column_names=return_column_names
        )
        parameters_list = insert_values_list
        if return_column_names is not None:
            data_items = list()
            description_items = list()
            for parameters in parameters_list:
                data_list, description = self.execute(
                    sql_object=sql_object,
                    parameters=parameters,
                    return_data=True,
                    connection=connection
                )
                data_items.append(data_list[0])
                description_items.append(description)
            data = data_items
            description = description_items[0]
        else:
            self.executemany(
                sql_object=sql_object,
                parameters_list=parameters_list,
                connection=connection
            )
            data = None
            description = None
        return data, description

    def insert_row(
        self,
        schema_name,
        table_name,
        insert_column_names=None,
        insert_values=None,
        return_column_names=None,
        connection=None
    ):
        if insert_column_names is not None and len(insert_column_names) == 0:
            insert_column_names = None
        if insert_values is not None and len(insert_values) == 0:
            insert_values = None
        if return_column_names is not None and len(return_column_names) == 0:
            return_column_names = None
        sql_object = self.compose_insert_sql(
            schema_name=schema_name,
            table_name=table_name,
            insert_column_names=insert_column_names,
            return_column_names=return_column_names
        )
        parameters = insert_values
        if return_column_names is not None:
            return_data = True
        else:
            return_data = False
        data_list, description = self.execute(
            sql_object=sql_object,
            parameters=parameters,
            return_data=return_data,
            connection=connection
        )
        if return_data:
            data = data_list[0]
        else:
            data = None
        return data, description

    def update_rows(
        self,
        schema_name,
        table_name,
        update_column_names,
        update_values_list,
        match_column_names=None,
        match_values_list=None,
        return_column_names=None,
        connection=None
    ):
        if match_column_names is not None and len(match_column_names) == 0:
            match_column_names = None
        if match_values_list is not None and len(match_values_list) == 0:
            match_values_list = None
        if return_column_names is not None and len(return_column_names) == 0:
            return_column_names = None
        sql_object = self.compose_update_sql(
            schema_name=schema_name,
            table_name=table_name,
            match_column_names=match_column_names,
            update_column_names=update_column_names,
            return_column_names=return_column_names
        )
        if match_values_list is None:
            match_values_list = [[]*len(update_values_list)]
        if match_values_list is not None:
            parameters_list = [list(update_values) + list(match_values) for update_values, match_values in zip(update_values_list, match_values_list)]
        else:
            parameters_list = update_values_list
        if return_column_names is not None:
            return_data = True
            data_items = list()
            description_items = list()
            for parameters in parameters_list:
                data_list, description = self.execute(
                    sql_object=sql_object,
                    parameters=parameters,
                    return_data=return_data,
                    connection=connection
                )
                data_items.append(data_list[0])
                description_items.append(description)
            data = data_items
            description = description_items[0]
        else:
            self.executemany(
                sql_object=sql_object,
                parameters_list=parameters_list,
                connection=connection
            )
            data = None
            description = None
        return data, description

    def update_row(
        self,
        schema_name,
        table_name,
        update_column_names,
        update_values,
        match_column_names=None,
        match_values=None,
        return_column_names=None,
        connection=None
    ):
        if match_column_names is not None and len(match_column_names) == 0:
            match_column_names = None
        if match_values is not None and len(match_values) == 0:
            match_values = None
        if return_column_names is not None and len(return_column_names) == 0:
            return_column_names = None
        if match_column_names is None or match_values is None:
            raise ValueError('Must specify columns and values to match when updating single row')
        sql_object = self.compose_update_sql(
            schema_name=schema_name,
            table_name=table_name,
            match_column_names=match_column_names,
            update_column_names=update_column_names,
            return_column_names=return_column_names
        )
        if match_values is not None:
            parameters = list(update_values) + list(match_values)
        else:
            parameters = update_values
        if return_column_names is not None:
            return_data = True
        else:
            return_data= False
        data_list, description = self.execute(
            sql_object=sql_object,
            parameters=parameters,
            return_data=return_data,
            connection=connection
        )
        if return_data:
            data=data_list[0]
        else:
            data = None
        return data, description

    def delete_rows(
        self,
        schema_name,
        table_name,
        match_column_names=None,
        match_values_list=None,
        return_column_names=None,
        connection=None
    ):
        if match_column_names is not None and len(match_column_names) == 0:
            match_column_names = None
        if match_values_list is not None and len(match_values_list) == 0:
            match_values_list = None
        if return_column_names is not None and len(return_column_names) == 0:
            return_column_names = None
        sql_object = self.compose_delete_sql(
            schema_name=schema_name,
            table_name=table_name,
            match_column_names=match_column_names,
            return_column_names=return_column_names
        )
        if match_values_list is not None and return_column_names is not None:
            parameters_list = match_values_list
            return_data = True
            data_items = list()
            description_items = list()
            for parameters in parameters_list:
                data_list, description = self.execute(
                    sql_object=sql_object,
                    parameters=parameters,
                    return_data=return_data,
                    connection=connection
                )
                data_items.append(data_list[0])
                description_items.append(description)
            data = data_items
            description = description_items[0]
        elif match_values_list is not None and return_column_names is None:
            parameters_list = match_values_list
            self.executemany(
                sql_object=sql_object,
                parameters_list=parameters_list,
                connection=connection
            )
            data = None
            description = None
        elif match_values_list is None and return_column_names is not None:
            parameters = None
            return_data=True
            data_list, description = self.execute(
                sql_object=sql_object,
                parameters=parameters,
                return_data=return_data,
                connection=connection
            )
            data = data_list
        else:
            parameters = None
            return_data = False
            data_list, description = self.execute(
                sql_object=sql_object,
                parameters=parameters,
                return_data=return_data,
                connection=connection
            )
            data=None
        return data, description

    def delete_row(
        self,
        schema_name,
        table_name,
        match_column_names=None,
        match_values=None,
        return_column_names=None,
        connection=None
    ):
        if match_column_names is not None and len(match_column_names) == 0:
            match_column_names = None
        if match_values is not None and len(match_values) == 0:
            match_values = None
        if return_column_names is not None and len(return_column_names) == 0:
            return_column_names = None
        if match_column_names is None or match_values is None:
            raise ValueError('Must specify columns and values to match when deleting single row')
        sql_object = self.compose_delete_sql(
            schema_name=schema_name,
            table_name=table_name,
            match_column_names=match_column_names,
            return_column_names=return_column_names
        )
        parameters = match_values
        if return_column_names is not None:
            return_data = True
        else:
            return_data = False
        data_list, description = self.execute(
            sql_object=sql_object,
            parameters=parameters,
            return_data=return_data,
            connection=connection
        )
        if return_data:
            data = data_list[0]
        else:
            data = None
        return data, description

    def compose_select_sql(
        self,
        schema_name,
        table_name,
        select_column_names=None,
        match_column_names=None
    ):
        sql_object = psycopg2.sql.SQL("SELECT")
        if select_column_names is not None and len(select_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("{select_column_names}").format(
                    select_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(select_column_name) for select_column_name in select_column_names]),
                )
            ])
        else:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("*")
            ])
        sql_object = psycopg2.sql.SQL(' ').join([
            sql_object,
            psycopg2.sql.SQL("FROM {schema_name}.{table_name}").format(
                schema_name=psycopg2.sql.Identifier(schema_name),
                table_name=psycopg2.sql.Identifier(table_name)
            )
        ])
        if match_column_names is not None and len (match_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("WHERE ({match_column_names}) = ({match_value_placeholders})").format(
                    match_column_names=psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(match_column_name) for match_column_name in match_column_names]),
                    match_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(match_column_names))
                )
            ])
        return sql_object

    def compose_insert_sql(
        self,
        schema_name,
        table_name,
        insert_column_names=None,
        return_column_names=None
    ):
        sql_object = psycopg2.sql.SQL("INSERT INTO {schema_name}.{table_name}").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name)
        )
        if insert_column_names is not None and len(insert_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("({insert_column_names}) VALUES ({insert_value_placeholders})").format(
                    insert_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(column_name) for column_name in insert_column_names]),
                    insert_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(insert_column_names))
                )
            ])
        else:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                 psycopg2.sql.SQL("DEFAULT VALUES")
            ])
        if return_column_names is not None and len (return_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("RETURNING {return_column_names}").format(
                    return_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(return_name) for return_name in return_column_names])
                )
            ])
        return sql_object

    def compose_update_sql(
        self,
        schema_name,
        table_name,
        update_column_names,
        match_column_names=None,
        return_column_names=None
    ):
        sql_object = psycopg2.sql.SQL("UPDATE {schema_name}.{table_name} SET {update_specifications}").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name),
            update_specifications=psycopg2.sql.SQL(', ').join([
                    psycopg2.sql.SQL('{update_column_name}={update_value_placeholder}').format(
                        update_column_name=psycopg2.sql.Identifier(update_column_name),
                        update_value_placeholder=psycopg2.sql.Placeholder()
                    )
                    for update_column_name in update_column_names
            ])
        )
        if match_column_names is not None and len (match_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("WHERE ({match_column_names}) = ({match_value_placeholders})").format(
                    match_column_names=psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(match_column_name) for match_column_name in match_column_names]),
                    match_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(match_column_names))
                )
            ])
        if return_column_names is not None and len (return_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("RETURNING {return_column_names}").format(
                    return_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(return_column_name) for return_column_name in return_column_names])
                )
            ])
        return sql_object
    
    def compose_delete_sql(
        self,
        schema_name,
        table_name,
        match_column_names=None,
        return_column_names=None
    ):
        sql_object = psycopg2.sql.SQL("DELETE FROM {schema_name}.{table_name}").format(
            schema_name=psycopg2.sql.Identifier(schema_name),
            table_name=psycopg2.sql.Identifier(table_name)
        )
        if match_column_names is not None and len (match_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("WHERE ({match_column_names}) = ({match_value_placeholders})").format(
                    match_column_names=psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(match_column_name) for match_column_name in match_column_names]),
                    match_value_placeholders=psycopg2.sql.SQL(', ').join(psycopg2.sql.Placeholder() * len(match_column_names))
                )
            ])
        if return_column_names is not None and len (return_column_names) > 0:
            sql_object = psycopg2.sql.SQL(' ').join([
                sql_object,
                psycopg2.sql.SQL("RETURNING {return_column_names}").format(
                    return_column_names = psycopg2.sql.SQL(', ').join([psycopg2.sql.Identifier(return_column_name) for return_column_name in return_column_names])
                )
            ])
        return sql_object

