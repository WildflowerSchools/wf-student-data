import wf_student_data.postgres as postgres
import wf_student_data.transparent_classroom as transparent_classroom
import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def update_tc_data(
    pg_client=None,
    pg_dbname=None,
    pg_user=None,
    pg_password=None,
    pg_host=None,
    pg_port=None,
    tc_client=None,
    tc_username=None,
    tc_password=None,
    tc_api_token=None,
    tc_url_base=None,
    progress_bar=False,
    notebook=False,
    delay=None
):
    # Initialize Postgres client
    if pg_client is None:
        pg_client = postgres.PostgresClient(
            dbname=pg_dbname,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
    # Initialize Transparent Classroom client
    if tc_client is None:
        tc_client = transparent_classroom.TransparentClassroomClient(
            username=tc_username,
            password=tc_password,
            api_token=tc_api_token,
            url_base=tc_url_base
        )
    # Create connection to Postgres student database
    conn = pg_client.connect()
    # Fetch school data from Transparent Classroom
    schools = tc_client.fetch_school_data()
    # Insert school data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=schools,
        schema_name='tc',
        table_name='schools',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Fetch classroom data from Transparent Classroom
    classrooms = tc_client.fetch_classroom_data(
        school_ids=schools.index,
        progress_bar=progress_bar,
        notebook=notebook,
        delay=delay
    )
    # Insert classroom data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=classrooms,
        schema_name='tc',
        table_name='classrooms',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Fetch session data from Transparent Classroom
    sessions = tc_client.fetch_session_data(
        school_ids=schools.index,
        progress_bar=progress_bar,
        notebook=notebook,
        delay=delay
    )
    # Insert session data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=sessions,
        schema_name='tc',
        table_name='sessions',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Fetch user data from Transparent Classroom
    users = tc_client.fetch_user_data(
        school_ids=schools.index,
        progress_bar=progress_bar,
        notebook=notebook,
        delay=delay
    )
    # Insert user data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=users,
        schema_name='tc',
        table_name='users',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Fetch child data and child-parent data from Transparent Classroom
    children, children_parents = tc_client.fetch_child_data(
        school_ids=schools.index,
        progress_bar=progress_bar,
        notebook=notebook,
        delay=delay
    )
    # Insert child data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=children,
        schema_name='tc',
        table_name='children',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Insert child-parent data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=children_parents,
        schema_name='tc',
        table_name='children_parents',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Fetch classroom-child data from Transparent Classroom
    classrooms_children = tc_client.fetch_classroom_child_data(
        session_ids=sessions.index,
        progress_bar=progress_bar,
        notebook=notebook,
        delay=delay
    )
    # Insert classroom-child data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=classrooms_children,
        schema_name='tc',
        table_name='classrooms_children',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Commit changes and close connection
    conn.commit()
    conn.close()


def populate_data_dict_from_local(
    pg_client=None,
    pg_dbname=None,
    pg_user=None,
    pg_password=None,
    pg_host=None,
    pg_port=None,
    directory='.',
    progress_bar=False,
    notebook=False
):
    # Initialize Postgres client
    if pg_client is None:
        pg_client = postgres.PostgresClient(
            dbname=pg_dbname,
            user=pg_user,
            password=pg_password,
            host=pg_host,
            port=pg_port
        )
    # Create connection to Postgres student database
    conn = pg_client.connect()
    # Ethnicity categories
    ## Fetch data from local
    ethnicity_categories = pd.read_pickle(os.path.join(
        directory,
        'ethnicity_categories.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=ethnicity_categories,
        schema_name='data_dict',
        table_name='ethnicity_categories',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Gender categories
    ## Fetch data from local
    gender_categories = pd.read_pickle(os.path.join(
        directory,
        'gender_categories.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=gender_categories,
        schema_name='data_dict',
        table_name='gender_categories',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Household income categories
    ## Fetch data from local
    household_income_categories = pd.read_pickle(os.path.join(
        directory,
        'household_income_categories.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=household_income_categories,
        schema_name='data_dict',
        table_name='household_income_categories',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # NPS categories
    ## Fetch data from local
    nps_categories = pd.read_pickle(os.path.join(
        directory,
        'nps_categories.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=nps_categories,
        schema_name='data_dict',
        table_name='nps_categories',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Boolean categories
    ## Fetch data from local
    boolean_categories = pd.read_pickle(os.path.join(
        directory,
        'boolean_categories.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=boolean_categories,
        schema_name='data_dict',
        table_name='boolean_categories',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Ethnicity map
    ## Fetch data from local
    ethnicity_map = pd.read_pickle(os.path.join(
        directory,
        'ethnicity_map.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=ethnicity_map,
        schema_name='data_dict',
        table_name='ethnicity_map',
        conn=conn,
        drop_index=True,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Gender map
    ## Fetch data from local
    gender_map = pd.read_pickle(os.path.join(
        directory,
        'gender_map.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=gender_map,
        schema_name='data_dict',
        table_name='gender_map',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Household income map
    ## Fetch data from local
    household_income_map = pd.read_pickle(os.path.join(
        directory,
        'household_income_map.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=household_income_map,
        schema_name='data_dict',
        table_name='household_income_map',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # NPS map
    ## Fetch data from local
    nps_map = pd.read_pickle(os.path.join(
        directory,
        'nps_map.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=nps_map,
        schema_name='data_dict',
        table_name='nps_map',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Boolean map
    ## Fetch data from local
    boolean_map = pd.read_pickle(os.path.join(
        directory,
        'boolean_map.pkl'
    ))
    ## Insert data into Postgres student database
    pg_client.insert_dataframe(
        dataframe=boolean_map,
        schema_name='data_dict',
        table_name='boolean_map',
        conn=conn,
        progress_bar=progress_bar,
        notebook=notebook
    )
    # Commit changes and close connection
    conn.commit()
    conn.close()

