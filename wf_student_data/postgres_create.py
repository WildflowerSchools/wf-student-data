import psycopg2
import os
import logging

logger = logging.getLogger(__name__)

SCHEMA="""
CREATE SCHEMA tc;

CREATE TABLE tc.updates (
    update_id       SERIAL,
    update_start    timestamp with time zone,
    update_end      timestamp with time zone,
    PRIMARY KEY (update_id)
);

CREATE TABLE tc.schools (
    update_id   integer,
    school_id   integer,
    name        text,
    address     text,
    phone       text,
    time_zone   text,
    PRIMARY KEY (update_id, school_id),
    FOREIGN KEY (update_id) REFERENCES tc.updates(update_id)
);

CREATE TABLE tc.sessions (
    update_id   integer,
    school_id   integer,
    session_id  integer,
    name        text,
    start_date  date,
    stop_date   date,
    current     boolean,
    inactive    boolean,
    children    integer,
    PRIMARY KEY (update_id, school_id, session_id),
    FOREIGN KEY (update_id) REFERENCES tc.updates(update_id),
    FOREIGN KEY (update_id, school_id) REFERENCES tc.schools(update_id, school_id)
);

CREATE TABLE tc.classrooms (
    update_id       integer,
    school_id       integer,
    classroom_id    integer,
    name            text,
    lesson_set_id   int,
    level           text,
    active          boolean,
    PRIMARY KEY (update_id, school_id, classroom_id),
    FOREIGN KEY (update_id) REFERENCES tc.updates(update_id),
    FOREIGN KEY (update_id, school_id) REFERENCES tc.schools(update_id, school_id)
);

CREATE TABLE tc.users (
    update_id   integer,
    school_id   integer,
    user_id     integer,
    first_name  text,
    last_name   text,
    email       text,
    roles       text[],
    inactive    boolean,
    type        text,
    PRIMARY KEY (update_id, school_id, user_id),
    FOREIGN KEY (update_id) REFERENCES tc.updates(update_id),
    FOREIGN KEY (update_id, school_id) REFERENCES tc.schools(update_id, school_id)
);

CREATE TABLE tc.children (
    update_id           integer,
    school_id           integer,
    child_id            integer,
    first_name          text,
    middle_name         text,
    last_name           text,
    birth_date          date,
    gender              text,
    dominant_language   text,
    ethnicity           text[],
    household_income    text,
    student_id          text,
    grade               text,
    program             text,
    first_day           date,
    last_day            date,
    exit_reason         text,
    current_child       boolean,
    PRIMARY KEY (update_id, school_id, child_id),
    FOREIGN KEY (update_id) REFERENCES tc.updates(update_id),
    FOREIGN KEY (update_id, school_id) REFERENCES tc.schools(update_id, school_id)
);

CREATE TABLE tc.children_parents (
    update_id   integer,
    school_id   integer,
    child_id    integer,
    parent_id   integer,
    PRIMARY KEY (update_id, school_id, child_id, parent_id),
    FOREIGN KEY (update_id) REFERENCES tc.updates(update_id),
    FOREIGN KEY (update_id, school_id, child_id) REFERENCES tc.children(update_id, school_id, child_id),
    FOREIGN KEY (update_id, school_id, parent_id) REFERENCES tc.users(update_id, school_id, user_id)
);

CREATE TABLE tc.classrooms_children (
    update_id       integer,
    school_id       integer,
    session_id      integer,
    classroom_id    integer,
    child_id        integer,
    PRIMARY KEY (update_id, school_id, session_id, classroom_id, child_id),
    FOREIGN KEY (update_id) REFERENCES tc.updates(update_id),
    FOREIGN KEY (update_id, school_id, session_id) REFERENCES tc.sessions(update_id, school_id, session_id),
    FOREIGN KEY (update_id, school_id, classroom_id) REFERENCES tc.classrooms(update_id, school_id, classroom_id),
    FOREIGN KEY (update_id, school_id, child_id) REFERENCES tc.children(update_id, school_id, child_id)
);

CREATE SCHEMA data_dict;

CREATE TABLE data_dict.ethnicity_categories (
    ethnicity_category              text,
    ethnicity_display_name_english  text,
    ethnicity_display_name_spanish  text,
    PRIMARY KEY (ethnicity_category)
);

CREATE TABLE data_dict.gender_categories (
    gender_category              text,
    gender_display_name_english  text,
    gender_display_name_spanish  text,
    PRIMARY KEY (gender_category)
);

CREATE TABLE data_dict.household_income_categories (
    household_income_category              text,
    household_income_display_name_english  text,
    household_income_display_name_spanish  text,
    PRIMARY KEY (household_income_category)
);

CREATE TABLE data_dict.nps_categories (
    nps_category              text,
    nps_display_name_english  text,
    nps_display_name_spanish  text,
    PRIMARY KEY (nps_category)
);

CREATE TABLE data_dict.boolean_categories (
    boolean_category              boolean,
    boolean_display_name_english  text,
    boolean_display_name_spanish  text,
    PRIMARY KEY (boolean_category)
);

CREATE TABLE data_dict.ethnicity_map (
    ethnicity_map_id    SERIAL,
    ethnicity_response  text,
    ethnicity_category  text,
    PRIMARY KEY (ethnicity_map_id),
    FOREIGN KEY (ethnicity_category) REFERENCES data_dict.ethnicity_categories(ethnicity_category)
);

CREATE TABLE data_dict.gender_map (
    gender_response  text,
    gender_category  text,
    PRIMARY KEY (gender_response),
    FOREIGN KEY (gender_category) REFERENCES data_dict.gender_categories(gender_category)
);

CREATE TABLE data_dict.household_income_map (
    household_income_response  text,
    household_income_category  text,
    PRIMARY KEY (household_income_response),
    FOREIGN KEY (household_income_category) REFERENCES data_dict.household_income_categories(household_income_category)
);

CREATE TABLE data_dict.nps_map (
    nps_response  integer,
    nps_category  text,
    PRIMARY KEY (nps_response),
    FOREIGN KEY (nps_category) REFERENCES data_dict.nps_categories(nps_category)
);

CREATE TABLE data_dict.boolean_map (
    boolean_response  text,
    boolean_category  boolean,
    PRIMARY KEY (boolean_response),
    FOREIGN KEY (boolean_category) REFERENCES data_dict.boolean_categories(boolean_category)
);

"""

def create_student_database(
    default_dbname=None,
    student_database_dbname=None,
    user=None,
    password=None,
    host=None,
    port=None
):
    # Read connection specifications from environment variables if not explicitly specified
    if default_dbname is None:
        default_dbname = os.getenv('POSTGRES_DEFAULT_DBNAME')
    if student_database_dbname is None:
        student_database_dbname = os.getenv('STUDENT_DATABASE_DBNAME')
    if user is None:
        user = os.getenv('STUDENT_DATABASE_USER')
    if password is None:
        password = os.getenv('STUDENT_DATABASE_PASSWORD')
    if host is None:
        host = os.getenv('STUDENT_DATABASE_HOST')
    if port is None:
        port = os.getenv('STUDENT_DATABASE_PORT')
    # Populate connection arguments, leaving out any not specified (so psycopg2 reverts to default)
    connect_kwargs = dict()
    if default_dbname is not None:
        connect_kwargs['dbname'] = default_dbname
    if user is not None:
        connect_kwargs['user'] = user
    if password is not None:
        connect_kwargs['password'] = password
    if host is not None:
        connect_kwargs['host'] = host
    if port is not None:
        connect_kwargs['port'] = port
    # Connect to default database (because student database does not exist yet)
    logger.info('Connecting to default database with connection specifications {}'.format(connect_kwargs))
    conn = psycopg2.connect(**connect_kwargs)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # Create student database
    logger.info('Creating student database')
    cur = conn.cursor()
    sql_object = psycopg2.sql.SQL("CREATE DATABASE {student_database_dbname};").format(
        student_database_dbname=psycopg2.sql.Identifier(student_database_dbname)
    )
    cur.execute(sql_object)
    cur.close()
    # Close connection to default database
    conn.close()
    # Connect to student database and create schemas and tables
    connect_kwargs['dbname'] = student_database_dbname
    logger.info('Connecting to student database with connection specifications {}'.format(connect_kwargs))
    with psycopg2.connect(**connect_kwargs) as conn:
        logger.info('Creating schemas and tables within student database')
        with conn.cursor() as cur:
            cur.execute(SCHEMA)