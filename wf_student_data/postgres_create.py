import psycopg2
import os
import logging

logger = logging.getLogger(__name__)

SCHEMA="""
CREATE SCHEMA tc;

CREATE TABLE tc.schools (
    school_id   integer,
    name        text,
    address     text,
    phone       text,
    time_zone   text,
    PRIMARY KEY (school_id)
);

CREATE TABLE tc.sessions (
    school_id   integer,
    session_id  integer,
    name        text,
    start_date  date,
    stop_date   date,
    current     boolean,
    inactive    boolean,
    children    integer,
    PRIMARY KEY (school_id, session_id),
    FOREIGN KEY (school_id) REFERENCES tc.schools(school_id)
);

CREATE TABLE tc.classrooms (
    school_id       integer,
    classroom_id    integer,
    name            text,
    lesson_set_id   int,
    level           text,
    active          boolean,
    PRIMARY KEY (school_id, classroom_id),
    FOREIGN KEY (school_id) REFERENCES tc.schools(school_id)
);

CREATE TABLE tc.users (
    school_id   integer,
    user_id     integer,
    first_name  text,
    last_name   text,
    email       text,
    roles       text[],
    inactive    boolean,
    type        text,
    PRIMARY KEY (school_id, user_id),
    FOREIGN KEY (school_id) REFERENCES tc.schools(school_id)
);

CREATE TABLE tc.children (
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
    PRIMARY KEY (school_id, child_id),
    FOREIGN KEY (school_id) REFERENCES tc.schools(school_id)
);

CREATE TABLE tc.children_parents (
    school_id   integer,
    child_id    integer,
    parent_id   integer,
    PRIMARY KEY (school_id, child_id, parent_id),
    FOREIGN KEY (school_id, child_id) REFERENCES tc.children(school_id, child_id),
    FOREIGN KEY (school_id, parent_id) REFERENCES tc.users(school_id, user_id)
);

CREATE TABLE tc.classrooms_children (
    school_id       integer,
    session_id      integer,
    classroom_id    integer,
    child_id        integer,
    PRIMARY KEY (school_id, session_id, classroom_id, child_id),
    FOREIGN KEY (school_id, session_id) REFERENCES tc.sessions(school_id, session_id),
    FOREIGN KEY (school_id, classroom_id) REFERENCES tc.classrooms(school_id, classroom_id),
    FOREIGN KEY (school_id, child_id) REFERENCES tc.children(school_id, child_id)
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
    conn = psycopg2.connect(**connect_kwargs)
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # Create student database
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
    with psycopg2.connect(**connect_kwargs) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA)