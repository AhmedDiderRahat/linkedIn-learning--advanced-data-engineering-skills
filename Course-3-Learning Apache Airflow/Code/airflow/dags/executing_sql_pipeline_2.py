from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.sqlite_operator import SqliteOperator

default_args = {
    'owner': 'adrahat'
}

with DAG(
        dag_id='executing_sql_pipeline_2',
        description='Pipeline using SQL operators',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['pipeline', 'sql', 'DML', 'DDL']
) as dag:

    create_table = SqliteOperator(
        task_id = 'create_table',

        sql = r"""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50) NOT NULL,
                age INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT true,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """,

        sqlite_conn_id = 'my_test_sqlite_connection',
        dag = dag
    )

    insert_values1 = SqliteOperator(
        task_id = 'insert_values1',

        sql = r"""
            INSERT INTO users (name, age, is_active) VALUES
                ('Julia', 30, false),
                ('Peter', 55, false),
                ('Emily', 37, false),
                ('Katrina', 54, false),
                ('Joseph', 27, true)
        """,
        sqlite_conn_id='my_test_sqlite_connection',
        dag=dag
    )

    insert_values2 = SqliteOperator(
        task_id = 'insert_values2',

        sql = r"""
            INSERT INTO users (name, age) VALUES
                ('Harry', 49),
                ('Nancy', 52),
                ('Elvis', 26),
                ('Mia', 20)
        """,

        sqlite_conn_id = 'my_test_sqlite_connection',
        dag = dag
    )

    display_result = SqliteOperator(
        task_id = 'display_result',

        sql = r"""SELECT * FROM users""",

        sqlite_conn_id='my_test_sqlite_connection',
        dag=dag,

        do_xcom_push = True
    )

create_table >> [insert_values1, insert_values2] >> display_result