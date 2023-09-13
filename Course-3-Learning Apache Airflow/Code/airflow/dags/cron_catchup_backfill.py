import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from random import choice


default_args = {
    'owner': 'adrahat'
}

def choose_branch():
    return choice([True, False])

def branch(ti):

    if ti.xcom_pull(task_ids = 'taskChoose'):
        return 'taskC'
    else:
        return 'taskD'

def task_c():
    print('Task C has Executed!')

with DAG(
        dag_id='cron_catchup_backfill',
        description='Using Cron- catchup and backfill',
        default_args=default_args,
        start_date=days_ago(5),
        schedule_interval='0 0 * * *',
        catchup = True,
        tags=['corn', 'catchup', 'backfill']
) as dag:

    taskA = BashOperator(
        task_id = 'taskA',
        bash_command = 'echo Task-A executed!'
    )

    taskChoose = PythonOperator(
        task_id = 'taskChoose',
        python_callable = choose_branch
    )

    taskBranch = BranchPythonOperator(
        task_id = 'taskBranch',
        python_callable = branch
    )

    taskC = PythonOperator(
        task_id = "taskC",
        python_callable = task_c
    )

    taskD = BashOperator(
        task_id = 'taskD',
        bash_command = 'echo Task-D has Executed!'
    )

    taskE = EmptyOperator(
        task_id = 'taskE',
    )

taskA >> taskChoose >> taskBranch >> [taskC, taskE]

taskC >> taskD