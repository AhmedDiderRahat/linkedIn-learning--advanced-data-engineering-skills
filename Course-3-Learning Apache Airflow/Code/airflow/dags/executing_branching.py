from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from random import choice

default_args = {
    'owner': 'adrahat'
}

def has_driving_license():
    return choice([True, False])

def branch(ti):

    values = ti.xcom_pull(task_ids = 'has_driving_license')

    print("Randomly Choose: ", values)

    if values:
        return 'eligible_to_drive'
    else:
        return 'not_eligible_to_drive'

def eligible_to_drive():
    print("You Can drive, you have a licence!")

def not_eligible_to_drive():
    print("I am afraid you out of luck, you need a licence to drive!")


with DAG(
        dag_id='executing_branching',
        description='Running branching pipeline',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['brancing', 'condition']
) as dag:

    has_driving_license = PythonOperator(
        task_id = 'has_driving_license',

        python_callable = has_driving_license
    )

    branch = BranchPythonOperator(
        task_id = 'branch',

        python_callable = branch
    )

    eligible_to_drive = PythonOperator(
        task_id='eligible_to_drive',

        python_callable=eligible_to_drive
    )

    not_eligible_to_drive = PythonOperator(
        task_id='not_eligible_to_drive',

        python_callable=not_eligible_to_drive
    )

has_driving_license >> branch >> [eligible_to_drive, not_eligible_to_drive]