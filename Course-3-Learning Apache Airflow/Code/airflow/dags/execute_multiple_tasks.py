from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.bash import BashOperator

default_args = {
        'owner' : 'adrahat'
}

with DAG(
        dag_id = 'execute_multiple_tasks',
        description = 'DAG with multiple tasks and dependencies',
        default_args = default_args,
        start_date = days_ago(1),
        schedule_interval = '@once'
) as dag:
	taskA = BashOperator(
		task_id = 'taskA',
		bash_command = 'echo task A has Executed!'
        )

	taskB = BashOperator(
		task_id = 'taskB',
		bash_command = 'echo task B has Executed!'
        )

taskA.set_downstream(taskB) 
