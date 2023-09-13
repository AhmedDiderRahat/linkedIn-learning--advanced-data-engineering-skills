from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
	'owner' : 'adrahat'
}

def print_function(): 
	print("The simpelest possible Python Operator!")

with DAG(
        dag_id = 'execute_python_operators',
        description = 'Python Operators in DAGs',
        default_args = default_args,
        start_date = days_ago(1),
        schedule_interval = timedelta(days=1),
	tags = ['simple', 'python']
) as dag:

	task = PythonOperator(
		task_id = 'python_tasks',
		python_callable = print_function
    	)

task
