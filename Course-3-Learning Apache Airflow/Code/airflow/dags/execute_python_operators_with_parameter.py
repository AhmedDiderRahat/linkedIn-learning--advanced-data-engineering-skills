from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
	'owner' : 'adrahat'
}

def greet_hello(name):
	print("Hello, {name}!".format(name = name))

def greet_hello_with_city(name, city):
	print("Hello, {name} from {city}!".format(name = name, city = city))


with DAG(
	dag_id = 'execute_python_operators_with_parameter',
	description = 'Parameterize Python Operators in DAGs',
        default_args = default_args,
        start_date = days_ago(1),
        schedule_interval = timedelta(days=1),
        tags = ['parameter', 'python']
) as dag:

	taskA = PythonOperator(
		task_id = 'taskA',
		python_callable = greet_hello,
		op_kwargs = {'name' : 'Rahat'}
	)

	taskB = PythonOperator(
		task_id = 'taskB',
		python_callable = greet_hello_with_city,
		op_kwargs = {'name' : 'Rahat', 'city' : 'Sylhet'}
	)

taskA >> taskB
