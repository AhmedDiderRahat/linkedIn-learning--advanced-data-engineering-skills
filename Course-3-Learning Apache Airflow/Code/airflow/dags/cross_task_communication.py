from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG

from airflow.operators.python import PythonOperator

default_args = {
        'owner' : 'adrahat'
}

def incr_by_1(counter):
	print("Count {counter}!".format(counter = counter))

	return counter + 1

def mul_by_100(counter):
	print("Count {counter}!".format(counter = counter))

	return counter * 100


with DAG(
        dag_id = 'cross_task_communication',
        description = 'Parameterize Python Operators in DAGs',
        default_args = default_args,
        start_date = days_ago(1),
        schedule_interval = timedelta(days=1),
        tags = ['xcom', 'python']
) as dag:

	taskA = PythonOperator(
		task_id = 'taskA',
		python_callable = incr_by_1,
		op_kwargs = {'counter' : 100}
	)

	taskB = PythonOperator(
		task_id = 'taskB',
		python_callable = mul_by_100,
		op_kwargs = {'counter' : 9}
	)

taskA >> taskB
