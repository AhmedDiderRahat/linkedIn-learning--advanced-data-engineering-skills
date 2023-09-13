import pandas as pd

from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

from airflow.models import Variable

from random import choice

default_args = {
    'owner': 'adrahat'
}

DATASET_PATH = '/home/adrahat/airflow/datasets/insurance.csv'

OUTPUT_PATH = '/home/adrahat/airflow/output/{0}.csv'

def read_csv():
    df = pd.read_csv(DATASET_PATH)

    print(df)

    return df.to_json()

def remove_null_values(ti):

    json_data = ti.xcom_pull(task_ids = 'read_csv')

    df = pd.read_json(json_data)

    df = df.dropna()

    print(df)

    return df.to_json()

def determine_branch():
    transform_action = Variable.get("transform_action", default_var = None)

    print("Transform Action: ", transform_action)

    if transform_action.startswith('filter'):
        return transform_action
    elif transform_action == 'group_by_region_smoker':
        return 'group_by_region_smoker'



def filter_by_southwest(ti):
    json_data = ti.xcom_pull(task_ids = 'remove_null_values')

    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southwest']

    region_df.to_csv(OUTPUT_PATH.format('southwest'), index = False)


def filter_by_southeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')

    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'southeast']

    region_df.to_csv(OUTPUT_PATH.format('southeast'), index=False)


def filter_by_northwest(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')

    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northwest']

    region_df.to_csv(OUTPUT_PATH.format('northwest'), index=False)

def filter_by_northeast(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')

    df = pd.read_json(json_data)

    region_df = df[df['region'] == 'northeast']

    region_df.to_csv(OUTPUT_PATH.format('northeast'), index=False)

def group_by_region_smoker(ti):
    json_data = ti.xcom_pull(task_ids='remove_null_values')

    df = pd.read_json(json_data)

    region_df = df.groupby('region').agg({
        'age' : 'mean',
        'bmi': 'mean',
        'charges': 'mean',
    }).reset_index()

    region_df.to_csv(OUTPUT_PATH.format('group_by_region'), index = False)

    smoker_df = df.groupby('smoker').agg({
        'age': 'mean',
        'bmi': 'mean',
        'charges': 'mean',
    }).reset_index()

    smoker_df.to_csv(OUTPUT_PATH.format('group_by_smoker'), index=False)

with DAG(
        dag_id='executing_branching_2',
        description='Running a branching pipeline',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['brancing', 'condition', 'pipeline', 'python']
) as dag:

    read_csv = PythonOperator(
        task_id = 'read_csv',

        python_callable = read_csv
    )

    remove_null_values = PythonOperator(
        task_id = 'remove_null_values',

        python_callable = remove_null_values
    )

    determine_branch = BranchPythonOperator(
        task_id='determine_branch',

        python_callable=determine_branch
    )

    filter_by_northeast = PythonOperator(
        task_id='filter_by_northeast',

        python_callable=filter_by_northeast
    )

    filter_by_northwest = PythonOperator(
        task_id='filter_by_northwest',

        python_callable=filter_by_northwest
    )

    filter_by_southwest = PythonOperator(
        task_id='filter_by_southwest',

        python_callable=filter_by_southwest
    )

    filter_by_southeast = PythonOperator(
        task_id='filter_by_southeast',

        python_callable=filter_by_southeast
    )

    group_by_region_smoker = PythonOperator(
        task_id='group_by_region_smoker',

        python_callable=group_by_region_smoker
    )


read_csv >> remove_null_values >> determine_branch >> [filter_by_northeast,
                                                       filter_by_northwest,
                                                       filter_by_southeast,
                                                       filter_by_southwest,
                                                       group_by_region_smoker]