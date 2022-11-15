from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    dag_id='dbt_dependencies',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test'
    )

    dbt_run >> dbt_test

