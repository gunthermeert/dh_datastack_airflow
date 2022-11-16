#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    dag_id='bash_sleep_3',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:

    start_dummy = DummyOperator(task_id="start")

    sleep_3 = BashOperator(
        task_id='sleep_3',
        bash_command='sleep 10s',
    )

    end_dummy = DummyOperator(task_id="end")

start_dummy >> sleep_3 >> end_dummy


