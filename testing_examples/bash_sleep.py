#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    dag_id='bash_sleep',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:

    sleep_1 = BashOperator(
        task_id='sleep_1',
        bash_command='sleep 5s',
    )

    sleep_2 = BashOperator(
        task_id='sleep_2',
        bash_command='sleep 5s',
    )

    sleep_3 = BashOperator(
        task_id='sleep_3',
        bash_command='sleep 20s',
    )

    sleep_7 = BashOperator(
        task_id='sleep_7',
        bash_command='sleep 5s',
    )
"""
    sleep_4 = BashOperator(
        task_id='sleep_4',
        bash_command='sleep 5s',
    )

    sleep_5 = BashOperator(
        task_id='sleep_5',
        bash_command='sleep 5s',
    )

    sleep_6 = BashOperator(
        task_id='sleep_6',
        bash_command='sleep 5s',
    )
"""

#sleep_1 >> sleep_6 >> sleep_7
#sleep_1 >> sleep_2 >> [sleep_3, sleep_4] >> sleep_5 >> sleep_7

sleep_1 >> sleep_2 >> sleep_7
sleep_3 >> sleep_7

