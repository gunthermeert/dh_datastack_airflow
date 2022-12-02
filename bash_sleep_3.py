#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    dag_id='bash_sleep_3',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    start_dummy = DummyOperator(task_id="start")


    def run_this_func(**context):
        print(context["dag_run"].conf)


    run_this = PythonOperator(
        task_id='run_this',
        provide_context=True,
        python_callable=run_this_func,
        dag=dag,
    )


    end_dummy = DummyOperator(task_id="end")

"""
    sleep_3 = BashOperator(
        task_id='sleep_3',
        bash_command='sleep 1m',
    )
"""

start_dummy >> run_this >> end_dummy


