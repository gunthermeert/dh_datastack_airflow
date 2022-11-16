#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    dag_id='bash_sleep_1',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:

    start_dummy = DummyOperator(task_id="start")

    wait_for = ExternalTaskSensor(
        task_id="wait_for",
        external_dag_id="bash_sleep_3",
        external_task_id="end",
        poke_interval=60,
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule",
    )

    trigger_sleep_3 = TriggerDagRunOperator(
        task_id="trigger_sleep_3",
        trigger_dag_id="bash_sleep_3",
        wait_for_completion=True,
        dag=dag,
    )

    end_dummy = DummyOperator(task_id="end")

start_dummy >> wait_for >> trigger_sleep_3 >> end_dummy


