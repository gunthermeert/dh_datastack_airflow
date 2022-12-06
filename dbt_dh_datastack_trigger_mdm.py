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
    dag_id='dbt_dh_datastack_trigger_mdm',
    start_date=datetime(2022, 11, 7),
    description='dbt dag to trigger mdm',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
) as dag:

    start_dummy = DummyOperator(task_id="start")

    trigger_mdm = TriggerDagRunOperator(
        task_id="trigger_mdm",
        trigger_dag_id="dbt_dh_datastack_mdm",
        wait_for_completion=True,
        dag=dag,
    )

    end_dummy = DummyOperator(task_id="end")

start_dummy >> trigger_mdm >> end_dummy


