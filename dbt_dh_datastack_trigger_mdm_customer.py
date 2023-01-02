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
    dag_id='dbt_dh_datastack_trigger_mdm_customer',
    start_date=datetime(2022, 11, 7),
    description='dbt dag to trigger mdm customer',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
) as dag:

    start_dummy = DummyOperator(task_id="start")

    trigger_mdm_customer = TriggerDagRunOperator(
        task_id="trigger_mdm_customer",
        trigger_dag_id="dbt_dh_customer_freshness",
        wait_for_completion=True,
        dag=dag,
    )

    end_dummy = DummyOperator(task_id="end")

start_dummy >> trigger_mdm_customer >> end_dummy


