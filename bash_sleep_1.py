#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


with DAG(
    dag_id='bash_sleep_1',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    params={
        "model_run": Param("all", type="string"),
    },
    catchup=False
) as dag:

    start_dummy = DummyOperator(task_id="start")

    test_var = '{{params.model_run}}'

    # test all sources
    t2 = BashOperator(
        task_id="t2",
        bash_command="echo ############# {{params.model_run}}",
            dag=dag,
    )
    end_dummy = DummyOperator(task_id="end")

    """
        trigger_sleep_3 = TriggerDagRunOperator(
            task_id="trigger_sleep_3",
            trigger_dag_id="bash_sleep_3",
            wait_for_completion=True,
            dag=dag,
        )
    """

start_dummy >> t2 >> end_dummy


