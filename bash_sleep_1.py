#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.param import Param
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from include.dbt_group_parser_v2 import DbtDagParser

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev


def modify_dro(context, dagrun_order):
    print(context)
    print(dagrun_order)
    dagrun_order.payload = {
        "message": "This is my conf message"
    }
    return dagrun_order

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

    model_run = '{{params.model_run}}'
    owner = "{{ dag_run.conf['owner'] }}"

    # test all sources
    t2 = BashOperator(
        task_id="t2",
        bash_command="echo ############# {{params.model_run}}",
            dag=dag,
    )


    run_this = TriggerDagRunOperator(
        task_id='run_this',
        trigger_dag_id='bash_sleep_3',
        op_kwargs={'model_run': '{{params.model_run}}'},
        dag=dag
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

start_dummy >> t2 >> run_this >> end_dummy


