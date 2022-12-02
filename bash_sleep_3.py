#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json
import os
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from include.dbt_group_parser_v2 import DbtDagParser

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev

"""
def get_run_model_var():
    model_run_var = Variable.get("MODEL_RUN_VAR", "{{params.model_run}}")

    return model_run_var
"""

with DAG(
    dag_id='bash_sleep_3',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    catchup=False,
    max_active_runs=1
) as dag:

    start_dummy = DummyOperator(task_id="start")

    # The parser parses out a dbt manifest.json file and dynamically creates tasks for "dbt run", "dbt snapshot", "dbt seed" and "dbt test"
    # commands for each individual model. It groups them into task groups which we can retrieve and use in the DAG.
    dag_parser = DbtDagParser(
        dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
        dbt_target=DBT_TARGET,
        dbt_model_run=Variable.get("MODEL_RUN_VAR", "{{params.model_run}}")
    )

    dbt_run_group = dag_parser.get_dbt_run_group()


    end_dummy = DummyOperator(task_id="end")

    Variable.set("MODEL_RUN_VAR", "all")

"""
    sleep_3 = BashOperator(
        task_id='sleep_3',
        bash_command='sleep 1m',
    )
"""
"""
get_var = PythonOperator(
    task_id='get_var',
    provide_context=True,
    python_callable=get_run_model_var,
    dag=dag,
)
"""
start_dummy >> dbt_run_group >> end_dummy


