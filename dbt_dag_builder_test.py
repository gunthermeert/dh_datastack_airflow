import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.param import Param
from airflow.utils.dates import datetime
from include.dbt_group_parser import DbtDagParser

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev

with DAG(
    dag_id='dbt_dag_builder_test',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds by parameters reading read manifest',
    schedule_interval="0 10 * * *",
    params={
        "model_run": Param("all", type="string"),
    },
    max_active_runs=1,
    catchup=False
) as dag:
    start_dummy = DummyOperator(task_id="start")

    #update deps
    print_param = BashOperator(
        task_id="print_param",
        bash_command=f"""echo dit is een parameter test: {{ params.model_run }} variable test: {DBT_PROJECT_DIR}""",
            dag=dag,
    )

    end_dummy = DummyOperator(task_id="end")

    start_dummy >> print_param >>  end_dummy