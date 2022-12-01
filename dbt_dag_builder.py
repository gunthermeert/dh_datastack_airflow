import os
from airflow import DAG
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime
from include.dbt_group_parser_v2 import DbtDagParser
from airflow.decorators import dag, task

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev

def set_model_env(model_run):
    os.environ['DBT_RUN_MODEL'] = model_run
    return os.getenv('DBT_RUN_MODEL')

with DAG(
    dag_id='dbt_dag_builder',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds by reading manifest',
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    params={
        "model_run": Param("all", type="string"),
    },
    catchup=False
) as dag:
    start_dummy = DummyOperator(task_id="start")

    #update deps
    dbt_update_packages = BashOperator(
        task_id="dbt_update_packages",
        bash_command=
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt deps
            """,
            dag=dag,
    )

    # test all sources
    dbt_source_test = BashOperator(
        task_id="dbt_source_test",
        bash_command=
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt {DBT_GLOBAL_CLI_FLAGS} test --select source:* 
            """,
            dag=dag,
    )

    t1 = PythonOperator(
        task_id='set_model_run',
        python_callable=set_model_env,
        op_kwargs={"model_run": "{{params.model_run}}"},
        dag=dag,
    )

    # The parser parses out a dbt manifest.json file and dynamically creates tasks for "dbt run", "dbt snapshot", "dbt seed" and "dbt test"
    # commands for each individual model. It groups them into task groups which we can retrieve and use in the DAG.
    dag_parser = DbtDagParser(
        dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
        dbt_target=DBT_TARGET,
        dbt_model_run="stg_dh_shop__customers"
    )

    dbt_run_group = dag_parser.get_dbt_run_group()

    end_dummy = DummyOperator(task_id="end")

    start_dummy >> dbt_update_packages >> dbt_source_test >> t1 >> dbt_run_group >> end_dummy