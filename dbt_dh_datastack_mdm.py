import os
from airflow import DAG
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime
from include.dbt_dag_parser import DbtDagParser
from airflow.decorators import dag, task

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR_MDM') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev
DBT_MANIFEST_FILEPATH = "/home/gunther/dh_datastack_dbt/dh_datastack_mdm/target/manifest.json"
DBT_MODEL_RUN = "all" #"model.dh_datastack.daily_product_sales" #"model.dh_datastack.int_finance__product_sales" #"model.dh_datastack.stg_dh_shop__customers"

with DAG(
    dag_id='dbt_dh_datastack_mdm',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds an airflow dag dynamically by reading manifest',
    schedule_interval="0 10 * * *",
    max_active_runs=1,
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

    # The parser parses out a dbt manifest.json file and dynamically creates tasks for "dbt run", "dbt snapshot", "dbt seed" and "dbt test"
    # commands for each individual model. It groups them into task groups which we can retrieve and use in the DAG.
    dag_parser = DbtDagParser(
        dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
        dbt_target=DBT_TARGET,
        dbt_manifest_filepath=DBT_MANIFEST_FILEPATH,
        #dbt_model_run=Variable.get("MODEL_RUN_VAR", "{{params.model_run}}") #or fill in manual with a model name #resource_type.project.model_name #"model.dh_datastack.daily_product_sales"
        dbt_model_run=DBT_MODEL_RUN,
    )

    dbt_run_group = dag_parser.get_dbt_run_group()

    end_dummy = DummyOperator(task_id="end")

    start_dummy >> dbt_update_packages >> dbt_source_test >> dbt_run_group >> end_dummy