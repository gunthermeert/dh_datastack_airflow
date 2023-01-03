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
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR_MARKETING') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev
DBT_MANIFEST_FILEPATH = "/home/gunther/dh_datastack_dbt/dh_datastack_marketing/target/manifest.json"
DBT_MODEL_RUN = "all" #"model.dh_datastack.daily_product_sales" #"model.dh_datastack.int_finance__product_sales" #"model.dh_datastack.stg_dh_shop__customers"

with DAG(
    dag_id='test_dh_customer_freshness',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds an airflow dag dynamically by reading manifest',
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    catchup=False
) as dag:
    start_dummy = DummyOperator(task_id="start")

    # test all sources
    dbt_customer_freshness_check = BashOperator(
        task_id="dbt_source_test",
        bash_command=
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt source freshness --select source:dh_datastack_mdm.CUSTOMERS
            """,
            dag=dag,
    )

    end_dummy = DummyOperator(task_id="end")

    start_dummy >> dbt_customer_freshness_check >> end_dummy