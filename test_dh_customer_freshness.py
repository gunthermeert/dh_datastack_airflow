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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

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

    # freshness check
    dbt_customer_freshness_check = BashOperator(
        task_id="dbt_customer_freshness_check",
        bash_command=
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt source freshness --select source:dh_datastack_mdm.CUSTOMERS
            """,
            dag=dag,
    )

    # refresh customer
    trigger_customer_refresh = TriggerDagRunOperator(
        task_id='trigger_customer_refresh',
        trigger_dag_id='dbt_dh_customer_freshness',
        wait_for_completion=True,
        trigger_rule='all_failed',
        dag=dag
    )

    # run model
    dbt_marketing_run_customer = BashOperator(
        task_id="dbt_marketing_run_customer",
        trigger_rule='one_success',
        bash_command=
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt --no-write-json run --target dev --select stg_dh_datastack_mdm__customers &&
            dbt --no-write-json test --target dev --select stg_dh_datastack_mdm__customers
            """,
            dag=dag,
    )

    end_dummy = DummyOperator(task_id="end")

    start_dummy >> dbt_customer_freshness_check >> dbt_marketing_run_customer >> end_dummy
    start_dummy >> dbt_customer_freshness_check >> trigger_customer_refresh >> dbt_marketing_run_customer >> end_dummy