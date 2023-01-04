import os
from airflow import DAG
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime
from include.poc_dbt_dag_parser import DbtDagParser
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR_MDM') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev
DBT_MANIFEST_FILEPATH = "/home/gunther/dh_datastack_dbt/dh_datastack_marketing/target/manifest.json"
DBT_MODEL_RUN = "model.dh_datastack_marketing.marketing_campaigns" #"model.dh_datastack_marketing.stg_dh_datastack_mdm__customers"

with DAG(
    dag_id='freshness_finance_customer',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds an airflow dag dynamically by reading manifest',
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    catchup=False
) as dag:
    start_dummy = DummyOperator(task_id="start")

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

    customer_freshness_check = BashOperator(
        task_id="customer_freshness_check",
        bash_command=f"""
        cd /home/gunther/dh_datastack_dbt/dh_datastack_mdm &&
        dbt source freshness --select source:mdm_finance_freshness.CUSTOMERS
        """,
        dag=dag,
    )

    customer_refresh_trigger = TriggerDagRunOperator(
        task_id="customer_refresh_trigger",
        trigger_dag_id="refresh_dh_datastack_mdm_customers",
        wait_for_completion=True,
        trigger_rule='all_failed',  # only if the first freshness task failed
        dag=dag,
    )

    customer_freshness_check_validation = BashOperator(
        task_id="customer_freshness_check_validation",
        bash_command=f"""
        cd /home/gunther/dh_datastack_dbt/dh_datastack_mdm &&
        dbt source freshness --select source:mdm_finance_freshness.CUSTOMERS
        """,
        dag=dag,
    )


    end_dummy = DummyOperator(task_id="end", trigger_rule="one_success")

    start_dummy >> customer_freshness_check >> end_dummy
    start_dummy >> customer_freshness_check >> customer_refresh_trigger >> customer_freshness_check_validation >> end_dummy