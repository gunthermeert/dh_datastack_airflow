import os
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime
from include.dbt_dag_parser import DbtDagParser
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = os.getenv('DBT_PROJECT_DIR_MARKETING') # DBT_PROJECT_DIR = /dh_datastack_dbt/dh_datastack
DBT_PROFILES_DIR = os.getenv('DBT_PROFILES_DIR') # DBT_PROFILES_DIR = /dh_datastack_dbt/.dbt
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = os.getenv('DBT_TARGET')# DBT_TARGET = dev
DBT_MANIFEST_FILEPATH = "/home/gunther/dh_datastack_dbt/dh_datastack_marketing/target/manifest.json"
DBT_MODEL_RUN = "model.dh_datastack_marketing.marketing_campaigns" #"all" choose a specific model or "all" to trigger dbt_dag_parser

with DAG(
    dag_id='dbt_dh_datastack_marketing_campaigns',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that runs dbt dbt_dh_datastack_marketing_campaigns model and the dependencies',
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

    # Start task group freshness checks
    with TaskGroup(group_id='freshness_checks_other_domains') as freshness_checks:
        freshness_check_mdm_customers = TriggerDagRunOperator(
            task_id="freshness_check_mdm_customers",
            trigger_dag_id="freshness_check_mdm",
            conf={'freshness_hours': '1', 'freshness_table': 'CUSTOMERS', 'freshness_table_lowercase': 'customers'},
            wait_for_completion=True,
            dag=dag,
        )
        freshness_check_mdm_products = TriggerDagRunOperator(
            task_id="freshness_check_mdm_products",
            trigger_dag_id="freshness_check_mdm",
            conf={'freshness_hours': '1', 'freshness_table': 'PRODUCTS', 'freshness_table_lowercase': 'products'},
            wait_for_completion=True,
            dag=dag,
        )

        freshness_check_finance_daily_product_sales = TriggerDagRunOperator(
            task_id="freshness_check_finance_daily_product_sales",
            trigger_dag_id="freshness_check_finance",
            conf={'freshness_hours': '1', 'freshness_table': 'DAILY_PRODUCT_SALES', 'freshness_table_lowercase': 'daily_product_sales'},
            wait_for_completion=True,
            dag=dag,
        )

    # End task group definition

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

    start_dummy >> dbt_source_test >> freshness_checks >> dbt_run_group >> end_dummy