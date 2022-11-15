#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json
import pendulum


from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta
from include.dbt_group_parser import DbtDagParser

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/home/gunther/dh_datastack_dbt/dh_datastack"
DBT_PROFILES_DIR = "/home/gunther/dh_datastack_dbt/.dbt"
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = "dev"
DBT_TAG = "tag_staging"

with DAG(
    dag_id='read_manifest',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds by reading manifest',
    schedule_interval="0 10 * * *",
    catchup=False
) as dag:
    start_dummy = DummyOperator(task_id="start")

    # We're using the dbt seed command here to populate the database for the purpose of this demo
    dbt_source_test = BashOperator(
        task_id="dbt_source_test",
        bash_command=
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt {DBT_GLOBAL_CLI_FLAGS} test --select source:dh_datastack
            """,
            dag=dag,
    )

    # The parser parses out a dbt manifest.json file and dynamically creates tasks for "dbt run" and "dbt test"
    # commands for each individual model. It groups them into task groups which we can retrieve and use in the DAG.
    dag_parser = DbtDagParser(
        dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
        dbt_target=DBT_TARGET,
    )

    dbt_run_group = dag_parser.get_dbt_run_group()

    end_dummy = DummyOperator(task_id="end")

    start_dummy >> dbt_source_test >> dbt_run_group >> end_dummy

