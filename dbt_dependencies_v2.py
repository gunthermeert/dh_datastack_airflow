"""
Shows how to parse a dbt manifest file to "explode" the dbt DAG into Airflow

Each dbt model is run as a bash command.
https://github.com/astronomer/airflow-dbt-demo
"""

from pendulum import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from include.dbt_dag_parser import DbtDagParser

# We're hardcoding these values here for the purpose of the demo, but in a production environment these
# would probably come from a config file and/or environment variables!
DBT_PROJECT_DIR = "/home/gunther/dh_datastack_dbt/dh_datastack"
DBT_PROFILES_DIR = "/home/gunther/dh_datastack_dbt/.dbt"
DBT_GLOBAL_CLI_FLAGS = "--no-write-json"
DBT_TARGET = "dev"
DBT_TAG = "tag_staging"


with DAG(
    dag_id="dbt_dependencies_v2",
    start_date=datetime(2020, 12, 23),
    description="A dbt wrapper for Airflow using a utility class to map the dbt DAG to Airflow tasks",
    schedule_interval=None,
    catchup=False,
    doc_md=__doc__
) as dag:

    start_dummy = DummyOperator(task_id="start")
    # We're using the dbt seed command here to populate the database for the purpose of this demo
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=(
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt {DBT_GLOBAL_CLI_FLAGS} seed
            """
        )
    )

    # We're using the dbt seed command here to populate the database for the purpose of this demo
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"""
            cd {DBT_PROJECT_DIR} &&
            dbt {DBT_GLOBAL_CLI_FLAGS} snapshot
            """
        )
    )

    end_dummy = DummyOperator(task_id="end")

    # The parser parses out a dbt manifest.json file and dynamically creates tasks for "dbt run" and "dbt test"
    # commands for each individual model. It groups them into task groups which we can retrieve and use in the DAG.
    dag_parser = DbtDagParser(
        dbt_global_cli_flags=DBT_GLOBAL_CLI_FLAGS,
        dbt_project_dir=DBT_PROJECT_DIR,
        dbt_profiles_dir=DBT_PROFILES_DIR,
        dbt_target=DBT_TARGET,
    )

    dbt_run_group = dag_parser.get_dbt_run_group()
    dbt_test_group = dag_parser.get_dbt_test_group()

    start_dummy >> dbt_seed >> dbt_snapshot >> dbt_run_group >> dbt_test_group >> end_dummy

