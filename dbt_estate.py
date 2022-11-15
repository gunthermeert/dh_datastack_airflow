#https://docs.astronomer.io/learn/airflow-dbt
import datetime
import json

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import datetime
from airflow.utils.dates import timedelta


dag = DAG(
    dag_id='dbt_estate',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    catchup=False
)

def load_manifest():
    local_filepath = "/home/gunther/dh_datastack_dbt/dh_datastack/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)

    return data

def make_dbt_task(node, dbt_verb):
    """Returns an Airflow operator either run and test an individual model"""
    DBT_DIR = "/home/gunther/dh_datastack_dbt/dh_datastack"
    GLOBAL_CLI_FLAGS = "--no-write-json"
    model = node.split(".")[-1]

    if dbt_verb == "run":
        dbt_task = BashOperator(
            task_id=node,
            bash_command=f"""
            cd {DBT_DIR} &&
            echo DBT_PROFILES DIR: $DBT_PROFILES_DIR &&
            dbt debug --config-dir &&
            echo welke folder? &&
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            dag=dag,
        )

    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            task_id=node_test,
            bash_command=f"""
            cd {DBT_DIR} &&
            dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --target dev --models {model}
            """,
            dag=dag,
        )

        #dbt {GLOBAL_CLI_FLAGS} {dbt_verb} --profiles-dir /home/gunther/dbt-projects/.dbt --target dev --models {model}

    return dbt_task

data = load_manifest()

dbt_tasks = {}
for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")

        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":

        # Set dependency to run tests on a model after model runs finishes
        node_test = node.replace("model", "test")
        dbt_tasks[node] >> dbt_tasks[node_test]

        # Set all model -> model dependencies
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:

            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_tasks[upstream_node] >> dbt_tasks[node]

