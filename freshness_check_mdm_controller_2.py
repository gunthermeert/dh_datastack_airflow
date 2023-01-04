from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.param import Param




with DAG(
    dag_id='freshness_check_mdm_controller_2',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds an airflow dag dynamically by reading manifest',
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    catchup=False
) as dag:
    start_dummy = DummyOperator(task_id="start")


    trigger = TriggerDagRunOperator(
        task_id="refresh_trigger",
        trigger_dag_id="freshness_check_mdm",
        conf={'freshness_hours': '6'},
        wait_for_completion=True,
        dag=dag,
    )


    end_dummy = DummyOperator(task_id="end", trigger_rule="one_success")

    start_dummy >> trigger >> end_dummy
