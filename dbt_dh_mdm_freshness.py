from airflow import DAG
from airflow.utils.dates import datetime
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='dbt_dh_mdm_freshness',
    description='DAG that is able to check the freshness and retrigger MDM loads',
    start_date=datetime(2022, 1, 1),
    catchup=False,
        params={
            "freshness_hours": "1",
            "freshness_table": "CUSTOMERS",
            "freshness_table_lowercase": "customers"
        },
    max_active_runs=1
) as dag:
    start_dummy = DummyOperator(task_id="start")

    freshness_check = BashOperator(
        task_id="freshness_check",
        bash_command="cd /home/gunther/dh_datastack_dbt/dh_datastack_mdm && dbt source freshness --select source:mdm_freshness_{{ params.freshness_hours }}_hour.{{ params.freshness_table }}",
        dag=dag,
    )

    refresh_trigger = TriggerDagRunOperator(
        task_id="refresh_trigger",
        trigger_dag_id="refresh_dh_datastack_mdm_{{ params.freshness_table_lowercase }}",
        wait_for_completion=True,
        trigger_rule='all_failed',  # only if the first freshness task failed
        dag=dag,
    )


    end_dummy = DummyOperator(task_id="end", trigger_rule="one_success")

    start_dummy >> freshness_check >> end_dummy
    start_dummy >> freshness_check >> refresh_trigger >> end_dummy