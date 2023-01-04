from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id='freshness_check_mdm',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that builds an airflow dag dynamically by reading manifest',
    schedule_interval="0 10 * * *",
    max_active_runs=1,
    catchup=False
) as dag:
    start_dummy = DummyOperator(task_id="start")


    freshness_check = BashOperator(
        task_id="freshness_check",
        bash_command=f"""
        cd /home/gunther/dh_datastack_dbt/dh_datastack_mdm &&
        dbt source freshness --select source:mdm_freshness_1_hour.CUSTOMERS
        """,
        dag=dag,
    )

    refresh_trigger = TriggerDagRunOperator(
        task_id="refresh_trigger",
        trigger_dag_id="refresh_dh_datastack_mdm_customers",
        wait_for_completion=True,
        trigger_rule='all_failed',  # only if the first freshness task failed
        dag=dag,
    )

    freshness_check_validation = BashOperator(
        task_id="freshness_check_validation",
        bash_command=f"""
        cd /home/gunther/dh_datastack_dbt/dh_datastack_mdm &&
        dbt source freshness --select source:mdm_freshness_1_hour.CUSTOMERS
        """,
        dag=dag,
    )


    end_dummy = DummyOperator(task_id="end", trigger_rule="one_success")

    start_dummy >> freshness_check >> end_dummy
    start_dummy >> freshness_check >> refresh_trigger >> freshness_check_validation >> end_dummy