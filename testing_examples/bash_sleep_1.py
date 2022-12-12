#https://docs.astronomer.io/learn/airflow-dbt
import time
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.param import Param
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import datetime


def set_run_model_var(**kwargs):
    Variable.set("MODEL_RUN_VAR", kwargs["model_run"])
    time.sleep(10) #otherwise the triggered dag might get some complications
    print("#######", kwargs["model_run"])

with DAG(
    dag_id='bash_sleep_1',
    start_date=datetime(2022, 11, 7),
    description='dbt dag for atlas estate',
    schedule_interval="0 10 * * *",
    params={
        "model_run": Param("all", type="string"),
    },
    catchup=False
) as dag:

    start_dummy = DummyOperator(task_id="start")

    set_var = PythonOperator(
        task_id='set_model_var',
        provide_context=True,
        python_callable=set_run_model_var,
        op_kwargs={'model_run': '{{params.model_run}}'},
        dag=dag,
    )

    dynamic_dbt_run = TriggerDagRunOperator(
        task_id='dbt_run',
        trigger_dag_id='bash_sleep_3',
        wait_for_completion=True,
        dag=dag
    )

    end_dummy = DummyOperator(task_id="end")

    """
        trigger_sleep_3 = TriggerDagRunOperator(
            task_id="trigger_sleep_3",
            trigger_dag_id="bash_sleep_3",
            wait_for_completion=True,
            dag=dag,
        )
    """

    """
        # test all sources
    t2 = BashOperator(
        task_id="t2",
        bash_command="echo ############# {{params.model_run}}",
            dag=dag,
    )
    """

start_dummy >> set_var >> dynamic_dbt_run >> end_dummy


