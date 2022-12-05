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
    dag_id='dbt_workflow',
    start_date=datetime(2022, 11, 7),
    description='dbt dag that by default runs all dbt models, otherwise via parameter MODEL_RUN_VAR',
    schedule_interval="0 10 * * *",
    max_active_runs=1,
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

    dbt_run_models = TriggerDagRunOperator(
        task_id='dbt_run_models',
        trigger_dag_id='dbt_dag_builder_lib',
        wait_for_completion=True,
        dag=dag
    )

    end_dummy = DummyOperator(task_id="end")


start_dummy >> set_var >> dbt_run_models >> end_dummy


