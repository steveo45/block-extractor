from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.papermill.operators.papermill import PapermillOperator


with DAG(
    dag_id='example_papermill_operator',
    default_args={
        'retries': 0
    },
    schedule='0 0 * * *',
    start_date=datetime(2022, 10, 1),
    template_searchpath='/usr/local/airflow/include',
    catchup=False
) as dag_1:

    notebook_task = PapermillOperator(
        task_id="run_example_notebook",
        input_nb="include/example.ipynb",
        output_nb="include/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )