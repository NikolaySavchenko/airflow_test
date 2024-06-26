from airflow import DAG
from airflow.operators.bash import BashOperator

from airflow.utils.dates import days_ago

dag = DAG('test_dag', start_date=days_ago(0, 0, 0, 0, 0))

operation = BashOperator(
    bash_command="pwd",
    dag=dag,
    task_id='operation_1'
)

operation
