# This demonstrated the DebugExecutor
# https://airflow.apache.org/docs/stable/executor/debug.html
# Set AIRFLOW__CORE__EXECUTOR=DebugExecutor

import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id="hello_airflow", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily")

hello = BashOperator(task_id="hello", bash_command="echo 'hello'", dag=dag)
airflow = PythonOperator(task_id="airflow", python_callable=lambda: print("airflow"), dag=dag)

hello >> airflow

if __name__ == "__main__":
    dag.clear(reset_dag_runs=True)
    dag.run()
