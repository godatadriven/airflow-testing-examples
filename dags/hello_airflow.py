import airflow.utils.dates
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id="hello_airflow", start_date=airflow.utils.dates.days_ago(3), schedule_interval="@daily")


def do_magic(**context):
    print(context)


hello = BashOperator(task_id="hello", bash_command="echo 'hello'", dag=dag)
airflow = PythonOperator(task_id="airflow", python_callable=do_magic, provide_context=True, dag=dag)

hello >> airflow
