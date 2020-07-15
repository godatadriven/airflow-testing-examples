import datetime

from airflow.operators.python_operator import PythonOperator


def test_python_operator():
    test = PythonOperator(task_id="test", python_callable=lambda: "testme")
    result = test.execute(context={})
    assert result == "testme"


def next_week(**context):
    return context["execution_date"] + datetime.timedelta(days=7)


def test_python_operator_with_context():
    test = PythonOperator(task_id="test", python_callable=next_week, provide_context=True)
    testdate = datetime.datetime(2020, 1, 1)
    result = test.execute(context={"execution_date": testdate})
    assert result == testdate + datetime.timedelta(days=7)
