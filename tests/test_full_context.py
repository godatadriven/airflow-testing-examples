import pytest
from airflow.operators.python_operator import PythonOperator


def test_full_context(test_dag, tmpdir):
    def do_magic(**context):
        with open(tmpdir / "test.txt", "w") as f:
            f.write(context["ds"])

    task = PythonOperator(task_id="test", python_callable=do_magic, provide_context=True, dag=test_dag)
    pytest.helpers.run_task(task=task, dag=test_dag)

    with open(tmpdir / "test.txt", "r") as f:
        assert f.readlines()[0] == test_dag.start_date.strftime("%Y-%m-%d")
