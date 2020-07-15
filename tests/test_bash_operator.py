from airflow.operators.bash_operator import BashOperator


def test_bash_operator():
    test = BashOperator(task_id="test", bash_command="echo testme", xcom_push=True)
    result = test.execute(context={})
    assert result == "testme"
