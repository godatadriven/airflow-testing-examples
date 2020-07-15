import json
from collections import namedtuple
from os import path
from pathlib import Path

import pytest
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Connection
from pytest_docker_tools import container, fetch

from testing_examples.operators.postgres_to_local_operator import PostgresToLocalOperator


@pytest.fixture(scope="module")
def postgres_credentials():
    """Namedtuple containing postgres credentials to define only once."""
    PostgresCredentials = namedtuple("PostgresCredentials", ["username", "password"])
    return PostgresCredentials("testuser", "testpass")


postgres_image = fetch(repository="postgres:11.1-alpine")

postgres = container(
    image="{postgres_image.id}",
    environment={
        "POSTGRES_USER": "{postgres_credentials.username}",
        "POSTGRES_PASSWORD": "{postgres_credentials.password}",
    },
    ports={"5432/tcp": None},
    volumes={
        path.join(path.dirname(__file__), "postgres-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
        }
    },
)


def test_postgres_to_local_operator(test_dag, mocker, tmpdir, postgres, postgres_credentials):
    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            host="localhost",
            conn_type="postgres",
            login=postgres_credentials.username,
            password=postgres_credentials.password,
            port=postgres.ports["5432/tcp"][0],
        ),
    )

    output_path = str(tmpdir / "pg_dump")
    task = PostgresToLocalOperator(
        task_id="test",
        postgres_conn_id="postgres",
        pg_query="SELECT * FROM dummy",
        local_path=output_path,
        dag=test_dag,
    )
    pytest.helpers.run_task(task=task, dag=test_dag)

    # Assert if output file exists
    output_file = Path(output_path)
    assert output_file.is_file()

    # Assert file contents, should be the same as in postgres-init.sql
    expected = [{"id": 1, "name": "dummy1"}, {"id": 2, "name": "dummy2"}, {"id": 3, "name": "dummy3"}]
    with open(output_file, "r") as f:
        assert json.load(f) == expected
