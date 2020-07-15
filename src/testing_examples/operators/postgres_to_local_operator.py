"""Dump JSON data from Postgres to local storage."""

import json
from typing import Optional

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.extras import RealDictCursor


class PostgresToLocalOperator(BaseOperator):
    """
    Airflow operator for storing a JSON-formatted
    Postgres query result on local disk.
    """

    ui_color = "#705B74"
    ui_fgcolor = "#8FA48B"

    @apply_defaults
    def __init__(
        self,
        pg_query: str,
        local_path: str,
        postgres_conn_id: Optional[str] = None,
        **kwargs
    ):
        super().__init__(**kwargs)
        self._pg_query = pg_query
        self._local_path = local_path
        self._postgres_conn_id = postgres_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self._postgres_conn_id)
        conn = postgres_hook.get_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(self._pg_query)

        with open(self._local_path, "w") as f:
            json.dump(cursor.fetchall(), f, indent=4)
