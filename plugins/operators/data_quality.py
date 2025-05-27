from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 postgres_conn_id='',
                 tests=None,
                 *args, **kwargs):
        """
        Args:
            postgres_conn_id: Airflow connection ID for Postgres
            tests: list of dicts, each with 'sql' and 'expected_result'
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.tests = tests or []

    def execute(self, context):
        postgres = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        for test in self.tests:
            sql = test['sql']
            expected = test['expected_result']

            self.log.info(f"Running test: {sql}")
            records = postgres.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"No results returned for query: {sql}")

            actual = records[0][0]
            if actual != expected:
                raise ValueError(f"Test failed: {sql}. Expected {expected}, got {actual}")

            self.log.info(f"Test passed: {sql} returned {actual}")
