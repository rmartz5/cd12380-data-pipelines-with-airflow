from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql_insert='',
                 truncate_table=False,
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_insert = sql_insert
        self.truncate_table = truncate_table

    def execute(self, context):
        self.log.info(f'Connecting to Redshift: {self.redshift_conn_id}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_table:
            self.log.info(f'Truncating Redshift table {self.table}')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        self.log.info(f'Inserting data into {self.table}')
        redshift.run(f'INSERT INTO {self.table} {self.sql_insert}')
