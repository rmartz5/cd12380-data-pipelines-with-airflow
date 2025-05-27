from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',        # renamed
                 sql_insert='',              # renamed
                 table='',
                 truncate=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_insert = sql_insert
        self.table = table
        self.truncate = truncate

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f'Truncate table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')

        self.log.info(f'Load fact table {self.table}')
        redshift.run(f'INSERT INTO {self.table} {self.sql_insert}')