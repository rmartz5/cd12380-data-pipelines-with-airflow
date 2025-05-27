from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT AS JSON '{}'
        REGION '{}'
    """

    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 region='',
                 truncate=False,
                 json_path='auto',  # <-- Add this
                 **kwargs):

        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.truncate = truncate
        self.json_path = json_path  # <-- Save it

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsBaseHook(self.aws_credentials_id, client_type='s3')
        credentials = aws_hook.get_credentials()

        if self.truncate:
            self.log.info(f'Truncating Redshift table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')

        rendered_s3_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_s3_key}'

        formatted_sql = self.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path,  # <-- Injected here
            self.region
        )

        self.log.info(f'Copying data from {s3_path} to Redshift table {self.table}')
        self.log.info(f'Running SQL:\n{formatted_sql}')
        redshift.run(formatted_sql)