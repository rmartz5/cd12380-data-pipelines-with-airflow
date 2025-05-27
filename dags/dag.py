from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries

# Default arguments for the DAG
default_args = {
    'owner': 'udacity',
    'start_date': datetime(2024, 1, 1),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Instantiate the DAG
dag = DAG(
    'sparkify_dwh_etl',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly',
    max_active_runs=1
)

# Start and end dummy tasks
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Staging tasks
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table='staging_events',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    json_path='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_credentials_id='aws_credentials',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    json_path='auto'
)

# Load fact table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songplays',
    sql_insert=SqlQueries.songplay_table_insert
)

# Load dimension tables
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='users',
    sql_insert=SqlQueries.user_table_insert,
    truncate_table=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='songs',
    sql_insert=SqlQueries.song_table_insert,
    truncate_table=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='artists',
    sql_insert=SqlQueries.artist_table_insert,
    truncate_table=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    table='time',
    sql_insert=SqlQueries.time_table_insert,
    truncate_table=True
)

# Data quality checks
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    postgres_conn_id='redshift',
    tests=[
        {'sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result': 0},
        {'sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0},
    ]
)

# DAG task pipeline
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
]

[
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table
] >> run_quality_checks

run_quality_checks >> end_operator
