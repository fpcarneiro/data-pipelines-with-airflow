from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'depends_on_past': False, # The DAG does not have dependencies on past runs
    'owner': 'udacity',
    'retries': 3, # On failure, the task are retried 3 times
    'retry_delay': timedelta(minutes=5), # Retries happen every 5 minutes
    'start_date': datetime(2019, 1, 1),
    'email_on_retry': False, # Do not email on retry
    #'catchup' = False, # Catchup is turned off
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'udacity-dend',
    s3_prefix = 'log_data',
    schema_to = 'public',
    table_to = 'staging_events',
    options = ["json 's3://udacity-dend/log_json_path.json'"],
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    aws_conn_id = 'aws_credentials',
    redshift_conn_id = "redshift",
    s3_from = 'udacity-dend',
    s3_prefix = 'song_data',
    schema_to = 'public',
    table_to = 'staging_songs',
    options = ["json 'auto'"],
    dag=dag
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    redshift_conn_id = "redshift",
    table_to = 'songplays',
    query = SqlQueries.songplay_table_insert,
    truncate_before = True,
    dag=dag
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    redshift_conn_id = "redshift",
    table_to = 'users',
    query = SqlQueries.user_table_insert,
    truncate_before = True,
    dag=dag
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    redshift_conn_id = "redshift",
    table_to = 'songs',
    query = SqlQueries.song_table_insert,
    truncate_before = True,
    dag=dag
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    redshift_conn_id = "redshift",
    table_to = 'artists',
    query = SqlQueries.artist_table_insert,
    truncate_before = True,
    dag=dag
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    redshift_conn_id = "redshift",
    table_to = 'time',
    query = SqlQueries.time_table_insert,
    truncate_before = True,
    dag=dag
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    redshift_conn_id = "redshift",
    tables=['songplays', 'users', 'songs', 'artists', 'time'],
    dag=dag
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator
