from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

# define dummy start operator
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# copy into table staging_events
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json_path="s3://udacity-dend/log_json_path.json"
)

# copy into table staging_songs
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data"    
)

#define load facts table
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="songplays",
    table_columns="(playid, start_time, userid, level, songid, artistid, sessionid, location, user_agent)",
    sql_select=SqlQueries.songplay_table_insert
)

# define load dimension tasks
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="users",
    table_columns="(userid, first_name, last_name, gender, level)",
    sql_select=SqlQueries.user_table_insert,
    append_mode=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="songs",
    table_columns="(songid, title, artistid, year, duration)",
    sql_select=SqlQueries.song_table_insert,
    append_mode=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="artists",
    table_columns="(artistid, name, location, lattitude, longitude)",
    sql_select=SqlQueries.artist_table_insert,
    append_mode=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",    
    table="time",
    table_columns="(start_time, hour, day, week, month, year, weekday)",
    sql_select=SqlQueries.time_table_insert,
    append_mode=False
)

# define data quality check task
dq_checks=[
        {'check_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0},
        {'check_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0}
    ]

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",    
    dq_checks=dq_checks
)

#define end operator
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Define task dependecies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
run_quality_checks >> end_operator

