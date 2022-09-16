from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

# add default parameters

# - The DAG does not have dependencies on past runs
# - On failure, the task are retried 3 times
# - Retries happen every 5 minutes
# - Catchup is turned off
# - Do not email on retry

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

# DAG is scheduled to run once an hour

dag = DAG('process_data_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=1
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    execution_date="{{ execution_date }}",
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",  
    append_data=True,
    table="staging_events",   
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json' \
                  TIMEFORMAT AS 'epochmillisecs' \
                  TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL"  
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,    
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id="redshift",    
    append_data=True,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    extra_params="FORMAT AS JSON 'auto' \
                  TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL"
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,    
    provide_context=True,
    conn_id='redshift',
    table="songplays",
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    provide_context=True,
    conn_id='redshift',
    table="users",
    query=SqlQueries.user_table_insert,
    truncate=True
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    provide_context=True,
    conn_id='redshift',
    table="songs",
    query=SqlQueries.song_table_insert,
    truncate=True
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    provide_context=True,
    conn_id='redshift',
    table="artists",
    query=SqlQueries.artist_table_insert,
    truncate=True
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    provide_context=True,
    conn_id='redshift',
    table="time",
    query=SqlQueries.time_table_insert,
    truncate=True
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,    
    provide_context=True,
    conn_id="redshift",    
    tables=["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# configure task dependencies

start_operator >> [stage_events_to_redshift, \
                   stage_songs_to_redshift]

[stage_events_to_redshift, \
 stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, \
                         load_song_dimension_table, \
                         load_artist_dimension_table, \
                         load_time_dimension_table]

[load_user_dimension_table, \
 load_song_dimension_table, \
 load_artist_dimension_table, \
 load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator