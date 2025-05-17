from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,

}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution',dag=dag)

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_path='s3://udacity-dend/log_data',
        region='us-west-2',
        json_option="s3://udacity-dend/log_json_path.json",
        provide_context=True,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        s3_path='s3://udacity-dend/song_data',
        region='us-west-2',
        provide_context=True,
        json_option='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id="redshift",
        sql=SqlQueries.songplay_table_insert,
        table='songplays',
        truncate=False,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="users",
        sql=SqlQueries.user_table_insert,
        truncate=False,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="songs",
        sql=SqlQueries.song_table_insert,
        truncate=False,
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="artists",
        sql=SqlQueries.artist_table_insert,
        truncate=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table="time",
        sql=SqlQueries.time_table_insert,
        truncate=False,
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        dag=dag,
        redshift_conn_id="redshift",
        tests=[
            {
                "table": "SELECT COUNT(*) FROM users WHERE userid IS NULL",
                "return": 0,
            },
        ],
        ignore_fails=False,
    )

    end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

    # set task dependencies according to required flow
    start_operator >> [
        stage_events_to_redshift,
        stage_songs_to_redshift,
    ] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table,
    ] >> run_quality_checks >> end_operator

final_project_dag = final_project()