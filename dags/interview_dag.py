from datetime import datetime, timedelta  

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator 

from interview.etl_spotify import client_credential, transform_artists, extract_playlist_tracks_artists, spotify_db_conn
import pandas as pd
import csv

default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(seconds=5),
        'retry_delay': timedelta(minutes=5), 
} 


@dag(description= 'DAG made for interview at MESHA', start_date= datetime(2022, 4, 16), schedule_interval= '@daily', default_args= default_args, tags = ['interview'], catchup= False)
def spotify_vagalumes_dag(): 
    @task() 
    def et_top_artists(): 
        client_ID = "42e08bce9c444915b66fa8569f3e3d00" 
        client_secret = "e0bb81c40e4d4b0e859c2477de4ddbc5" 
        top50Brasil = "37i9dQZEVXbMXbN3EUUhlg"

        extracted_data = extract_playlist_tracks_artists(top50Brasil, client_credential(client_ID, client_secret))
        transformed_data = transform_artists(extracted_data)   
        pd.DataFrame.from_dict(transformed_data).to_csv("dags/interview/tmp/artists.csv", header=True, index=False) 
    extract_transform = et_top_artists()
    
    truncate_artists = PostgresOperator(
        task_id="truncate_top_artists",
        postgres_conn_id= "spotify_conn",
        sql="TRUNCATE TABLE artists"
    )

    @task() 
    def load_artists(): 
        conn = spotify_db_conn()  
        cursor = conn.cursor()
        with open('dags/interview/tmp/artists.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute( "INSERT INTO artists(name) VALUES (%s)", row) 
        conn.commit()
    load = load_artists()

    extract_transform >> truncate_artists >> load
    
dag = spotify_vagalumes_dag() 
