from datetime import datetime, timedelta  

from airflow.decorators import dag, task
from airflow.providers.postgres.operators.postgres import PostgresOperator 



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
def interview_dag(): 
    @task() 
    def et_top50_brasil_artists(): 
        from interview.etl_spotify import SpotifyClient
        import pandas as pd
        client_ID = "42e08bce9c444915b66fa8569f3e3d00" 
        client_secret = "e0bb81c40e4d4b0e859c2477de4ddbc5" 
        top50_brasil = "37i9dQZEVXbMXbN3EUUhlg"

        spotify = SpotifyClient(client_ID, client_secret)
        extracted_data = spotify.extract_playlist_artists(top50_brasil, 50) 
        flat_artists = SpotifyClient.transform_artists(extracted_data)
        pd.DataFrame.from_dict(flat_artists).to_csv("dags/interview/tmp/artists.csv", header=True, index=False) 
    extract_transform = et_top50_brasil_artists()
    
    truncate_artists = PostgresOperator(
        task_id="truncate_top_artists",
        postgres_conn_id= "spotify_conn",
        sql="TRUNCATE TABLE artists"
    )

    @task() 
    def load_artists(): 
        from interview.etl_spotify import spotify_db_conn
        import csv
        conn = spotify_db_conn()  
        cursor = conn.cursor()
        with open('dags/interview/tmp/artists.csv', 'r') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                cursor.execute( "INSERT INTO artists(name) VALUES (%s)", row) 
        conn.commit()
    load = load_artists() 

    @task()
    def et_urls(): 
        from interview.vagalumes_crawler import vagalumes_top100_crawler 
        url = "https://www.vagalume.com.br/top100/artistas/geral/2022/04/"
        return vagalumes_top100_crawler(url)

    @task(multiple_outputs=True) 
    def et_top100_vagalumes_musics(urls): 
        from interview.vagalumes_crawler import flat_musics
        return flat_musics(urls) 
    
    @task
    def load_top100_vagalumes_musics(data): 
        import pandas as pd
        pd.DataFrame.from_dict(data).to_csv("dags/interview/tmp/musics.csv", header=True, index=False)

    extract_transform >> truncate_artists >> load 
    load_top100_vagalumes_musics(et_top100_vagalumes_musics(et_urls()))

dag = interview_dag() 
