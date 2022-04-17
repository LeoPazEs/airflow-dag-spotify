import requests
import base64

from airflow.providers.postgres.hooks.postgres import PostgresHook



def client_credential(clientID, clientSecret): 
    url = "https://accounts.spotify.com/api/token?" 
    client_encoded = base64.b64encode(bytes(f"{clientID}:{clientSecret}", "ISO-8859-1")).decode('ascii')
    header = {
        "Authorization" : f"Basic {client_encoded}",
        "Content-Type" : "application/x-www-form-urlencoded",
    } 
    data = {
        "grant_type": "client_credentials"
    }
    credential = requests.post(url, data = data ,headers= header) 
    json_response = credential.json()
    return json_response["access_token"] 

def extract_playlist_tracks_artists(playlist_id, client_credential): 
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?market=ES&fields=items(track(artists(name)))&limit=50"
    header = {
        "Content-Type" : "application/json",
        "Authorization" : f"Bearer {client_credential}"
    } 
    tracks = requests.get(url, headers= header)  
    json_response = tracks.json()
    return json_response["items"]  

def transform_artists(playlist_tracks_artists):
    transformed_artists = [] 

    for track in playlist_tracks_artists: 
        for artist in track["track"]["artists"]: 
            if not artist["name"] in transformed_artists: transformed_artists.append(artist["name"])
    
    return {"artists" : transformed_artists} 

def spotify_db_conn(): 
    pg_hook = PostgresHook(
        postgres_conn_id = "spotify_conn",
        schema = "airflow_db"
    ) 
    pg_conn = pg_hook.get_conn() 
    return pg_conn
