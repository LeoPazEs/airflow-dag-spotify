import requests
import base64

from airflow.providers.postgres.hooks.postgres import PostgresHook 

from datetime import datetime, timedelta

class SpotifyClient(): 
    def __init__(self, client_id, client_secret): 
        self.id = client_id 
        self.secret = client_secret 
        self.__credential = None

          
    @property
    def access_token(self):
        if not self.__credential or self.__credential["expire_date"] <= datetime.now():
            self.__credential = self.get_credential() 
            return self.__credential["access_token"]
        return self.__credential["access_token"]

    @staticmethod
    def transform_artists(playlist_artists):
        transformed_artists = [] 
        for track in playlist_artists: 
            for artist in track["track"]["artists"]: 
                if not artist["name"] in transformed_artists: transformed_artists.append(artist["name"])    
        return {"artists" : transformed_artists}
        
    def get_credential(self): 
        url = "https://accounts.spotify.com/api/token?" 
        header = {
            "Authorization" : f"Basic {self.encode_client_credentials()}",
            "Content-Type" : "application/x-www-form-urlencoded",
        } 
        data = { "grant_type": "client_credentials" }
        json_response = (requests.post(url, data = data ,headers= header)).json()
        return {"access_token" : json_response["access_token"], "expire_date": datetime.now() + timedelta(seconds= int(json_response["expires_in"]))}
    
    def encode_client_credentials(self): 
        return base64.b64encode(bytes(f"{self.id}:{self.secret}", "ISO-8859-1")).decode('ascii')
    
    def extract_playlist_artists(self, playlist_id, limit): 
        url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks?market=ES&fields=items(track(artists(name)))&limit={limit}"
        header = {
            "Content-Type" : "application/json",
            "Authorization" : f"Bearer {self.access_token}"
        } 
        tracks = requests.get(url, headers= header)  
        json_response = tracks.json()
        return json_response["items"]


def spotify_db_conn(): 
    pg_hook = PostgresHook(
        postgres_conn_id = "spotify_conn",
        schema = "airflow_db"
    ) 
    pg_conn = pg_hook.get_conn() 
    return pg_conn
