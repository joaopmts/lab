from airflow.hooks.base import BaseHook
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

class SpotifyHook(BaseHook):
    def __init__(self):
        super().__init__()
        self.conn_id = "spotify_default"

    def get_spotify_token(self):
        conn = self.get_connection(self.conn_id)
        client_id = conn.login
        client_secret = conn.password

        sp = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=client_id,
                client_secret=client_secret
            )
        )
    
    def get_spotify_auth(self):
        conn = self.get_connection(self.conn_id)
        client_id = conn.login
        client_secret = conn.password
        host = conn.host

        return client_id, client_secret