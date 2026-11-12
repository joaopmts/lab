from airflow.hooks.base import BaseHook


class SpotifyHook(BaseHook):
    """Acesso à API do Spotify usando a connection ``spotify_default``
    (login = client_id, password = client_secret)."""

    def __init__(self, conn_id="spotify_default"):
        super().__init__()
        self.conn_id = conn_id

    def get_spotify_auth(self):
        conn = self.get_connection(self.conn_id)
        return conn.login, conn.password

    def get_conn(self):
        import spotipy
        from spotipy.oauth2 import SpotifyClientCredentials

        client_id, client_secret = self.get_spotify_auth()
        return spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(client_id=client_id, client_secret=client_secret)
        )

    # Nome antigo mantido por compatibilidade (antes não retornava nada)
    get_spotify_token = get_conn
