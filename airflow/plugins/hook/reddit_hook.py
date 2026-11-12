from airflow.hooks.base import BaseHook


class RedditHook(BaseHook):
    """Acesso à API do Reddit via praw usando a connection ``reddit_default``
    (client_id, client_secret e user_agent no extra)."""

    def __init__(self, conn_id="reddit_default"):
        super().__init__()
        self.conn_id = conn_id

    def get_cli(self):
        extra = self.get_connection(self.conn_id).extra_dejson
        return extra.get("client_id"), extra.get("client_secret"), extra.get("user_agent")

    def get_conn(self):
        import praw

        client_id, client_secret, user_agent = self.get_cli()
        return praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
        )

    def reddit_search(self, query, subreddit="all", sort="new", time_filter="day", limit=None):
        return self.get_conn().subreddit(subreddit).search(
            query=query,
            sort=sort,
            time_filter=time_filter,
            limit=limit,
        )
