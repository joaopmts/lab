import praw
from airflow.hooks.base import BaseHook

class RedditHook(BaseHook):
    def __init__(self):
        super().__init__()
        self.conn_id = "reddit_default"

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson

        client_id = extra.get("client_id")
        client_secret = extra.get("client_secret")
        user_agent = extra.get("user_agent")

        reddit = praw.Reddit(
            client_id=client_id,
            client_secret=client_secret,
            user_agent=user_agent,
            )
        return reddit
    
    def get_cli(self):
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson

        client_id = extra.get("client_id")
        client_secret = extra.get("client_secret")
        user_agent = extra.get("user_agent")

        return client_id, client_secret, user_agent
    
    def reddit_search(self, query, subreddit, sort, time_filter, limit):
        reddit = self.get_conn()
        results = reddit.subreddit(subreddit).search(
            query=query,
            sort=sort,
            time_filter=time_filter,
            limit=limit
        )

        return results
