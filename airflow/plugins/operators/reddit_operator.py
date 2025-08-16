from airflow.models import BaseOperator
from hook.reddit_hook import RedditHook

class RedditOperator(BaseOperator):

    def __init__(self, query, subreddit = "all", sort="new", time_filter="hour", **kwargs):
        self.query = query
        self.subreddit = subreddit
        self.sort = sort
        self.time_filter = time_filter
        super().__init__(**kwargs)
    
    def appenddata(self, r):
        posts = []
        for s in r:
            posts.append({
                "id": getattr(s, "id", None),
                "title": getattr(s, "title", None),
                "score": getattr(s, "score", None),
                "subreddit": str(getattr(s, "subreddit", "")),
                "author": str(getattr(s, "author", "")),
                "url": getattr(s, "url", None),
                "permalink": getattr(s, "permalink", None),
                "created_utc": getattr(s, "created_utc", None), 
                "num_comments": getattr(s, "num_comments", None),
            })

        return posts

    def search(self):
        hook = RedditHook()
        r = hook.reddit_search(
            query=self.query,
            subreddit=self.subreddit,
            sort=self.sort,
            time_filter=self.time_filter,
        )

        return r
    
    def execute(self, context):
        posts = self.search()
        return self.appenddata(posts)

