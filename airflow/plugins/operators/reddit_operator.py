from airflow.models import BaseOperator
from hook.reddit_hook import RedditHook

class RedditOperator(BaseOperator):

    def __init__(self, query, subreddit = "all", sort="new", time_filter="hour", limit=None, **kwargs):
        self.query = query
        self.subreddit = subreddit
        self.sort = sort
        self.time_filter = time_filter
        self.limit = limit
        super().__init__(**kwargs)
    
    def appenddata(self, r):
        posts = []
        for s in r:
            posts.append({
                "id": getattr(s, "id", None),
                "title": getattr(s, "title", None),
                "selftext": getattr(s, "selftext", None),   # corpo do post
                "url": getattr(s, "url", None),
                "permalink": getattr(s, "permalink", None),
                "subreddit": str(getattr(s, "subreddit", "")),
                "author": str(getattr(s, "author", "")),
                "score": getattr(s, "score", None),
                "upvote_ratio": getattr(s, "upvote_ratio", None),
                "num_comments": getattr(s, "num_comments", None),
                "created_utc": getattr(s, "created_utc", None),
                "edited": getattr(s, "edited", None),
                "is_self": getattr(s, "is_self", None),
                "stickied": getattr(s, "stickied", None),
                "over_18": getattr(s, "over_18", None),
                "spoiler": getattr(s, "spoiler", None),
                "link_flair_text": getattr(s, "link_flair_text", None),
                "media": getattr(s, "media", None),
                "preview": getattr(s, "preview", None),
            })

        return posts

    def search(self):
        hook = RedditHook()
        r = hook.reddit_search(
            query=self.query,
            subreddit=self.subreddit,
            sort=self.sort,
            time_filter=self.time_filter,
            limit=self.limit
        )

        return r
    
    def execute(self, context):
        posts = self.search()
        return self.appenddata(posts)

