from airflow.sdk import BaseOperator

from hook.reddit_hook import RedditHook


class RedditOperator(BaseOperator):
    """Busca posts no Reddit e retorna (via XCom) uma lista de dicts com exatamente os
    campos de ``REDDIT_POST_SCHEMA`` (scripts/lab_utils.py)."""

    template_fields = ("query", "subreddit", "time_filter")

    def __init__(self, query, subreddit="all", sort="new", time_filter="hour", limit=None, **kwargs):
        super().__init__(**kwargs)
        self.query = query
        self.subreddit = subreddit
        self.sort = sort
        self.time_filter = time_filter
        self.limit = limit

    def execute(self, context):
        from scripts.lab_utils import submission_to_dict

        results = RedditHook().reddit_search(
            query=self.query,
            subreddit=self.subreddit,
            sort=self.sort,
            time_filter=self.time_filter,
            limit=self.limit,
        )
        posts = [submission_to_dict(p) for p in results]
        self.log.info("%d posts encontrados para '%s'", len(posts), self.query)
        return posts
