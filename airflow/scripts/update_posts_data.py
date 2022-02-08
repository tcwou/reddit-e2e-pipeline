import flair
from reddit_to_postgres import RedditToPostgres
from rtp_args import get_args

args = get_args()

flair_sentiment = flair.models.TextClassifier.load('./models/.flair/models/sentiment-en-mix-distillbert_4.pt')

rtp = RedditToPostgres(
        end_date=args.end_date,
        start_date_delta=args.start_date_delta,
        filter_keys=[
            'id',
            'author',
            'created_utc',
            'domain',
            'full_link',
            'is_original_content',
            'is_self',
            'is_video',
            'link_flair_text',
            'over_18',
            'subreddit',
            'subreddit_id',
            'title',
            'url',
            'score',
            'ups',
            'upvote_ratio',
            'num_comments',
            'num_crossposts',
            'total_awards_received',
            'selftext'
        ],
        table_name='public.posts',
        sentiment_model=flair_sentiment
    )
rtp.load_rapi_posts(day_offset=args.day_offset)
