from reddit_to_postgres import RedditToPostgres
from rtp_args import get_args

args = get_args()

rtp = RedditToPostgres(
        end_date=args.end_date,
        start_date_delta=args.start_date_delta,
        filter_keys=[
            'id',
            'name',
            'display_name', 
            'active_user_count', 
            'subscribers', 
            'public_description', 
            'created_utc', 
            'over18',
        ],
        table_name='public.dim_subreddits'
    )
rtp.load_sr_info()