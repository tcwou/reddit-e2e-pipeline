from reddit_to_postgres import RedditToPostgres
import datetime
import numpy as np
import pandas as pd
import re
from rtp_args import get_args

args = get_args()

DAY_OFFSET = args.day_offset
TABLE_NAME = "public.subreddit_aggregate"

rtp = RedditToPostgres(table_name=TABLE_NAME)

end_date = args.end_date
start_date_delta=args.start_date_delta

if end_date:
    end_date = datetime.date(end_date[0], end_date[1], end_date[2])
else:
    end_date = datetime.date.today()

start_date = end_date - datetime.timedelta(start_date_delta)

start_date = (start_date - datetime.timedelta(DAY_OFFSET))
end_date = (end_date - datetime.timedelta(DAY_OFFSET))

rtp.postgres_create_partitions(start_date, end_date)

start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d") 

delete_query = (
    f"DELETE FROM {TABLE_NAME} "
    f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
)

rtp.postgres_execute(delete_query)

agg_query = (f"""
    INSERT INTO {TABLE_NAME}
    SELECT
        date,
        subreddit,
        posts_1d,
        upvotes_1d,
        comments_1d,
        comments_1d * 1.0 / posts_1d AS avg_comments_1d,
        awards_1d,
        awards_1d * 1.0 / posts_1d AS avg_awards_1d,
        self_1d * 1.0 / posts_1d AS self_post_ratio_1d,
        authors_1d,
        posts_1d * 1.0 / authors_1d AS posts_per_author_1d,
        sentiment_1d,
        sent_weight_num_1d,
        sent_weight_denom_1d
    FROM (
        SELECT
            p.date,
            p.subreddit,
            COUNT(*) AS posts_1d,
            SUM(p.ups) AS upvotes_1d,
            SUM(p.num_comments) AS comments_1d,
            SUM(p.total_awards_received) AS awards_1d,
            SUM(CASE WHEN p.is_self THEN 1 ELSE 0 END) AS self_1d,
            COUNT(DISTINCT p.author) AS authors_1d,
            MAX(s.subscribers) AS subscribers,
            MAX(s.active_user_count) AS active_users_1d,
            CASE 
                WHEN SUM(CASE WHEN p.sentiment IS NOT NULL THEN p.ups END) < 1 THEN NULL
                ELSE SUM(p.sentiment * p.ups) * 1.0 / SUM(CASE WHEN p.sentiment IS NOT NULL THEN p.ups END)
            END AS sentiment_1d,
            SUM(p.sentiment * p.ups) AS sent_weight_num_1d,
            SUM(CASE WHEN p.sentiment IS NOT NULL THEN p.ups END) AS sent_weight_denom_1d
        FROM (
            SELECT
                id,
                date,
                subreddit_id,
                subreddit,
                ups,
                num_comments,
                total_awards_received, 
                is_self,
                author,
                sentiment
            FROM public.posts 
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        ) p
        LEFT JOIN public.dim_subreddits s
          ON p.subreddit_id = s.id
          AND p.date = s.date
        GROUP BY p.date, p.subreddit
    ) sr_daily;
    """
)

rtp.postgres_execute(agg_query)
