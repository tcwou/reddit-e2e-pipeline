import psycopg2
import json

with open("../configs/extract_data_config.json", "r") as f:
    CONFIG = json.load(f)

conn = None
posts = "CREATE TABLE posts (id varchar,author varchar,created_utc integer,domain varchar,full_link varchar,is_original_content boolean,is_self boolean,is_video boolean,link_flair_text varchar, over_18 boolean,subreddit varchar,subreddit_id varchar,title varchar,url varchar, score integer, ups integer, upvote_ratio float, num_comments integer, num_crossposts integer, total_awards_received integer, selftext varchar, date date, symbol varchar ARRAY, sentiment float) PARTITION BY RANGE (date);"
dim_users = "CREATE TABLE dim_users (id varchar, name varchar, created_utc integer, is_employee boolean, is_gold boolean, is_mod boolean, verified boolean, has_verified_email boolean, link_karma integer, comment_karma integer, total_karma integer, date date) PARTITION BY RANGE (date);"
dim_subreddits = "CREATE TABLE dim_subreddits (id varchar, name varchar, display_name varchar, active_user_count integer, subscribers integer, public_description varchar, created_utc integer, over18 boolean, date date) PARTITION BY RANGE (date);"
dim_companies = "CREATE TABLE dim_companies(id varchar, symbol varchar, name varchar, exchange varchar, asset_type varchar, ipo_date varchar, delisting_date varchar, status varchar, date date) PARTITION BY RANGE (date);"
subreddit_aggregate = "CREATE TABLE subreddit_aggregate (date date, subreddit varchar, posts_1d integer, upvotes_1d integer, comments_1d integer, avg_comments_1d float, awards_1d integer, avg_awards_1d float, self_post_ratio_1d float, authors_1d integer, posts_per_author_1d float, sentiment_1d float, sent_weight_num_1d float, sent_weight_denom_1d bigint) PARTITION BY RANGE (date);"
subreddit_symbol_aggregate = "CREATE TABLE subreddit_symbol_aggregate (date date, subreddit varchar, symbol varchar, mentions_1d integer, upvotes_1d integer, comments_1d integer, awards_1d integer, self_1d integer, authors_1d integer, sent_weight_num_1d float, sent_weight_denom_1d bigint) PARTITION BY RANGE (date);"
tables = [posts, dim_users, dim_subreddits, dim_companies, subreddit_aggregate, subreddit_symbol_aggregate]

try:
    conn = psycopg2.connect(
        dbname=CONFIG["pg_dbname"],
        user=CONFIG["pg_user"],
        password=CONFIG["pg_password"],
        host=CONFIG["pg_host"],
        port=CONFIG["pg_port"]
    )
    cur = conn.cursor()
    for table in tables:
        cur.execute(table)
        conn.commit()

finally:
    if conn is not None:
        conn.close()
