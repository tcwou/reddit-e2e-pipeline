import flair
import json
from requests import get, post, auth
from requests.auth import HTTPBasicAuth
import datetime
import time
import logging
import psycopg2
import re
from psycopg2.extras import execute_values
from typing import Dict, List, Tuple, Any
import string

logging.getLogger().setLevel(logging.INFO)

class RedditToPostgres():
    """
    A class to initalize connection to postgres db and load reddit data

    ...

    Attributes
    ----------
    config_path : str, default '/configs/extract_data_config.json'
        path to extract data json config
    size : int, default 100
        length of response per query, reddit has limit of 100 per request
    sleep : int, default 3
        sleep time between requests in seconds
    filter_keys : List[str], default None
        keep only these keys from the response 
    table_name : str, default None
        db table name to store result
    end_date : Tuple[int, int, int], default None
        end date (yyyy, mm, dd)
    start_date_delta : int, default 1
        delta from start date
    Methods
    -------
    load_ps_posts():
        Load posts from pushshift into db
    load_rapi_posts():
        Load posts from reddit api into db from pushshift ids
    load_sr_info():
        Load subreddit info into db
    postgres_create_partitions(start_date, end_date):
        Create partitions in table attribute for start_date to end_date
    postgres_execute(query):
        Execute provided query in db
    postgres_insert(rows):
        Insert data into db
    """
    def __init__(
            self,
            config_path: str = "./configs/extract_data_config.json",
            size: int = 100,
            sleep: int = 3,
            filter_keys: List[str] = None,
            table_name: str = None,
            end_date: Tuple[int, int, int] = None,
            start_date_delta: int = 1,
            sentiment_model = None,
            **kwargs) -> None:

        super().__init__(**kwargs)
        self.config_path = config_path
        self.size = size
        self.sleep = sleep
        self.filter_keys = filter_keys
        self.table_name = table_name
        self.sentiment_model = sentiment_model

        with open(self.config_path, "r") as f:
            CONFIG = json.load(f)

        # pushshift config
        self.ps_endpoint = CONFIG["ps_endpoint"]
        self.subreddits = CONFIG["subreddits"]

        if end_date:
            self.end_date = datetime.date(end_date[0], end_date[1], end_date[2])
        else:
            self.end_date = datetime.date.today()
            
        self.start_date = self.end_date - datetime.timedelta(start_date_delta)

        # reddit API config
        USERNAME = CONFIG["r_username"]
        PASSWORD = CONFIG["r_password"]
        C_ID = CONFIG["r_client_id"]
        C_SEC = CONFIG["r_client_secret"]
        USER_AGENT = f'{USERNAME} request'

        subreddits = CONFIG["subreddits"]

        body = {'grant_type': 'password', 'username': USERNAME, 'password': PASSWORD}
        auth = HTTPBasicAuth(C_ID, C_SEC)
        auth_r = post('https://www.reddit.com/api/v1/access_token',
                          data=body,
                          headers={'user-agent': USER_AGENT},
                  auth=auth)
        auth_d = auth_r.json()

        token = f'bearer {auth_d["access_token"]}'

        self.headers = {'authorization': token, 'user-agent': USER_AGENT}

        # postgres config
        self.pg_dbname=CONFIG["pg_dbname"]
        self.pg_user=CONFIG["pg_user"]
        self.pg_password=CONFIG["pg_password"]
        self.pg_host=CONFIG["pg_host"]
        self.pg_port=CONFIG["pg_port"]

    def get_data(self, subreddit=None, id_endpoint=None,
            reddit_headers=None, ps_params=None):
        """ Get data from pushshift or reddit api, returns length of response and data """
        attempts = 10
        for attempt in range(attempts):
            try:
                if reddit_headers:
                    if subreddit:
                        endpoint = f'https://oauth.reddit.com/r/{subreddit}/about'
                    elif id_endpoint:
                        endpoint = id_endpoint

                    r = get(endpoint, headers=reddit_headers)

                elif ps_params:
                    r = get(self.ps_endpoint, params=ps_params)

                logging.info(f'Request sent: {r.url}')
                logging.info(f'Received response {r.status_code}')

                data_batch = r.json()['data']
                response_length = len(data_batch)
            except json.decoder.JSONDecodeError:
                logging.warn(
                    f'HTTP status code: {r.status_code}, '
                    f'retrying attempt {attempt+1} of {attempts}'
                )
            else:
                break

        # sleep for rate limiting
        time.sleep(self.sleep)

        return response_length, data_batch

    def transform_columns(self, filtered_dict, use_created_date=True, sid=False):
        """Transform date and id columns, return dictionary"""
        if use_created_date:
            filtered_dict['date'] = datetime.datetime.fromtimestamp(filtered_dict['created_utc']).date()
        else:
            filtered_dict['date'] = datetime.date.today()
        if sid:
            filtered_dict['id'] = filtered_dict['id'] + '_' + filtered_dict['date'].strftime('%Y-%m-%d')

        # remove NULL characters from user input string columns
        for col in ['full_link', 'title', 'selftext', 'link_flair_text']:
            if (col in filtered_dict) and (type(filtered_dict[col]) == str):
                filtered_dict[col] = filtered_dict[col].replace("\x00", "\uFFFD")

        return filtered_dict

    def filter_data(self, row, sid=False):
        """Apply column filters and transform date and id columns, return dictionary of data"""
        filtered_dict = {k: row.get(k) for k in self.filter_keys}

        return filtered_dict

    def extract_tickers(self, data_out, symbols=None):
        """Create symbol col or extract tickers from title, return list of dictionaries

        Symbols : List[Tuple(str, str)], default None
        Supply list of ticker symbols to be used for title-symbol matching
        """
        sym_ignore = ('IT', 'DD', 'RE', 'NICE', 'YOLO', 'MOON' ,'IS', 'NEW', 'JAN', 'FOR',
            'TA', 'EPS', 'IQ', 'MEME', 'MF', 'OUT', 'CORP')

        for row in data_out:
            if symbols:
                matches = set()
                # remove non-useful punctuation from text
                punc = string.punctuation
                punc = punc.replace('-', '').replace('$', '')
                title = row['title'].translate(str.maketrans('', '', punc))
                title = ' ' + title + ' '
                for tup in symbols:
                    # regex is too slow, use 'in' operator on most common patterns
                    if (len(tup[0]) == 1) or (tup[0] in sym_ignore):
                        if ('$' + tup[0] + ' ' in title
                            or ((len(tup[1]) > 4) and (tup[1].lower() in title.lower()))):
                            matches.add(tup[0])
                    else:
                        if (' ' + tup[0] + ' ' in title 
                            or '$' + tup[0] + ' ' in title 
                            or ((len(tup[1]) > 4) and (tup[1].lower() in title.lower()))):
                            matches.add(tup[0])

                if len(matches) == 0:
                    row['symbol'] = None 
                else:
                    row['symbol'] = list(matches)
            else:
                row['symbol'] = None

        return data_out

    def transform_data(self, batch,
            nested=False, r_api=False, use_created_date=True, sid=False,
            symbol_col=False, symbols=None):
        """
        Filter data according to API type, return list of dictionaries
        """
        data_out = []

        if nested:
            for batch_dict in batch:
                if r_api:
                    batch_dict = batch_dict['data']
                batch_dict = self.filter_data(batch_dict)
                data_out.append(self.transform_columns(batch_dict, sid=sid))

        else:
            batch = self.filter_data(batch)
            data_out.append(self.transform_columns(
                batch,
                use_created_date=use_created_date,
                sid=sid
            ))

        if symbol_col:
            data_out = self.extract_tickers(data_out, symbols=symbols)

        return data_out

    def get_sentiment(self, batch):
        """Get sentiment of text"""
        for batch_dict in batch:
            text = batch_dict['selftext']
            if (self.sentiment_model 
                and len(text) > 1 
                and '[removed]' not in text
                and '[deleted]' not in text):
                s = flair.data.Sentence(batch_dict['selftext'])
                self.sentiment_model.predict(s)
                score = (1 if s.labels[0].value == 'POSITIVE' else -1) * s.labels[0].score
                batch_dict['sentiment'] = score
            else:
                batch_dict['sentiment'] = None
        return batch

    def postgres_execute(self, query: str, conn=None) -> None:
        """Submit delete query to postgres DB and return None"""
        try:
            conn = psycopg2.connect(
                dbname=self.pg_dbname,
                user=self.pg_user,
                password=self.pg_password,
                host=self.pg_host,
                port=self.pg_port
            )

            cur = conn.cursor()
            logging.info(f'PSQL Query: {query}')

            cur.execute(query)
            conn.commit()

        finally:
            if conn is not None:
                conn.close()

    def postgres_insert(self, rows: List[Dict[str, Any]], conn=None) -> None:
        """Insert rows into postgres DB and return None"""
        try:
            conn = psycopg2.connect(
                dbname=self.pg_dbname,
                user=self.pg_user,
                password=self.pg_password,
                host=self.pg_host,
                port=self.pg_port
            )
            cur = conn.cursor()
            cols = rows[0].keys()
            query = f"INSERT INTO {self.table_name} ({','.join(cols)}) VALUES %s"
            logging.info(f'PSQL Query: {query}')
            values = [[val for val in row.values()] for row in rows]

            execute_values(cur, query, values)
            conn.commit()

        finally:
            if conn is not None:
                conn.close()

    def postgres_get(self, query: str, conn=None) -> List[Tuple[Any]]:
        """Submit select query to postgres DB and return result"""
        try:
            conn = psycopg2.connect(
                dbname=self.pg_dbname,
                user=self.pg_user,
                password=self.pg_password,
                host=self.pg_host,
                port=self.pg_port
            )

            cur = conn.cursor()

            logging.info(f'PSQL Query: {query}')

            cur.execute(query)
            result = cur.fetchall()

        finally:
            if conn is not None:
                conn.close()

        return result

    def postgres_create_partitions(self, 
        start_date: datetime.datetime, end_date: datetime.datetime, conn=None) -> None:
        """Create partitions in table"""

        delta = end_date - start_date

        for i in range(delta.days + 1):
            query_start_date = (start_date + datetime.timedelta(i)).strftime("%Y-%m-%d")
            query_end_date = (start_date + datetime.timedelta(i+1)).strftime("%Y-%m-%d")

            partition_name = self.table_name + '_' + query_start_date.replace('-', '')
            partition_query = (
                f"CREATE TABLE {partition_name} "
                f"PARTITION OF {self.table_name} "
                f"FOR VALUES FROM ('{query_start_date}') TO ('{query_end_date}')"
            )
            try:
                self.postgres_execute(partition_query, conn=conn)
            except psycopg2.errors.DuplicateTable:
                logging.info(f"Partition '{partition_name}' already exists")
                pass


    def load_ps_posts(self,
        start_date: datetime.datetime = None, 
        end_date: datetime.datetime = None, 
        day_offset: int = 0) -> None:
        """Load posts from pushshift into db and insert into DB"""
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        start_date = (start_date - datetime.timedelta(day_offset))
        end_date = (end_date - datetime.timedelta(day_offset))

        self.postgres_create_partitions(start_date, end_date)

        before_unixtime  = int(time.mktime(end_date.timetuple()))
        after_unixtime = int(time.mktime(start_date.timetuple()))

        start_date = start_date.strftime("%Y-%m-%d")
        end_date = end_date.strftime("%Y-%m-%d")    

        daily_posts = []

        for subreddit in self.subreddits:

            after = after_unixtime
            n_posts = 0
            response_length = self.size

            # query for more posts while 100 posts are returned
            while response_length == self.size:

                ps_params = {
                    "subreddit": subreddit,
                    "sort": 'asc',
                    "sort_type": 'created_utc',
                    "after": after,
                    "before": before_unixtime,
                    "size": self.size
                }

                response_length, data_batch = self.get_data(
                    subreddit=subreddit,
                    ps_params=ps_params
                )
                filtered_posts_batch = self.transform_data(
                    data_batch,
                    nested=True,
                    symbol_col=True
                )
                daily_posts += filtered_posts_batch

                if response_length > 0:
                    # set after parameter to unixtime of last post
                    after = data_batch[response_length-1]['created_utc']
                    n_posts += response_length

            logging.info(f'# posts retrieved for {subreddit}: {n_posts}')

            if len(daily_posts) == 0:
                logging.warn((
                    f"No posts retrieved for subreddit '{subreddit}'"
                    f" from {end_date} to {start_date}"
                ))

        delete_query = (
            f"DELETE FROM {self.table_name} "
            f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        )

        self.postgres_execute(delete_query)
        self.postgres_insert(daily_posts)

    def load_rapi_posts(self,
        start_date: datetime.datetime = None, 
        end_date: datetime.datetime = None, 
        day_offset: int = 2) -> None:
        """Load posts from reddit api into db using pushshift ids and replace in DB"""
        if start_date is None:
            start_date = self.start_date
        if end_date is None:
            end_date = self.end_date

        start_date = (start_date - datetime.timedelta(day_offset)).strftime("%Y-%m-%d")
        end_date = (end_date - datetime.timedelta(day_offset)).strftime("%Y-%m-%d")

        daily_posts = []
        r_endpoint = 'https://oauth.reddit.com/by_id/'

        query = (
            f"SELECT id FROM {self.table_name} "
            f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        )

        post_ids = self.postgres_get(query)
        post_ids = ['t3_' + r[0] for r in post_ids]

        ticker_query = (
            "SELECT symbol, name FROM dim_companies " 
            "WHERE date = (SELECT MAX(date) FROM dim_companies) "
            "AND name IS NOT NULL "
            "AND symbol NOT LIKE '%-%';"
        )

        companies = self.postgres_get(ticker_query)
        symbols = []
        for row in companies:
            ticker = row[0]
            name = (re.sub("[\(\[].*?[\)\]]", "", row[1])).strip()
            name = re.split(r'\sinc|\scorp|\sltd|\setf|\s-', name.lower())[0].strip()
            symbols.append((ticker, name))

        for i in range(1 + len(post_ids) // self.size):

            batch = post_ids[i*self.size:(i+1)*self.size]
            endpoint = r_endpoint + ','.join(batch) + f'?limit={self.size}'
            response_length, data_batch = self.get_data(
                id_endpoint=endpoint,
                reddit_headers=self.headers
            )
            data_batch = data_batch['children']
            logging.info(f'{len(data_batch)} posts retrieved by id from reddit api')

            filtered_posts_batch = self.transform_data(
                data_batch,
                nested=True,
                r_api=True,
                use_created_date=False,
                symbol_col=True,
                symbols=symbols
                )
            sentiment_posts_batch = self.get_sentiment(filtered_posts_batch)
            daily_posts += filtered_posts_batch

        delete_query = (
            f"DELETE FROM {self.table_name} "
            f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        )

        self.postgres_execute(delete_query)
        self.postgres_insert(daily_posts)

    def load_sr_info(self) -> None:
        """Load subreddit info from reddit api and insert into DB"""
        start_date = datetime.date.today()
        end_date = start_date + datetime.timedelta(1)

        self.postgres_create_partitions(start_date, end_date)

        start_date = start_date.strftime("%Y-%m-%d")  
        end_date = end_date.strftime("%Y-%m-%d")    

        sr_info = []

        for subreddit in self.subreddits:

            response_length, data_batch = self.get_data(
                subreddit=subreddit,
                reddit_headers=self.headers
            )
            filtered_data_batch = self.transform_data(data_batch, use_created_date=False)
            sr_info += filtered_data_batch

            logging.info(f'info retrieved for {subreddit}')

        delete_query = (
            f"DELETE FROM {self.table_name} "
            f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        )

        self.postgres_execute(delete_query)
        self.postgres_insert(sr_info)
