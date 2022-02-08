from reddit_to_postgres import RedditToPostgres
import datetime
import numpy as np
import pandas as pd
import re

TABLE_NAME = 'public.dim_companies'

rtp = RedditToPostgres(table_name=TABLE_NAME)

"""Load US ticker-company info and insert into DB"""
start_date = datetime.date.today()
end_date = start_date + datetime.timedelta(1)

rtp.postgres_create_partitions(start_date, end_date)
url = "https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=demo"
df = pd.read_csv(url)

# transform data before insertion into DB
df.columns = [re.sub('(?<!^)(?=[A-Z])', '_', col).lower() for col in df.columns]
df['date'] = start_date
df['id'] = df['symbol'] + '_' + df['date'].astype('str')
df['id'] = df['id'].str.replace('-', '')
df = df.reindex(columns=[df.columns[-1]] + [col for col in df.columns[:len(df.columns)-1]])
df = df.replace({np.nan: None})

company_info = df.to_dict('records')

start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")   

delete_query = (
    f"DELETE FROM {TABLE_NAME} "
    f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
)

rtp.postgres_execute(delete_query)
rtp.postgres_insert(company_info)