# Reddit Stocks Data Pipeline & Visualization
In this project I extract posts from popular stock investing subreddits, enrich the data with ticker and subreddit information, and aggregate the data. I have created a dashboard to visualize some top level insights about the subreddits and top company & fund tickers.

[Dashboard is available to view here](https://www.wouhoo.net/stocks/) (mobile unfriendly).

Instructions to run the code are provided at the bottom of this readme.

The project makes use of two approaches to data pipelining:
1) On-prem using Docker and Airflow
2) AWS Cloud-based using ECR, Lambda, and Step Functions 

[The dashboard](https://www.wouhoo.net/stocks/) is the end-result of the AWS approach and is hosted on an EC2 instance. The pipeline runs on a daily schedule with a delay of two days to allow upvotes and comments to accumulate.  

# Data Flow
![diagram](https://wouhoo-public.s3.us-east-2.amazonaws.com/Reddit+pipeline+ETL.svg)

Stock ticker info is obtained from the [alphavantage.co API](www.alphavantage.co). (See [extract_company_info.py](https://github.com/tcwou/reddit-e2e-pipeline/blob/main/airflow/scripts/extract_company_info.py).)

Subreddit info is obtained directly from the reddit API. (See [extract_subreddit_info.py](https://github.com/tcwou/reddit-e2e-pipeline/blob/main/airflow/scripts/extract_subreddit_info.py).)

For post submissions the [reddit API](https://www.reddit.com/dev/api/) has a number of limitations, one of which is the inability to pull submissions from a specific subreddit over a specified time range. To overcome this we first obtain the post submissions by subreddit from the [pushshift.io API](https://pushshift.io/api-parameters/). (See [extract_posts_data.py](https://github.com/tcwou/reddit-e2e-pipeline/blob/main/airflow/scripts/extract_posts_data.py)).

The pushshift.io data is captured at the time of submission, so will not have upvote or comment information that we require. The reddit API does support getting post information by id, so we can make a request to the reddit API with the ids that we obtain from pushshift. We also label the post with ticker information using rule based matching, then if the post contains text (a 'selftext' post) we calculate sentiment using a pre-trained model by flair*. (See [update_posts_data.py](https://github.com/tcwou/reddit-e2e-pipeline/blob/main/airflow/scripts/update_posts_data.py)).

  \* the flair model is trained on IMDB reviews and may not fully translate to stock sentiment data.

The various extract data scripts make use of the [reddit_to_postgres class](https://github.com/tcwou/reddit-e2e-pipeline/blob/main/airflow/scripts/reddit_to_postgres.py) which stores the postgres connection and API credential attributes, and provides functions for querying and inserting data, as well as cleaning and transforming the data. 

For analysis purposes two aggregate tables with 'per subreddit' grain and 'subreddit-ticker' grain respectively and created and stored in the postgres data warehouse. These tables are queried by the dashboard.

For reproducibility purposes the tasks in the data flow are contaierized using Docker and can be orchestrated using either Airflow, or AWS Step Functions.

# AWS

![diagram](https://wouhoo-public.s3.us-east-2.amazonaws.com/AWS.svg)

AWS provides a serveless solution to keep the data flow running on the cloud. Eventbridge schedules the start of the flow to run on a daily basis. The DAG is defined as a step function state machine, calling lambda functions for each task. RDS postgres acts as the datawarehouse, and the 'plotly dash' dashboard is hosted on an EC2 instance running apache webserver.

# Run Code (using airflow)

* create docker config file in airflow/configs/, a template is provided.
* create credentials config file in airflow/scripts/configs/ with postgres db and API credentials, a template is provided.
* create requirements.txt and DockerFile within airflow/scripts/ and build Docker image
* set airflow variables as defined in [extract_data_dag_daily.py](https://github.com/tcwou/reddit-e2e-pipeline/blob/main/airflow/dags/extract_data_dag_daily.py). The recommended value for day offset is 2.
* run dashboard defined in [app.py](https://github.com/tcwou/reddit-e2e-pipeline/blob/main/flask/app.py) using either 'python app.py' for test mode, or gunicorn for production

