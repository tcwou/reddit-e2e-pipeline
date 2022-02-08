# Reddit Stocks Data Pipeline & Visualization
In this project I extract posts from popular stock investing subreddits, enrich the data with ticker and subreddit information, and aggregate the data. I have created a dashboard to visualize some top level insights about the subreddits and top company & fund tickers.

[Dashboard is available to view here](https://www.wouhoo.net/stocks/) (mobile unfriendly).

Instructions to run the code are provided at the bottom of this readme.

The project makes use of two approaches to data pipelining:
1) On-prem using Docker and Airflow
2) AWS Cloud-based using ECR, Lambda, and Step Functions 

[The dashboard](https://www.wouhoo.net/stocks/) is the end-result of the AWS approach and is hosted on an EC2 instance. The pipeline runs on a daily schedule with a delay of two days to allow upvotes and comments to accumulate.  

