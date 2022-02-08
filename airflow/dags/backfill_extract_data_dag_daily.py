from datetime import timedelta
import json

from airflow import DAG

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

# import airflow variables
OWNER = Variable.get("owner")
EMAIL = Variable.get("email")
AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
END_DATE = Variable.get("backfill_end_date")
START_DATE_DELTA = Variable.get("start_date_delta_backfill")


with open(f"{AIRFLOW_HOME}/configs/docker_extract_load_postgres_config.json", "r") as f:
    CONFIG = json.load(f)

default_args = {
    'owner': OWNER,
    'depends_on_past': False,
    'email': EMAIL,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'backfill_extract_data',
    default_args=default_args,
    description='Backfill ELT for reddit data',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['reddit'],
) as dag:


    docker_extract_pushshift_to_postgres = DockerOperator(
        task_id=f'docker_extract_pushshift_to_postgres',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"extract_posts_data.py --end_date {END_DATE} --start_date_delta {START_DATE_DELTA}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_extract_reddit_api_to_postgres = DockerOperator(
        task_id=f'docker_extract_reddit_api_to_postgres',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"update_posts_data.py --end_date {END_DATE} --start_date_delta {START_DATE_DELTA}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_execute_subreddit_aggregate = DockerOperator(
        task_id=f'docker_execute_subreddit_aggregate',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"subreddit_agg.py --end_date {END_DATE} --start_date_delta {START_DATE_DELTA}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_execute_subreddit_symbol_aggregate = DockerOperator(
        task_id=f'docker_execute_subreddit_symbol_aggregate',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"subreddit_symbol_agg.py --end_date {END_DATE} --start_date_delta {START_DATE_DELTA}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    (
        docker_extract_pushshift_to_postgres >>
        docker_extract_reddit_api_to_postgres >>
        docker_execute_subreddit_aggregate >>
        docker_execute_subreddit_symbol_aggregate
    )
