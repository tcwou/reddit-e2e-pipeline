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
DAY_OFFSET = Variable.get("day_offset_daily")

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
    'extract_data',
    default_args=default_args,
    description='ELT for reddit data',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=['reddit'],
) as dag:


    docker_extract_pushshift_to_postgres = DockerOperator(
        task_id=f'docker_extract_pushshift_to_postgres',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"extract_posts_data.py --day_offset {DAY_OFFSET}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_extract_reddit_api_to_postgres = DockerOperator(
        task_id=f'docker_extract_reddit_api_to_postgres',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"update_posts_data.py --day_offset {DAY_OFFSET}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_extract_sr_info_to_postgres = DockerOperator(
        task_id=f'docker_extract_sr_info_to_postgres',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"extract_subreddit_info.py",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_extract_company_info_to_postgres = DockerOperator(
        task_id=f'docker_extract_company_info_to_postgres',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"extract_company_info.py",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_execute_subreddit_aggregate = DockerOperator(
        task_id=f'docker_execute_subreddit_aggregate',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"subreddit_agg.py --day_offset {DAY_OFFSET}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    docker_execute_subreddit_symbol_aggregate = DockerOperator(
        task_id=f'docker_execute_subreddit_symbol_aggregate',
        image=CONFIG['docker_image'],
        api_version='auto',
        auto_remove=True,
        command=f"subreddit_symbol_agg.py --day_offset {DAY_OFFSET}",
        docker_url=CONFIG['docker_url'],
        network_mode="host"
    )

    (
        [docker_extract_sr_info_to_postgres,
         docker_extract_company_info_to_postgres,
         docker_extract_pushshift_to_postgres] >>
        docker_extract_reddit_api_to_postgres >>
        docker_execute_subreddit_aggregate >>
        docker_execute_subreddit_symbol_aggregate
    )
