from airflow import DAG
from airflow.operators.bash import BashOperator
import subprocess
from datetime import datetime
import pendulum

with DAG(
    "job_crawler",
    description="Crawl job data from various sources",
    start_date=pendulum.now('Asia/Saigon').subtract(days=1).start_of("day"),
    schedule_interval='0 8,17 * * *', # at morning and afternoon
    catchup=False,
    max_active_runs=2,  # 2 run song song
) as dag:
    topcv = BashOperator(
        task_id="topcv_crawler",
        bash_command=(
            "cd /opt/airflow/scripts/bronze && "
            "python3 topcv_crawler.py"  
        ),
    )

    careerviet = BashOperator(
        task_id="careerviet_crawler",
        bash_command=(
            "cd /opt/airflow/scripts/bronze && "
            "python3 careerviet_crawler.py"
        ),
    )

    vietnamworks = BashOperator(
        task_id="vietnamworks_crawler",
        bash_command=(
            "cd /opt/airflow/scripts/bronze && "
            "python3 vietnamworks_crawler.py"
        ),
    )