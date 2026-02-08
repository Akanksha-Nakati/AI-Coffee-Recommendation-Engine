"""
Complete Coffee Pipeline: Scraping → Databricks Processing → Embeddings
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'akanksha',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'coffee_full_pipeline',
    default_args=default_args,
    description='End-to-end coffee recommendation pipeline',
    schedule_interval='0 2 * * 0',  # Weekly
    start_date=days_ago(1),
    catchup=False,
    tags=['coffee', 'etl', 'ml'],
)

# Stage 1: Data Ingestion
start = DummyOperator(task_id='start', dag=dag)

scrape_cafes = BashOperator(
    task_id='scrape_cafe_menus',
    bash_command='python /opt/airflow/local_scrapers/scrape_starbucks.py && python /opt/airflow/local_scrapers/scrape_dunkin.py',
    dag=dag,
)

scrape_reddit = BashOperator(
    task_id='scrape_reddit_discussions',
    bash_command='python /opt/airflow/local_scrapers/scrape_reddit.py',
    dag=dag,
)

ingestion_complete = DummyOperator(task_id='ingestion_complete', dag=dag)

# Stage 2: Data Quality & Validation
def validate_data():
    """Validate scraped data meets quality thresholds"""
    import json
    from glob import glob
    
    checks = {}
    
    # Check minimum data requirements
    starbucks = glob('/opt/airflow/data/raw/starbucks/*.json')
    if starbucks:
        with open(starbucks[-1]) as f:
            checks['starbucks'] = len(json.load(f)) >= 50
    
    reddit = glob('/opt/airflow/data/raw/reddit/*.json')
    if reddit:
        with open(reddit[-1]) as f:
            checks['reddit'] = len(json.load(f)) >= 100
    
    if not all(checks.values()):
        raise Exception(f"Data quality check failed: {checks}")
    
    print("✅ All data quality checks passed")

validate = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data,
    dag=dag,
)

# Stage 3: Databricks Processing (Placeholder)
# TODO: Add Databricks API calls when ready
process_bronze = DummyOperator(task_id='process_to_bronze', dag=dag)
process_silver = DummyOperator(task_id='process_to_silver', dag=dag)
process_gold = DummyOperator(task_id='process_to_gold', dag=dag)

# Stage 4: ML Pipeline (Placeholder)
generate_embeddings = DummyOperator(task_id='generate_embeddings', dag=dag)
build_vector_index = DummyOperator(task_id='build_vector_index', dag=dag)

# Stage 5: Completion
def send_completion_notification():
    """Log pipeline completion"""
    print("🎉 Pipeline completed successfully!")
    print(f"Timestamp: {datetime.now().isoformat()}")

complete = PythonOperator(
    task_id='pipeline_complete',
    python_callable=send_completion_notification,
    dag=dag,
)

# Define workflow
start >> [scrape_cafes, scrape_reddit] >> ingestion_complete
ingestion_complete >> validate
validate >> process_bronze >> process_silver >> process_gold
process_gold >> generate_embeddings >> build_vector_index >> complete