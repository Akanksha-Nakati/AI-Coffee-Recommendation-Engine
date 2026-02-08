"""
Coffee Recommender Data Pipeline
Orchestrates scraping, processing, and embedding generation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import sys
import os

# Add scrapers to path
sys.path.insert(0, '/opt/airflow/local_scrapers')

# Default arguments for all tasks
default_args = {
    'owner': 'akanksha',
    'email': ['akankshanakatii@gmail.com'],
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'coffee_data_pipeline',
    default_args=default_args,
    description='Scrape café menus and Reddit discussions weekly',
    schedule_interval='0 2 * * 0',  # Every Sunday at 2am
    start_date=days_ago(1),
    catchup=False,
    tags=['coffee', 'scraping', 'data-engineering'],
)


# Task 1: Scrape Starbucks
def scrape_starbucks_task():
    """Scrape Starbucks menu"""
    from scrape_starbucks import StarbucksScraper, save_to_json
    
    print("🚀 Starting Starbucks scraping...")
    scraper = StarbucksScraper(headless=True)
    
    try:
        drinks = scraper.scrape_all()
        if drinks:
            save_to_json(drinks, 'starbucks')
            print(f"✅ Successfully scraped {len(drinks)} drinks")
        else:
            print("⚠️ No drinks scraped")
    finally:
        scraper.close()


scrape_starbucks = PythonOperator(
    task_id='scrape_starbucks',
    python_callable=scrape_starbucks_task,
    dag=dag,
)


# Task 2: Scrape Dunkin
def scrape_dunkin_task():
    """Scrape Dunkin menu"""
    from scrape_dunkin import DunkinScraper, save_to_json
    
    print("🚀 Starting Dunkin scraping...")
    scraper = DunkinScraper(headless=True)
    
    try:
        drinks = scraper.scrape_all()
        if drinks:
            save_to_json(drinks, 'dunkin')
            print(f"✅ Successfully scraped {len(drinks)} drinks")
    finally:
        scraper.close()


scrape_dunkin = PythonOperator(
    task_id='scrape_dunkin',
    python_callable=scrape_dunkin_task,
    dag=dag,
)


# Task 3: Scrape Reddit
def scrape_reddit_task():
    """Scrape Reddit discussions"""
    from scrape_reddit import RedditCoffeeScraper, save_to_json
    
    print("🚀 Starting Reddit scraping...")
    scraper = RedditCoffeeScraper()
    
    discussions = scraper.scrape_all_subreddits()
    
    if discussions:
        save_to_json(discussions)
        print(f"✅ Successfully scraped {len(discussions)} discussions")
    else:
        print("⚠️ No discussions scraped")


scrape_reddit = PythonOperator(
    task_id='scrape_reddit',
    python_callable=scrape_reddit_task,
    dag=dag,
)


# Task 4: Data Quality Check
def data_quality_check():
    """Verify scraped data quality"""
    import json
    from glob import glob
    
    print("🔍 Running data quality checks...")
    
    # Check Starbucks data
    starbucks_files = glob('/opt/airflow/data/raw/starbucks/*.json')
    if starbucks_files:
        with open(starbucks_files[-1], 'r') as f:
            starbucks_data = json.load(f)
        print(f"✅ Starbucks: {len(starbucks_data)} drinks")
        
        if len(starbucks_data) < 50:
            print("⚠️ Warning: Low drink count for Starbucks")
    else:
        raise Exception("❌ No Starbucks data found!")
    
    # Check Dunkin data
    dunkin_files = glob('/opt/airflow/data/raw/dunkin/*.json')
    if dunkin_files:
        with open(dunkin_files[-1], 'r') as f:
            dunkin_data = json.load(f)
        print(f"✅ Dunkin: {len(dunkin_data)} drinks")
    
    # Check Reddit data
    reddit_files = glob('/opt/airflow/data/raw/reddit/*.json')
    if reddit_files:
        with open(reddit_files[-1], 'r') as f:
            reddit_data = json.load(f)
        print(f"✅ Reddit: {len(reddit_data)} discussions")
        
        if len(reddit_data) < 100:
            print("⚠️ Warning: Low discussion count")
    else:
        raise Exception("❌ No Reddit data found!")
    
    print("✅ All quality checks passed!")


quality_check = PythonOperator(
    task_id='data_quality_check',
    python_callable=data_quality_check,
    dag=dag,
)


# Task 5: Generate Summary Report
def generate_summary():
    """Generate summary of scraped data"""
    import json
    from glob import glob
    from datetime import datetime
    
    summary = {
        'pipeline_run': datetime.now().isoformat(),
        'data_sources': {}
    }
    
    # Starbucks
    starbucks_files = glob('/opt/airflow/data/raw/starbucks/*.json')
    if starbucks_files:
        with open(starbucks_files[-1], 'r') as f:
            data = json.load(f)
        summary['data_sources']['starbucks'] = {
            'count': len(data),
            'file': starbucks_files[-1]
        }
    
    # Dunkin
    dunkin_files = glob('/opt/airflow/data/raw/dunkin/*.json')
    if dunkin_files:
        with open(dunkin_files[-1], 'r') as f:
            data = json.load(f)
        summary['data_sources']['dunkin'] = {
            'count': len(data),
            'file': dunkin_files[-1]
        }
    
    # Reddit
    reddit_files = glob('/opt/airflow/data/raw/reddit/*.json')
    if reddit_files:
        with open(reddit_files[-1], 'r') as f:
            data = json.load(f)
        summary['data_sources']['reddit'] = {
            'count': len(data),
            'file': reddit_files[-1]
        }
    
    # Save summary
    summary_path = '/opt/airflow/data/pipeline_summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print("\n📊 Pipeline Summary:")
    print(json.dumps(summary, indent=2))


summary_report = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary,
    dag=dag,
)


# Define task dependencies
# Scraping tasks run in parallel, then quality check, then summary
[scrape_starbucks, scrape_dunkin, scrape_reddit] >> quality_check >> summary_report