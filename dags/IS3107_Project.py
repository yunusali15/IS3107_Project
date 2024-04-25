import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


scripts = "/home/yunusali/airflow/project_scripts/"


@task(task_id="fetch_stock_data")
def fetch_stock_data():
    try:
        os.system("python3 " + scripts + 'fetch_stock_data')
        # os.system("python3 /home/yunusali/airflow/project_scripts/fetch_stock_data.py")
    except Exception as e:
        raise Exception(f"Error occurred in fetch_stock_data task: {e}")
    return "Fetched Stock News"



@task(task_id="collect_stock_news")
def collect_stock_news():
    try:
        os.system("python3 " + scripts + "collect_stock_news.py")
    except Exception as e:
        raise Exception(f"Error occurred in collect_stock_news task: {e}")
    return "Collected Stock News"



@task(task_id="generate_sentiment_scores")
def generate_sentiment_scores():
    try:
        os.system("python3 " + scripts + 'generate_sentiment_scores.py')
    except Exception as e:
        raise Exception(f"Error occurred in generate_sentiment_scores task: {e}")
    return "Generated Sentiment Report"



@task(task_id="generate_stock_analysis")
def generate_stock_analysis():
    try:
       os.system("python3 " + scripts + "generate_stock_analysis.py")
    except Exception as e:
        raise Exception(f"Error occurred in generate_stock_analysis task: {e}")
    return "Generated Stock Analysis"


@task(task_id="cleanup_files")
def cleanup_files():
    try:
        os.system("python3 " + scripts + 'cleanup_files.py')
    except Exception as e:
        raise Exception(f"Error occurred in cleanup_files task: {e}")
    return "Cleaned Files Up"



@dag(start_date=datetime(2024, 1, 1), catchup=False, tags=["project"])
def generate_sentiment_stock_analysis():
    fetch_data = fetch_stock_data()
    collect_news = collect_stock_news()
    generate_sentiment = generate_sentiment_scores()
    generate_analysis = generate_stock_analysis()
    cleanup = cleanup_files()

    fetch_data >> collect_news >> generate_sentiment >> generate_analysis >>  cleanup

generate_stock_analysis_dag = generate_sentiment_stock_analysis()
