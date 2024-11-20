from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Get the absolute path of the `src` folder, relative to the DAG file
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import your existing functions and modules
from src.extract.jikan_extractor import JikanExtractor
from src.extract.anilist_extractor import AnilistExtractor
from src.transform.sentiment_analysis import SentimentAnalyzer
from src.transform.transform_data import TransformAnimeData
from src.load.load_postgresql import LoadPsql
from src.utils.save_data_to_file import SaveData

# Helper functions for Airflow tasks
def extract_and_save_jikan_top_50():
    jikan_extractor = JikanExtractor()
    top_50_jikan = jikan_extractor.fetch_top_anime()
    top_50_jikan_df = jikan_extractor.extract_to_dataframe(top_50_jikan)
    SaveData.save_data_to_file(top_50_jikan_df, 'data/top_50_jikan', file_format='csv')

def extract_and_save_anilist_top_50():
    anilist_extractor = AnilistExtractor()
    top_50_anilist = anilist_extractor.fetch_top_anime()
    top_50_anilist_df = anilist_extractor.extract_to_dataframe(top_50_anilist)
    SaveData.save_data_to_file(top_50_anilist_df, 'data/top_50_anilist', file_format='csv')

def transform_and_save_top_50():
    jikan_df = pd.read_csv('data/top_50_jikan.csv')
    anilist_df = pd.read_csv('data/top_50_anilist.csv')
    transformer = TransformAnimeData(sentiment_analyzer=SentimentAnalyzer())
    merged_top_data = transformer.merge_and_clean_data(jikan_df, anilist_df)
    SaveData.save_data_to_file(merged_top_data, 'data/merged_top_50', file_format='csv')

def fetch_and_save_reviews():
    merged_data = pd.read_csv('data/merged_top_50.csv')
    jikan_extractor = JikanExtractor()
    anilist_extractor = AnilistExtractor()
    transformer = TransformAnimeData(sentiment_analyzer=SentimentAnalyzer())
    reviews_df = transformer.fetch_and_process_reviews(merged_data, jikan_extractor, anilist_extractor)
    SaveData.save_data_to_file(reviews_df, 'data/top_50_reviews', file_format='csv')

def load_to_postgresql():
    db_config = {
        'host': os.getenv('DB_HOST'),
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': 5432
    }
    merged_data = pd.read_csv('data/merged_top_50.csv')
    reviews_df = pd.read_csv('data/top_50_reviews.csv')
    dataloader = LoadPsql()
    dataloader.load_to_postgresql(merged_data, 'anime_data', db_config)
    dataloader.load_to_postgresql(reviews_df, 'anime_review', db_config)

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

# Define the DAG
with DAG(
    'anime_pipeline',
    default_args=default_args,
    description='Anime data ETL pipeline using Jikan and Anilist APIs',
    schedule_interval=None,  # Set to desired schedule interval, e.g., '@daily'
    start_date=datetime(2024, 11, 1),
    catchup=False,
) as dag:
    
    extract_jikan_top_50 = PythonOperator(
        task_id='extract_jikan_top_50',
        python_callable=extract_and_save_jikan_top_50
    )

    extract_anilist_top_50 = PythonOperator(
        task_id='extract_anilist_top_50',
        python_callable=extract_and_save_anilist_top_50
    )

    transform_top_50 = PythonOperator(
        task_id='transform_top_50',
        python_callable=transform_and_save_top_50
    )

    fetch_reviews = PythonOperator(
        task_id='fetch_reviews',
        python_callable=fetch_and_save_reviews
    )

    load_data = PythonOperator(
        task_id='load_to_postgresql',
        python_callable=load_to_postgresql
    )

    # Define task dependencies
    [extract_jikan_top_50, extract_anilist_top_50] >> transform_top_50 >> fetch_reviews >> load_data
