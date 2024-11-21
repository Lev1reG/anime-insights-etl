from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, date
from dotenv import load_dotenv
import os
import sys
import datetime
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../')))

# Import the modules and functions from your project
from src.extract.jikan_extractor import JikanExtractor
from src.extract.anilist_extractor import AnilistExtractor
from src.transform.sentiment_analysis import SentimentAnalyzer
from src.transform.transform_data import TransformAnimeData
from src.load.load_postgresql import LoadPsql
from src.utils.save_data_to_file import SaveData

load_dotenv()

# Database configuration
db_config = {
    'host': os.getenv('DB_HOST'),
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': 5432
}

jikan_extractor = JikanExtractor()
anilist_extractor = AnilistExtractor()
sentiment_analyzer = SentimentAnalyzer()
transformer = TransformAnimeData(sentiment_analyzer=sentiment_analyzer)
dataloader = LoadPsql()

# Define Python callables for each task (replace with actual functions)
# Extract, Transform, Load methods as defined previously...

# Define the DAG
with DAG(
    'anime_etl_dag',
    default_args={'retries': 1},
    description='ETL pipeline for anime data and reviews',
    schedule_interval='@daily',
    start_date=datetime.datetime(2024, 11, 20),
    catchup=False,
) as dag:
    
    # Top Anime Extract
    def extract_top_anilist():
        top_50_anilist = anilist_extractor.fetch_top_anime()
        top_50_anilist_df = anilist_extractor.extract_to_dataframe(top_50_anilist)
        SaveData.save_data_to_file(top_50_anilist_df, '/tmp/top_50_anilist', 'csv')

    extract_top_anilist_task = PythonOperator(
        task_id='extract_top_anilist_task',
        python_callable=extract_top_anilist
    )

    def extract_top_jikan():
        top_50_jikan = jikan_extractor.fetch_top_anime()
        top_50_jikan_df = jikan_extractor.extract_to_dataframe(top_50_jikan)
        SaveData.save_data_to_file(top_50_jikan_df, '/tmp/top_50_jikan', 'csv')

    extract_top_jikan_task = PythonOperator(
        task_id='extract_top_jikan_task',
        python_callable=extract_top_jikan
    )

    # Current Season Anime Extract
    def extract_current_anilist():
        current_year = date.today().year
        current_month = date.today().month
        if 3 <= current_month <= 5:
            current_season = 'SPRING'
        elif 6 <= current_month <= 8:
            current_season = 'SUMMER'
        elif 9 <= current_month <= 11:
            current_season = 'FALL'
        else:
            current_season = 'WINTER'
        current_season_anilist = anilist_extractor.fetch_anime_by_season(year=current_year, season=current_season)
        current_season_anilist_df = anilist_extractor.extract_to_dataframe(current_season_anilist)
        SaveData.save_data_to_file(current_season_anilist_df, '/tmp/current_season_anilist', 'csv')

    extract_current_anilist_task = PythonOperator(
        task_id='extract_current_anilist_task',
        python_callable=extract_current_anilist
    )

    def extract_current_jikan():
        current_year = date.today().year
        current_month = date.today().month
        if 3 <= current_month <= 5:
            current_season = 'spring'
        elif 6 <= current_month <= 8:
            current_season = 'summer'
        elif 9 <= current_month <= 11:
            current_season = 'fall'
        else:
            current_season = 'winter'
        current_season_jikan = jikan_extractor.fetch_anime_by_season(year=current_year, season=current_season)
        current_season_jikan_df = jikan_extractor.extract_to_dataframe(current_season_jikan)
        SaveData.save_data_to_file(current_season_jikan_df, '/tmp/current_season_jikan', 'csv')

    extract_current_jikan_task = PythonOperator(
        task_id='extract_current_jikan_task',
        python_callable=extract_current_jikan
    )

    # Extract Trending Anilist
    def extract_trending_anilist():
        trending_50_anilist = anilist_extractor.fetch_trending_anime()
        trending_50_anilist_df = anilist_extractor.extract_to_dataframe(trending_50_anilist)
        SaveData.save_data_to_file(trending_50_anilist_df, '/tmp/trending_50_anilist', 'csv')

    extract_trending_anilist_task = PythonOperator(
        task_id='extract_trending_anilist_task',
        python_callable=extract_trending_anilist
    )

    # Merge Top Anime Data
    def merge_top_data():
        jikan_df = pd.read_csv('/tmp/top_50_jikan.csv')
        anilist_df = pd.read_csv('/tmp/top_50_anilist.csv')
        merged_top_data = transformer.merge_and_clean_data(jikan_df=jikan_df, anilist_df=anilist_df)
        SaveData.save_data_to_file(merged_top_data, '/tmp/merged_top_data', 'csv')

    merge_top_data_task = PythonOperator(
        task_id='merge_top_data_task',
        python_callable=merge_top_data
    )

    # Merge Current Season Anime Data
    def merge_current_data():
        jikan_df = pd.read_csv('/tmp/current_season_jikan.csv')
        anilist_df = pd.read_csv('/tmp/current_season_anilist.csv')
        merged_current_data = transformer.merge_and_clean_data(jikan_df=jikan_df, anilist_df=anilist_df)
        SaveData.save_data_to_file(merged_current_data, '/tmp/merged_current_data', 'csv')

    merge_current_data_task = PythonOperator(
        task_id='merge_current_data_task',
        python_callable=merge_current_data
    )

    # Clean Trending Data
    def clean_data_trending():
        trending_df = pd.read_csv('/tmp/trending_50_anilist.csv')
        trending_50_anilist_df_cleaned = transformer.clean_data(trending_df)
        SaveData.save_data_to_file(trending_50_anilist_df_cleaned, '/tmp/cleaned_trending_data', 'csv')

    clean_data_trending_task = PythonOperator(
        task_id='clean_data_trending_task',
        python_callable=clean_data_trending
    )

    # Extract Top Anime Reviews
    def extract_top_reviews():
        merged_top_df = pd.read_csv('/tmp/merged_top_data.csv')
        top_50_reviews_df = transformer.fetch_and_process_reviews(merged_data=merged_top_df, jikan_extractor=jikan_extractor, anilist_extractor=anilist_extractor)
        SaveData.save_data_to_file(top_50_reviews_df, '/tmp/top_50_reviews', 'csv')

    extract_top_reviews_task = PythonOperator(
        task_id='extract_top_reviews_task',
        python_callable=extract_top_reviews
    )

    # Extract Trending Anime Reviews
    def extract_trending_reviews():
        merged_trending_df = pd.read_csv('/tmp/cleaned_trending_data.csv')
        trending_50_reviews_df = transformer.fetch_and_process_reviews(merged_data=merged_trending_df, jikan_extractor=jikan_extractor, anilist_extractor=anilist_extractor)
        SaveData.save_data_to_file(trending_50_reviews_df, '/tmp/trending_50_reviews', 'csv')

    extract_trending_reviews_task = PythonOperator(
        task_id='extract_trending_reviews_task',
        python_callable=extract_trending_reviews
    )
    
    # Extract Current Season Reviews
    def extract_current_season_reviews():
        merged_current_df = pd.read_csv('/tmp/merged_current_data.csv')
        current_season_reviews_df = transformer.fetch_and_process_reviews(merged_data=merged_current_df, jikan_extractor=jikan_extractor, anilist_extractor=anilist_extractor)
        SaveData.save_data_to_file(current_season_reviews_df, '/tmp/current_season_reviews', 'csv')

    extract_current_season_reviews_task = PythonOperator(
        task_id='extract_current_season_reviews_task',
        python_callable=extract_current_season_reviews
    )

    # Load Top Data
    def load_top_anime_data():
        merged_top_data_df = pd.read_csv('/tmp/merged_top_data.csv')
        dataloader.load_to_postgresql(merged_top_data_df, 'top_anime_data', db_config)

    load_top_anime_data_task = PythonOperator(
        task_id='load_top_anime_data_task',
        python_callable=load_top_anime_data
    )

    # Load Top Reviews
    def load_top_anime_reviews():
        merged_top_reviews_df = pd.read_csv('/tmp/top_50_reviews.csv')
        dataloader.load_to_postgresql(merged_top_reviews_df, 'top_anime_reviews', db_config)

    load_top_anime_reviews_task = PythonOperator(
        task_id='load_top_anime_reviews_task',
        python_callable=load_top_anime_reviews
    )

    # Load Current Season Data
    def load_current_season_anime_data():
        merged_current_season_data_df = pd.read_csv('/tmp/merged_current_data.csv')
        dataloader.load_to_postgresql(merged_current_season_data_df, 'current_season_anime_data', db_config)

    load_current_season_anime_data_task = PythonOperator(
        task_id='load_current_season_anime_data_task',
        python_callable=load_current_season_anime_data
    )

    # Load Current Season Reviews
    def load_current_season_anime_reviews():
        merged_current_season_reviews_df = pd.read_csv('/tmp/current_season_reviews.csv')
        dataloader.load_to_postgresql(merged_current_season_reviews_df, 'current_season_anime_reviews', db_config)

    load_current_season_anime_reviews_task = PythonOperator(
        task_id='load_current_season_anime_reviews_task',
        python_callable=load_current_season_anime_reviews
    )

    # Load Trending Data
    def load_trending_anime_data():
        trending_data_df = pd.read_csv('/tmp/cleaned_trending_data.csv')
        dataloader.load_to_postgresql(trending_data_df, 'trending_anime_data', db_config)

    load_trending_anime_data_task = PythonOperator(
        task_id='load_trending_anime_data_task',
        python_callable=load_trending_anime_data
    )

    # Load Trending Reviews
    def load_trending_anime_reviews():
        trending_reviews_df = pd.read_csv('/tmp/trending_50_reviews.csv')
        dataloader.load_to_postgresql(trending_reviews_df, 'trending_anime_reviews', db_config)

    load_trending_anime_reviews_task = PythonOperator(
        task_id='load_trending_anime_reviews_task',
        python_callable=load_trending_anime_reviews
    )

# Define dependencies
# Top Anime Data Flow
[extract_top_anilist_task, extract_top_jikan_task] >> merge_top_data_task
merge_top_data_task >> extract_top_reviews_task
extract_top_reviews_task >> [load_top_anime_data_task, load_top_anime_reviews_task]

# Current Season Anime Data Flow
[extract_current_anilist_task, extract_current_jikan_task] >> merge_current_data_task
merge_current_data_task >> extract_current_season_reviews_task
extract_current_season_reviews_task >> [load_current_season_anime_data_task, load_current_season_anime_reviews_task]

# Trending Anime Data Flow
extract_trending_anilist_task >> clean_data_trending_task
clean_data_trending_task >> extract_trending_reviews_task
extract_trending_reviews_task >> [load_trending_anime_data_task, load_trending_anime_reviews_task]

