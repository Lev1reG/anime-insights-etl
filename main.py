import pandas as pd
import difflib
import time
import uuid
from dotenv import load_dotenv
import os
from datetime import date

# Import the Extraction Functions
from src.extract.jikan_extractor import JikanExtractor
from src.extract.anilist_extractor import AnilistExtractor
# Import the Transformation Functions
from src.transform.sentiment_analysis import SentimentAnalyzer
from src.transform.transform_data import TransformAnimeData
# Import the Load Functions
from src.load.load_postgresql import LoadPsql
# Import the Utils
from src.utils import save_data_to_file
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

if __name__ == '__main__':
  jikan_extractor = JikanExtractor()
  anilist_extractor = AnilistExtractor()
  sentiment_analyzer = SentimentAnalyzer()
  transformer = TransformAnimeData(sentiment_analyzer=sentiment_analyzer)
  dataloader = LoadPsql()

  current_year = date.today().year
  current_month = date.today().month

  # Decide season
  if  3 <= current_month <= 5:
    current_season = 'spring'
  elif 6 <= current_month <= 8:
    current_season = 'summer'
  elif 9 <= current_month <= 11:
    current_season = 'fall'
  else:
    current_season = 'winter'

  # UNCOMMENT THIS LATER
  ############### GET DATA ###############
  # Get top 50 anime from Jikan and transform into DataFrame
  top_50_jikan = jikan_extractor.fetch_top_anime()
  top_50_jikan_df = jikan_extractor.extract_to_dataframe(top_50_jikan)
  
  # Get top 50 anime from Anilist and transform into DataFrame
  top_50_anilist = anilist_extractor.fetch_top_anime()
  top_50_anilist_df = anilist_extractor.extract_to_dataframe(top_50_anilist)

  # Get trending 50 anime from Anilist and transform into DataFrame
  trending_50_anilist = anilist_extractor.fetch_trending_anime()
  trending_50_anilist_df = anilist_extractor.extract_to_dataframe(trending_50_anilist)  

  # Get current season data from Anilist
  current_season_anilist = anilist_extractor.fetch_anime_by_season(year=current_year, season=current_season)
  current_season_anilist_df = anilist_extractor.extract_to_dataframe(current_season_anilist)

  # Get current season data from Jikan
  current_season_jikan = jikan_extractor.fetch_anime_by_season(year=current_year, season=current_season)
  current_season_jikan_df = jikan_extractor.extract_to_dataframe(current_season_jikan)

  ############### TRANSFORM DATA ###############
  # Transform top 50 data
  merged_top_data = transformer.merge_and_clean_data(jikan_df=top_50_jikan_df, anilist_df=top_50_anilist_df)

  # Transform current season data
  merged_current_data = transformer.merge_and_clean_data(jikan_df=current_season_jikan_df, anilist_df=current_season_anilist_df)

  # Transform trending 50 data
  trending_50_anilist_df_cleaned = transformer.clean_data(trending_50_anilist_df)

  # Fetch and process reviews for the top 50 data
  top_50_reviews_df = transformer.fetch_and_process_reviews(merged_data=merged_top_data, jikan_extractor=jikan_extractor, anilist_extractor=anilist_extractor)

  # Fetch and process reviews for the trending 50 data
  trending_50_reviews_df = transformer.fetch_and_process_reviews(merged_data=trending_50_anilist_df_cleaned, jikan_extractor=jikan_extractor, anilist_extractor=anilist_extractor)

  # Fetch and process reviews for the current season data
  current_season_reviews_df = transformer.fetch_and_process_reviews(merged_data=merged_current_data, jikan_extractor=jikan_extractor, anilist_extractor=anilist_extractor)

  ############### LOAD/SAVE DATA ###############
  # Save results
  SaveData.save_data_to_file(merged_top_data, 'data/top_anime_data', file_format='csv')
  SaveData.save_data_to_file(trending_50_anilist_df_cleaned, 'data/trending_anime_data', file_format='csv')
  SaveData.save_data_to_file(merged_current_data, 'data/current_anime_data', file_format='csv')
  SaveData.save_data_to_file(top_50_reviews_df, 'data/top_anime_reviews', file_format='csv')
  SaveData.save_data_to_file(trending_50_reviews_df, 'data/trending_anime_reviews', file_format='csv')
  SaveData.save_data_to_file(current_season_reviews_df, 'data/current_season_anime_reviews', file_format='csv')

  # Load transformed data into PostgreSQL
  # dataloader.load_to_postgresql(merged_data, 'anime_data', db_config)
  # dataloader.load_to_postgresql(reviews_df, 'anime_review', db_config)

  print("Data transformation and review processing complete.")