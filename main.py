import pandas as pd
import difflib
import time
import uuid
# Import the Extraction Functions
from src.extract.jikan_extractor import JikanExtractor
from src.extract.anilist_extractor import AnilistExtractor
# Import the Transformation Functions
from src.transform.sentiment_analysis import SentimentAnalyzer
from src.transform.transform_data import TransformAnimeData
# Import the Utils
from src.utils import save_data_to_file
from src.utils.save_data_to_file import SaveData

if __name__ == '__main__':
  jikan_extractor = JikanExtractor()
  anilist_extractor = AnilistExtractor()
  sentiment_analyzer = SentimentAnalyzer()
  transformer = TransformAnimeData(sentiment_analyzer=sentiment_analyzer)

  # UNCOMMENT THIS LATER
  # Get top 50 anime from Jikan and transform into DataFrame
  top_50_jikan = jikan_extractor.fetch_top_anime()
  top_50_jikan_df = jikan_extractor.extract_to_dataframe(top_50_jikan)
  
  # Get top 50 anime from Anilist and transform into DataFrame
  top_50_anilist = anilist_extractor.fetch_top_anime()
  top_50_anilist_df = anilist_extractor.extract_to_dataframe(top_50_anilist)

  # Transform data
  merged_data = transformer.merge_and_clean_data(top_50_jikan_df, top_50_anilist_df)

  # Fetch and process reviews
  reviews_df = transformer.fetch_and_process_reviews(merged_data, jikan_extractor, anilist_extractor)

  # Save results
  SaveData.save_data_to_file(merged_data, 'data/merged_anime_data', file_format='csv')
  SaveData.save_data_to_file(reviews_df, 'data/anime_reviews_table', file_format='csv')

  print("Data transformation and review processing complete.")