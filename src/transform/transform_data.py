import pandas as pd
import uuid
import difflib
from thefuzz import fuzz
from datetime import datetime

class TransformAnimeData:
    def __init__(self, sentiment_analyzer):
        self.sentiment_analyzer = sentiment_analyzer

    @staticmethod
    def find_similar_titles(row):
        """
        Compare titles from Jikan and Anilist for similarity.
        Returns the most likely unified title.
        """
        title_jikan = row['title_jikan']
        title_anilist = row['title_anilist']

        if pd.notna(title_jikan) and pd.notna(title_anilist):
            title_jikan = title_jikan.lower()
            title_anilist = title_anilist.lower()
            # similarity = difflib.SequenceMatcher(None, title_jikan, title_anilist).ratio()
            similarity = fuzz.ratio(title_jikan, title_anilist)
            
            if similarity > 60:  # High similarity threshold
                return title_jikan  # Keep Jikan's title
            else:
                # Titles are significantly different; keep both for manual review
                return f"{title_jikan} / {title_anilist}"
            
        return title_jikan or title_anilist
    
    def clean_data(self, data_df):
        """Clean the trending data fetched from the Anilist API."""
        # Rename and adjust columns to match the unified format
        data_df.rename(columns={'anime_title': 'title', 'average_score': 'score'}, inplace=True)
        data_df['score'] = data_df['score'] / 10  # Normalize score to match Jikan's scale (0-10)

        # Handle missing values (if any)
        data_df.dropna(subset=['mal_id', 'title'], inplace=True)  # Ensure essential columns are not missing

        # Add additional columns
        current_date = datetime.now().date()
        data_df['fetched_date'] = current_date

        # Add a unique anime ID for internal reference, if not already present
        if 'anime_id' not in data_df.columns:
            data_df.insert(0, 'anime_id', [str(uuid.uuid4()) for _ in range(len(data_df))])

        return data_df



    def merge_and_clean_data(self, jikan_df, anilist_df):
        """Merge and clean the data from Jikan and Anilist."""
        # Ensure consistent types
        jikan_df['mal_id'] = jikan_df['mal_id'].astype(str)
        anilist_df['mal_id'] = anilist_df['mal_id'].astype('Int64').astype(str)

        # Problems:
        # Anilist:
        # - mal_id
        # - anime_title
        # - average_score: 91, 92

        # Jikan
        # - mal_id
        # - title
        # - score: 9.32, 9.13

        # Rename and adjust Anilist columns
        anilist_df.rename(columns={'anime_title': 'title', 'average_score': 'score'}, inplace=True)
        anilist_df['score'] = anilist_df['score'] / 10

        # Merge data
        merged_data = pd.merge(
            jikan_df, 
            anilist_df, 
            on='mal_id', 
            how='outer', 
            suffixes=('_jikan', '_anilist')
        )

        # Combine titles
        # merged_data['title'] = merged_data['title_jikan'].combine_first(merged_data['title_anilist'])
        merged_data['title'] = merged_data.apply(self.find_similar_titles, axis=1)

        # Calculate average score
        merged_data['score'] = merged_data[['score_jikan', 'score_anilist']].mean(axis=1)
        merged_data['score'] = merged_data['score'].fillna(merged_data['score_jikan'])
        merged_data['score'] = merged_data['score'].fillna(merged_data['score_anilist'])

        # Drop unnecessary columns
        columns_to_drop = [
            'title_jikan', 'title_anilist', 'score_jikan', 'score_anilist',
            'type', 'episodes', 'start_date', 'end_date', 'popularity_jikan',
            'watching_jikan', 'completed_jikan', 'on_hold_jikan', 'dropped_jikan',
            'plan_to_watch_jikan', 'total', 'anime_id', 'popularity_anilist', 'dropped_anilist',
            'on_hold_anilist', 'plan_to_watch_anilist', 'scored_by'
        ]
        merged_data.drop(columns=columns_to_drop, inplace=True, errors='ignore')

        merged_data.dropna(inplace=True)

        current_date = datetime.now().date()
        merged_data['fetched_date'] = current_date

        merged_data.insert(0, 'anime_id', [str(uuid.uuid4()) for _ in range(len(merged_data))])

        return merged_data

    def fetch_and_process_reviews(self, merged_data, jikan_extractor, anilist_extractor, review_limit=3):
        """Fetch and process reviews for the merged data."""
        reviews_table = []

        for index, row in merged_data.iterrows():
            if index == 3: break
            mal_id = row['mal_id']
            anime_title = row['title']
            print(f"Fetching reviews for: {anime_title} (MAL ID: {mal_id})")

            # Fetch reviews from Jikan
            try:
                jikan_reviews = jikan_extractor.fetch_anime_reviews(mal_id=mal_id, page_limit=review_limit)
                for review in jikan_reviews:
                    reviews_table.append({
                        'review_id': str(uuid.uuid4()),
                        'mal_id': mal_id,
                        'review': review['review'],
                        'source': 'Jikan'
                    })
            except Exception as e:
                print(f"Error fetching Jikan reviews for {anime_title}: {e}")

            # Fetch reviews from Anilist
            try:
                anilist_reviews = anilist_extractor.fetch_anime_reviews_by_mal_id(mal_id=mal_id, page_limit=review_limit)
                for review in anilist_reviews:
                    reviews_table.append({
                        'review_id': str(uuid.uuid4()),
                        'mal_id': mal_id,
                        'review': review['review_body'],
                        'source': 'Anilist'
                    })
            except Exception as e:
                print(f"Error fetching Anilist reviews for {anime_title}: {e}")

        # Create a DataFrame
        reviews_df = pd.DataFrame(reviews_table)
        reviews_df['review'] = reviews_df['review'].str.replace('\n', ' ', regex=False)
        reviews_df['review'] = reviews_df['review'].str.slice(0, 2000) + '...'

        # Add sentiment analysis
        reviews_df['sentiment_score'] = reviews_df['review'].apply(lambda r: self.sentiment_analyzer.analyze_sentiment(r)[0])
        reviews_df['sentiment_label'] = reviews_df['review'].apply(lambda r: self.sentiment_analyzer.analyze_sentiment(r)[1])

        reviews_df.dropna(inplace=True)

        current_date = datetime.now().date()
        reviews_df['fetched_date'] = current_date

        return reviews_df