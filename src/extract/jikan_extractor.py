import requests
import pandas as pd
import logging

pd.set_option("display.max_columns", 200)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

BASE_URL = "https://api.jikan.moe/v4"


class JikanExtractor:
    def __init__(self):
        self.base_url = BASE_URL

    def fetch_anime_by_season(self, year, season, page=1):
        """
        Fetch anime by year and season from Jikan API.

        Args:
            year (int): The year of the anime season.
            season (str): The season (e.g., 'winter', 'spring', 'summer', 'fall').
            page (int): The page number of the results.

        Returns:
            dict: JSON response containing anime data.
        """
        endpoint = f"{self.base_url}/seasons/{year}/{season}"
        params = {"page": page}

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            logging.info(
                f"Successfully fetched anime data for {season} {year}, page {page}."
            )
            data = response.json()
            anime_list = data["data"]
            records = []
            for anime in anime_list:
                records.append(
                    {
                        "mal_id": anime.get("mal_id"),
                        "title": anime.get("title"),
                        "status": anime.get("status"),
                        "type": anime.get("type"),
                        "episodes": anime.get("episodes"),
                        "score": anime.get("score"),
                        "start_date": anime.get("aired", {}).get("from"),
                        "end_date": anime.get("aired", {}).get("to"),
                        "popularity": anime.get("popularity"),
                        "favorites": anime.get("favorites"),
                        "genres": [genre["name"] for genre in anime.get("genres", [])],
                        "rating": anime.get("rating"),
                        "producers": [
                            producer["name"] for producer in anime.get("producers", [])
                        ],
                        "studios": [
                            studio["name"] for studio in anime.get("studios", [])
                        ],
                    }
                )

            return records
        except requests.exceptions.RequestException as e:
            logging.error(f"Error while fetching anime data: {e}")
            return None

    def fetch_anime_reviews(self, mal_id, page=1):
        """
        Fetch reviews for a specific anime from Jikan API.
        
        Args:
            mal_id (int): The MyAnimeList ID of the anime.
            page (int): The page number of the reviews to fetch.

        Returns:
            list: List of dictionaries containing anime reviews.
        """
        endpoint = f"{self.base_url}/anime/{mal_id}/reviews"
        params = {"page": page}

        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()
            logging.info(f"Successfully fetched reviews for anime ID {mal_id}, page {page}.")
            data = response.json()
            review_list = data["data"]
            records = []
            for review in review_list:
                records.append(
                    {
                        "mal_id": mal_id,
                        "username": review.get("user", {}).get("username"),
                        "tags": review.get("tags", []),
                        "episodes_watched": review.get("episodes_watched"),
                        "review": review.get("review"),
                        "score": review.get("score"),
                    }
                )
            return records
        except requests.exceptions.RequestException as e:
            logging.error(f"Error while fetching reviews for anime ID {mal_id}: {e}")
            return None

    def extract_to_dataframe(self, data):
        """
        Convert JSON data or list into a DataFrame.

        Args:
            data (dict or list): The JSON data or list to be transformed into a DataFrame.

        Returns:
            DataFrame: Pandas DataFrame containing the data.
        """
        if isinstance(data, dict):
            if "data" not in data:
                logging.error("No valid data to extract to DataFrame.")
                return pd.DataFrame()
            data = data["data"]
        elif not isinstance(data, list):
            logging.error("Input data must be a dictionary or a list.")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        logging.info("Successfully extracted data to DataFrame.")
        return df


if __name__ == "__main__":
    extractor = JikanExtractor()

    # Fetch anime by season
    year = 2021
    season = "summer"
    anime_data = extractor.fetch_anime_by_season(year, season, page=1)

    if anime_data:
        anime_df = extractor.extract_to_dataframe(anime_data)
        print(anime_df.head())

        # Fetch and print reviews for the first anime in the DataFrame
        if not anime_df.empty:
            mal_id = anime_df.iloc[0]['mal_id']
            reviews_data = extractor.fetch_anime_reviews(mal_id, page=1)
            if reviews_data:
                reviews_df = extractor.extract_to_dataframe(reviews_data)
                print(reviews_df.head())
