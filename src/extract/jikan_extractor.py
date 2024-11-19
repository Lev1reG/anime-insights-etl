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

    def fetch_anime_by_season(self, year, season):
        """
        Fetch anime by year and season from Jikan API.

        Args:
            year (int): The year of the anime season.
            season (str): The season (e.g., 'winter', 'spring', 'summer', 'fall').

        Returns:
            list: List of dictionaries containing anime data.
        """
        endpoint = f"{self.base_url}/seasons/{year}/{season}"
        records = []
        page = 1

        while True:
            params = {"page": page}

            try:
                response = requests.get(endpoint, params=params)
                response.raise_for_status()
                logging.info(
                    f"Successfully fetched anime data for {season} {year}, page {page}."
                )
                data = response.json()
                anime_list = data["data"]

                if not anime_list:
                    logging.info(
                        f"No more data available after page {page}. All pages have been fetched."
                    )
                    break

                for anime in anime_list:
                    mal_id = anime.get("mal_id")
                    statistics_data = self.fetch_anime_statistics(mal_id)
                    records.append(
                        {
                            "mal_id": mal_id,
                            "title": anime.get("title"),
                            "type": anime.get("type"),
                            "episodes": anime.get("episodes"),
                            "score": anime.get("score"),
                            "scored_by": anime.get("scored_by"),
                            "start_date": anime.get("aired", {}).get("from"),
                            "end_date": anime.get("aired", {}).get("to"),
                            "popularity": anime.get("popularity"),
                            "genres": [
                                genre["name"] for genre in anime.get("genres", [])
                            ],
                            "producers": [
                                producer["name"]
                                for producer in anime.get("producers", [])
                            ],
                            "studios": [
                                studio["name"] for studio in anime.get("studios", [])
                            ],
                            "watching": (
                                statistics_data.get("watching")
                                if statistics_data
                                else None
                            ),
                            "completed": (
                                statistics_data.get("completed")
                                if statistics_data
                                else None
                            ),
                            "on_hold": (
                                statistics_data.get("on_hold")
                                if statistics_data
                                else None
                            ),
                            "dropped": (
                                statistics_data.get("dropped")
                                if statistics_data
                                else None
                            ),
                            "plan_to_watch": (
                                statistics_data.get("plan_to_watch")
                                if statistics_data
                                else None
                            ),
                            "total": (
                                statistics_data.get("total")
                                if statistics_data
                                else None
                            ),
                        }
                    )
                page += 1
            except requests.exceptions.RequestException as e:
                logging.error(f"Error while fetching anime data on page {page}: {e}")
                break

        return records

    def fetch_top_anime(self):
        """
        Fetch top 50 anime from Jikan API by fetching the first two pages.

        Returns:
            list: List of dictionaries containing top anime data.
        """
        endpoint = f"{self.base_url}/top/anime"
        records = []
        pages_to_fetch = 2  # Fetch the first 2 pages

        for page in range(1, pages_to_fetch + 1):
            params = {"page": page}

            try:
                response = requests.get(endpoint, params=params)
                response.raise_for_status()
                logging.info(f"Successfully fetched top anime data, page {page}.")
                data = response.json()
                anime_list = data["data"]

                for anime in anime_list:
                    mal_id = anime.get("mal_id")
                    statistics_data = self.fetch_anime_statistics(mal_id)
                    records.append(
                        {
                            "mal_id": mal_id,
                            "title": anime.get("title"),
                            "type": anime.get("type"),
                            "episodes": anime.get("episodes"),
                            "score": anime.get("score"),
                            "start_date": anime.get("aired", {}).get("from"),
                            "end_date": anime.get("aired", {}).get("to"),
                            "popularity": anime.get("popularity"),
                            "genres": [
                                genre["name"] for genre in anime.get("genres", [])
                            ],
                            "producers": [
                                producer["name"]
                                for producer in anime.get("producers", [])
                            ],
                            "studios": [
                                studio["name"] for studio in anime.get("studios", [])
                            ],
                            "watching": (
                                statistics_data.get("watching")
                                if statistics_data
                                else None
                            ),
                            "completed": (
                                statistics_data.get("completed")
                                if statistics_data
                                else None
                            ),
                            "on_hold": (
                                statistics_data.get("on_hold")
                                if statistics_data
                                else None
                            ),
                            "dropped": (
                                statistics_data.get("dropped")
                                if statistics_data
                                else None
                            ),
                            "plan_to_watch": (
                                statistics_data.get("plan_to_watch")
                                if statistics_data
                                else None
                            ),
                            "total": (
                                statistics_data.get("total")
                                if statistics_data
                                else None
                            ),
                        }
                    )
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Error while fetching top anime data on page {page}: {e}"
                )
                break

        return records

    def fetch_anime_reviews(self, mal_id):
        """
        Fetch all reviews for a specific anime from Jikan API.

        Args:
            mal_id (int): The MyAnimeList ID of the anime.

        Returns:
            list: List of dictionaries containing anime reviews.
        """
        endpoint = f"{self.base_url}/anime/{mal_id}/reviews"
        records = []
        page = 1

        while True:
            params = {"page": page}

            try:
                response = requests.get(endpoint, params=params)
                response.raise_for_status()
                logging.info(
                    f"Successfully fetched reviews for anime ID {mal_id}, page {page}."
                )
                data = response.json()
                review_list = data["data"]

                if not review_list:
                    logging.info(
                        f"No more reviews available after page {page}. All pages have been fetched."
                    )
                    break

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
                page += 1
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Error while fetching reviews for anime ID {mal_id}, page {page}: {e}"
                )
                break

        return records

    def fetch_anime_statistics(self, mal_id):
        """
        Fetch statistics for a specific anime from Jikan API.

        Args:
            mal_id (int): The MyAnimeList ID of the anime.

        Returns:
            dict: Dictionary containing anime statistics.
        """
        endpoint = f"{self.base_url}/anime/{mal_id}/statistics"

        try:
            response = requests.get(endpoint)
            response.raise_for_status()
            logging.info(f"Successfully fetched statistics for anime ID {mal_id}.")
            data = response.json()
            return data["data"]
        except requests.exceptions.RequestException as e:
            logging.error(f"Error while fetching statistics for anime ID {mal_id}: {e}")
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

    top_anime_data = extractor.fetch_top_anime()
    if top_anime_data:
        top_anime_df = extractor.extract_to_dataframe(top_anime_data)
        print(top_anime_df.head())
