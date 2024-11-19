from numpy.random import f
import requests
import pandas as pd
import logging

pd.set_option("display.max_columns", 200)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

ANILIST_API_URL = "https://graphql.anilist.co"


class AnilistExtractor:
    def __init__(self):
        self.api_url = ANILIST_API_URL

    def fetch_anime_reviews_by_mal_id(self, mal_id):
        """
        Fetch anime reviews from AniList API based on MyAnimeList ID (mal_id).

        Args:
            mal_id (int): The MyAnimeList ID of the anime.

        Returns:
            list: List of dictionaries containing anime reviews.
        """
        query = """
        query ($malId: Int, $page: Int) {
            Page(page: $page, perPage: 50) {
                media(idMal: $malId, type: ANIME) {
                    id
                    idMal
                    title {
                        romaji
                        english
                    }
                    reviews(page: 1) {
                        nodes {
                            user {
                                name
                            }
                            body
                            score
                        }
                    }
                }
            }
        }
        """

        records = []
        page = 1

        while True:
            variables = {"malId": mal_id, "page": page}

            try:
                response = requests.post(
                    self.api_url, json={"query": query, "variables": variables}
                )
                response.raise_for_status()
                logging.info(
                    f"Successfully fetched anime reviews for MAL ID {mal_id}, page {page}."
                )
                data = response.json()["data"]["Page"]["media"]

                if not data:
                    logging.info(
                        f"No more data available after page {page}. All pages have been fetched."
                    )
                    break

                for anime in data:
                    anime_id = anime["id"]
                    anime_title = anime["title"]["romaji"]
                    reviews = anime.get("reviews", {}).get("nodes", [])

                    for review in reviews:
                        records.append(
                            {
                                "anime_id": anime_id,
                                "mal_id": anime.get("idMal"),
                                "anime_title": anime_title,
                                "username": review["user"]["name"],
                                "review_body": review.get("body"),
                                "score": review["score"],
                            }
                        )
                page += 1
            except requests.exceptions.RequestException as e:
                logging.error(
                    f"Error while fetching anime reviews for MAL ID {mal_id} on page {page}: {e}"
                )
                break

        return records

    def fetch_anime_by_season(self, year, season):
        """
        Fetch anime data from AniList API based on year and season.
        
        Args:
            year (int): The year of the anime season.
            season (str): The season (e.g., 'WINTER', 'SPRING', 'SUMMER', 'FALL').

        Returns:
            list: List of dictionaries containing anime score and watch count.
        """
        query = """
        query ($year: Int, $season: MediaSeason, $page: Int) {
            Page(page: $page, perPage: 50) {
                media(seasonYear: $year, season: $season, type: ANIME) {
                    id
                    idMal
                    title {
                        romaji
                        english
                    }
                    averageScore
                    popularity
                    stats {
                        statusDistribution {
                            status
                            amount
                        }
                    }
                }
            }
        }
        """

        records = []
        page = 1

        while True:
            variables = {
                "year": year,
                "season": season.upper(),
                "page": page
            }

            try:
                response = requests.post(self.api_url, json={"query": query, "variables": variables})
                if response.status_code != 200:
                    logging.error(f"Query failed to run by returning code of {response.status_code}. {response.text}")
                    break
                response.raise_for_status()
                logging.info(f"Successfully fetched anime data for {season} {year}, page {page}.")
                data = response.json()["data"]["Page"]["media"]

                if not data:
                    logging.info(f"No more data available after page {page}. All pages have been fetched.")
                    break

                for anime in data:
                    # Extract status distribution
                    status_distribution = anime.get("stats", {}).get("statusDistribution", [])
                    watch_counts = {status["status"].lower(): status["amount"] for status in status_distribution}

                    records.append(
                        {
                            "anime_id": anime["id"],
                            "mal_id": anime.get("idMal"),
                            "anime_title": anime["title"]["romaji"],
                            "average_score": anime.get("averageScore"),
                            "popularity": anime.get("popularity"),
                            "watching": watch_counts.get("watching"),
                            "completed": watch_counts.get("completed"),
                            "dropped": watch_counts.get("dropped"),
                            "on_hold": watch_counts.get("on_hold"),
                            "plan_to_watch": watch_counts.get("plan_to_watch"),
                        }
                    )
                page += 1
            except requests.exceptions.RequestException as e:
                logging.error(f"Error while fetching anime data for {season} {year} on page {page}: {e}")
                break

        return records

    def extract_to_dataframe(self, data):
        """
        Convert JSON data or list into a DataFrame.

        Args:
            data (list): The list of dictionaries to be transformed into a DataFrame.

        Returns:
            DataFrame: Pandas DataFrame containing the data.
                                "review_summary": review["summary"],
        """
        if not isinstance(data, list):
            logging.error("Input data must be a list.")
            return pd.DataFrame()

        df = pd.DataFrame(data)
        logging.info("Successfully extracted data to DataFrame.")
        return df


if __name__ == "__main__":
    extractor = AnilistExtractor()

    # Fetch anime reviews by MyAnimeList ID
    mal_id = 51019
    reviews_data = extractor.fetch_anime_reviews_by_mal_id(mal_id)

    if reviews_data:
        reviews_df = extractor.extract_to_dataframe(reviews_data)
        print(reviews_df.head(15))

    year = 2021
    season = "SUMMER"
    score_watch_data = extractor.fetch_anime_by_season(year, season)
    if score_watch_data:
        score_watch_df = extractor.extract_to_dataframe(score_watch_data)
        print(score_watch_df.head())
