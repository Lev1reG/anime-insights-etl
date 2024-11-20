# Anime Insights: A Data-Driven Analysis of Trends and Viewer Sentiment Pipeline

## Authors

1. Sulaiman Fawwaz Abdillah Karim (22/493813/TK/54120)
2. Sakti Cahya Buana (22/503237/TK/54974)
3. Deren Tanaphan (22/503261/TK/54976)

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline to analyze anime trends using data from Jikan and AniList APIs. The extracted data is transformed, analyzed, and visualized to provide insights into trends and viewer sentiment in the anime industry.

## Notion Blog

Read the blog post on Notion: [Anime Insights: A Data-Driven Analysis of Trends and Viewer Sentiment](https://jolly-bee-29a.notion.site/End-to-End-Data-Engineering-Pipeline-Project-e1159e22f06f47d9ade543bd327a27b6?pvs=73)

## Presentation Videos

See the presentation videos on this link:

## Key Features

- Extracts anime data and reviews from Jikan and AniList APIs.

- Performs sentiment analysis to determine viewer sentiment.

- Loads the transformed data into an Azure-hosted PostgreSQL database.

- Provides an interactive Streamlit dashboard for Business Intelligence analysis.

## Technologies Used

- Programming Language: Python

- ETL Orchestration: Apache Airflow

- Database: PostgreSQL (hosted on Azure)

- Cloud Provider: Azure Virtual Machine (b2als_v2)

- Dashboard Framework: Streamlit

- APIs Used: Jikan (MyAnimeList) and AniList

## Repository Structure

```
project-root/
  |-- dags/                  # Airflow DAGs
  |-- src/
      |-- extract/           # Extraction scripts (Jikan & AniList)
      |-- transform/         # Transformation scripts
      |-- load/              # Load scripts (PostgreSQL)
      |-- utils/             # Utility functions
  |-- data/                  # Temporary storage for extracted data
  |-- streamlit_app/         # Streamlit dashboard
  |-- .env                   # Environment variables for database credentials
  |-- requirements.txt       # Python dependencies
  |-- README.md              # Project README
```

## Prerequisites

- Apache Airflow 

- All required Python packages (see requirements.txt)

## Running the Project

### 1. Setting Up Project Environment

**Step 1:** Clone the repository

```bash
git clone https://github.com/Lev1reG/anime-insights-etl.git
cd anime-insights-etl
```

**Step 2**: Create a Python virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

**Step 3**: Install the required Python packages

```bash
pip install -r requirements.txt
```

**Step 4**: Set up environment variables (.env)
Make a file named `.env` in the project root directory and add the following environment variables:

```.env
DB_HOST=<your_db_host>
DB_NAME=<your_db_name
DB_USER=<your_db_user>
DB_PASSWORD=<your_db_password>
```

### 2. Configuring Apache Airflow

**Step 1**: Configure the AIRFLOW_HOME environment variable

Configure your `AIRFLOW_HOME` environment variable to point to the `dags/` directory in the project root.

```bash
export AIRFLOW_HOME=/path/to/anime-insights-etl/dags
```

Airflow will automatically detect the DAGs in the `dags/` directory.

**Step 2**: Start the Airflow

```bash
airflow standalone
```

The Airflow web server will be available at `http://localhost:8080/`.

### 3. Running the Streamlit Dashboard

**Step 1**: Navigate to the project directory
Ensure you are in the project directory where the Streamlit script is located.

**Step 2**: Run the Streamlit script

```bash
streamlit run streamlit_app/app.py
```

The Streamlit dashboard will be available at `http://localhost:8501/`.

## Running the ETL Pipeline

To run the pipeline, navigate to the Airflow web interface and manually trigger the DAG named anime_etl_pipeline. Alternatively, you can set the DAG schedule to run automatically (daily, weekly, etc.).

## Business Intelligence Dashboard

The Streamlit-based dashboard allows users to visualize anime trends, viewer engagement, and sentiment analysis.

Access the dashboard at: [http://4.145.91.218:8501/](http://4.145.91.218:8501/)
