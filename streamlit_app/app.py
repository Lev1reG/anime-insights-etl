import streamlit as st
import pandas as pd
import psycopg2
import altair as alt
import matplotlib.pyplot as plt
import os
from dotenv import load_dotenv

load_dotenv()

# Database connection
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    database=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD"),
)


# Load data function
@st.cache_data
def load_data(query):
    return pd.read_sql(query, conn)

# Streamlit App Setup
st.title("Anime Business Intelligence Dashboard")

# Sidebar Navigation
st.sidebar.title("Dashboard Navigation")
view_option = st.sidebar.selectbox(
    "Select Dataset to Analyze",
    [
        "Top Anime Overview",
        "Trending Anime Overview",
        "Current Season Anime Overview",
        "Top Anime Reviews Sentiment",
        "Trending Anime Reviews Sentiment",
        "Current Season Anime Reviews Sentiment",
        "Viewer Behavior Analysis",
        "Content Quality vs Popularity Analysis"
    ]
)

# Load Data Based on Selection
if view_option == "Top Anime Overview":
    query = "SELECT * FROM top_anime_data"
    df = load_data(query)
    st.write("## Top Anime Overview")
    st.write(df.head())
    
    # Plot Score vs Watching with Genres
    st.write("### Score vs Watching (by Genre)")
    chart = alt.Chart(df).mark_circle(size=60).encode(
        x='score',
        y='watching',
        color='genres',
        tooltip=['title', 'genres', 'score', 'watching', 'completed', 'plan_to_watch']
    ).interactive()
    st.altair_chart(chart, use_container_width=True)
    
    # Genre Distribution
    st.write("### Genre Distribution")
    df['genres'] = df['genres'].astype(str)  # Ensure genres column is string type
    genres = df['genres'].str.strip("[]").str.replace("'", "").str.split(', ').explode()
    genre_counts = genres.value_counts()
    st.bar_chart(genre_counts)
    
    # Completion Status Overview
    st.write("### Completion Status Overview")
    status_distribution = df[['watching', 'completed', 'on_hold', 'dropped', 'plan_to_watch']].sum()
    st.bar_chart(status_distribution)

elif view_option == "Trending Anime Overview":
    query = "SELECT * FROM trending_anime_data"
    df = load_data(query)
    st.write("## Trending Anime Overview")
    st.write(df.head())
    
    # Plot Popularity vs Score
    st.write("### Popularity vs Score")
    popularity_chart = alt.Chart(df).mark_circle(size=60).encode(
        x='score',
        y='popularity',
        color='title',
        tooltip=['title', 'score', 'popularity', 'current', 'completed', 'planning']
    ).interactive()
    st.altair_chart(popularity_chart, use_container_width=True)

    # Popularity Over Time
    st.write("### Popularity Over Time")
    popularity_time_chart = alt.Chart(df).mark_line().encode(
        x='fetched_date:T',
        y='popularity',
        color='title'
    ).interactive()
    st.altair_chart(popularity_time_chart, use_container_width=True)

elif view_option == "Current Season Anime Overview":
    query = "SELECT * FROM current_season_anime_data"
    df = load_data(query)
    st.write("## Current Season Anime Overview")
    st.write(df.head())
    
    # Plot Score vs Watching with Genres
    st.write("### Score vs Watching (by Genre)")
    current_season_chart = alt.Chart(df).mark_circle(size=60).encode(
        x='score',
        y='watching',
        color='genres',
        tooltip=['title', 'genres', 'score', 'watching', 'completed', 'plan_to_watch']
    ).interactive()
    st.altair_chart(current_season_chart, use_container_width=True)
    
    # Viewer Completion Status
    st.write("### Viewer Completion Status")
    status_distribution = df[['watching', 'completed', 'on_hold', 'dropped', 'plan_to_watch']].sum()
    st.bar_chart(status_distribution)

elif view_option == "Top Anime Reviews Sentiment":
    query = "SELECT * FROM top_anime_reviews"
    df = load_data(query)
    st.write("## Top Anime Reviews Sentiment Analysis")
    st.write(df.head())
    
    # Sentiment Distribution
    st.write("### Sentiment Distribution")
    sentiment_counts = df['sentiment_label'].value_counts()
    st.bar_chart(sentiment_counts)
    
    # Sentiment Score Distribution
    st.write("### Sentiment Score Distribution")
    plt.figure(figsize=(10, 6))
    plt.boxplot(df['sentiment_score'])
    st.pyplot(plt)

elif view_option == "Trending Anime Reviews Sentiment":
    query = "SELECT * FROM trending_anime_reviews"
    df = load_data(query)
    st.write("## Trending Anime Reviews Sentiment Analysis")
    st.write(df.head())
    
    # Sentiment Distribution
    st.write("### Sentiment Distribution")
    sentiment_counts = df['sentiment_label'].value_counts()
    st.bar_chart(sentiment_counts)

    # Sentiment Score Distribution
    st.write("### Sentiment Score Distribution")
    plt.figure(figsize=(10, 6))
    plt.boxplot(df['sentiment_score'])
    st.pyplot(plt)

elif view_option == "Current Season Anime Reviews Sentiment":
    query = "SELECT * FROM current_season_anime_reviews"
    df = load_data(query)
    st.write("## Current Season Anime Reviews Sentiment Analysis")
    st.write(df.head())
    
    # Sentiment Label Pie Chart
    st.write("### Sentiment Label Distribution")
    sentiment_counts = df['sentiment_label'].value_counts()
    plt.figure(figsize=(8, 8))
    plt.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%', startangle=140)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    st.pyplot(plt)
    
    # Review Sentiment Score Over Time
    st.write("### Sentiment Score Over Time")
    score_chart = alt.Chart(df).mark_line().encode(
        x='fetched_date:T',
        y='sentiment_score',
        color='source',
        tooltip=['fetched_date', 'sentiment_score', 'source']
    ).interactive()
    st.altair_chart(score_chart, use_container_width=True)

elif view_option == "Viewer Behavior Analysis":
    st.write("## Viewer Behavior Analysis")
    query = "SELECT title, watching, completed, on_hold, dropped FROM top_anime_data"
    df_behavior = load_data(query)

    # Stacked bar chart to show viewer behavior
    st.write("### Viewer Engagement: Watching vs Completed vs Dropped")
    df_behavior_melted = df_behavior.melt(id_vars=['title'], value_vars=['watching', 'completed', 'on_hold', 'dropped'], var_name='status', value_name='count')
    engagement_chart = alt.Chart(df_behavior_melted).mark_bar().encode(
        x='title',
        y='count',
        color='status',
        tooltip=['title', 'status', 'count']
    ).interactive()
    st.altair_chart(engagement_chart, use_container_width=True)

elif view_option == "Content Quality vs Popularity Analysis":
    st.write("## Content Quality vs Popularity Analysis")
    query = "SELECT title, score, popularity FROM trending_anime_data"
    df_quality = load_data(query)

    # Scatter plot for Score vs Popularity
    st.write("### Score vs Popularity")
    quality_popularity_chart = alt.Chart(df_quality).mark_circle(size=60).encode(
        x='score',
        y='popularity',
        color='title',
        tooltip=['title', 'score', 'popularity']
    ).interactive()
    st.altair_chart(quality_popularity_chart, use_container_width=True)

st.sidebar.info("Use the dropdown to select different analyses and explore the data!")
