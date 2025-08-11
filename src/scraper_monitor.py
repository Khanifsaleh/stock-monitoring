import streamlit as st
import pandas as pd
import sqlite3
import time
from datetime import datetime, timedelta
from utils.db_setup import init_status_table, update_status, get_status, now_jakarta
from run_scrapers import RunScrapers
import sys

from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import DB_PATH

st.set_page_config(page_title="Scraper Dashboard", layout="wide")
init_status_table()

st.title("📰 Stock News Scraper Dashboard")
# --- Show Status Table
st.subheader("Status Overview")
status_df = get_status()
st.dataframe(status_df)

# --- Show scraped news counts
def get_news_counts():
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql("SELECT source, COUNT(*) as count FROM news GROUP BY source", conn)
        total_count = df["count"].sum()
    except Exception:
        df = pd.DataFrame(columns=["source", "count"])
        total_count = 0
    conn.close()
    return total_count, df

total_count, per_source_df = get_news_counts()
st.metric("Total News", total_count)
st.dataframe(per_source_df)

# --- Scraper Runner
def run_scraping():
    scraping_status = status_df[status_df["activity"] == "scraping"]["status"].iloc[0]
    if scraping_status == "running":
        print("Scraping is already running. Skipping...")
        return

    # Mark as running
    update_status("scraping", "running")
    try:
        print("Starting scraping...")
        RunScrapers("all")
        update_status("scraping", "success")

    except Exception as e:
        print(f"Scraping failed: {e}")
        update_status("scraping", "failed")
        st.error(f"Scraping failed: {e}")

# --- Manual Trigger Button
if st.button("Run Scraping Now"):
    with st.spinner("Scraping in progress... please wait"):
        run_scraping()