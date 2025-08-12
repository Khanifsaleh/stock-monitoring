import streamlit as st
from utils.db_setup import init_status_table, update_status, get_status, get_news_counts
from run_scrapers import RunScrapers

st.set_page_config(page_title="Scraper Dashboard", layout="wide")
init_status_table()

st.title("📰 Stock News Scraper Dashboard")
# --- Show Status Table
st.subheader("Status Overview")
status_df = get_status()
st.dataframe(status_df)

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