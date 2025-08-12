import sqlite3
from datetime import datetime
import pytz
import sys
import pandas as pd
from github import Github
import streamlit as st
from sqlalchemy import text

from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
JAKARTA_TZ = pytz.timezone("Asia/Jakarta")
conn = st.connection('stock_news_db', type='sql')

def now_jakarta():
    """Return current datetime in Asia/Jakarta as ISO string."""
    return datetime.now(JAKARTA_TZ).strftime("%Y-%m-%d %H:%M:%S")

def init_status_table():
    # Create the table if it doesn't exist
    with conn.engine.connect() as connection:
        connection.execute(text("""
            CREATE TABLE IF NOT EXISTS status (
                activity TEXT PRIMARY KEY,
                status TEXT,
                dw_modify_ts TEXT
            )
        """))

        # Insert default statuses
        for activity in ["scraping", "extracting_information"]:
            connection.execute(
                text("""
                    INSERT OR IGNORE INTO status (activity, status, dw_modify_ts)
                    VALUES (:activity, :status, :ts)
                """),
                {
                    "activity": activity,
                    "status": "success",
                    "ts": now_jakarta()
                }
            )

        # Commit all changes
        connection.commit()

def update_status(activity, status):
    with conn.engine.connect() as connection:
        connection.execute(
            text("""
                UPDATE status
                SET status = :status, dw_modify_ts = :ts
                WHERE activity = :activity
            """),
            {
                "status": status,
                "ts": now_jakarta(),
                "activity": activity
            }
        )
        connection.commit()

def get_status():
    df = pd.read_sql("SELECT * FROM status", conn.engine)
    return df

def get_news_counts():
    df = pd.read_sql("SELECT source, COUNT(*) as count FROM news GROUP BY source", conn.engine)
    total_count = df["count"].sum()
    return total_count, df

def update_db_to_repo():
    github = Github(st.secrets["git"]["token"])
    repo_owner = 'Khanifsaleh'
    repo_name = 'stock-monitoring'
    repo = github.get_user(repo_owner).get_repo(repo_name)
    db_url = st.secrets["connections"]["stock_news_db"]["url"]
    db_path = db_url.replace("sqlite:///", "")
    with open(db_path, 'rb') as f:
        db_content = f.read()
    content = repo.get_contents(db_path)
    repo.update_file(
        content.path,
        f"update stock_news database",
        db_content, content.sha,
        branch="main"
    )
