import sqlite3
from datetime import datetime
import pytz
import sys
import pandas as pd

from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[2]))
from config import DB_PATH
JAKARTA_TZ = pytz.timezone("Asia/Jakarta")

def now_jakarta():
    """Return current datetime in Asia/Jakarta as ISO string."""
    return datetime.now(JAKARTA_TZ).strftime("%Y-%m-%d %H:%M:%S")

def init_status_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS status (
            activity TEXT PRIMARY KEY,
            status TEXT,
            dw_modify_ts TEXT
        )
    """)
    for activity in ["scraping", "extracting_information"]:
        c.execute("""
            INSERT OR IGNORE INTO status (activity, status, dw_modify_ts)
            VALUES (?, ?, ?)
        """, (activity, "success", now_jakarta()))
    conn.commit()
    conn.close()

def update_status(activity, status):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        UPDATE status
        SET status = ?, dw_modify_ts = ?
        WHERE activity = ?
    """, (status, now_jakarta(), activity))
    conn.commit()
    conn.close()

def get_status():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql("SELECT * FROM status", conn)
    conn.close()
    return df