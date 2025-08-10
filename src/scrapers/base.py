import sqlite3
import pandas as pd
from abc import ABC, abstractmethod

class BaseScraper(ABC):
    def __init__(self, db_path, table_name, source):
        self.db_path = db_path
        self.table_name = table_name
        self.source = source
        self.conn = sqlite3.connect(self.db_path)
        
    def get_last_date(self):
        """Get the most recent published date from DB"""
        query = f"""
            SELECT MAX(published) AS last_date
            FROM {self.table_name}
            WHERE source = '{self.source}'
        """
        last_date = pd.read_sql(query, self.conn)["last_date"].iloc[0]
        return pd.to_datetime(last_date)
    
    def get_scraped_links(self, last_date):
        """Fetch the links from the source."""
        last_date = last_date.strftime('%Y-%m-%d 00:00:00')
        query = f"""
                    SELECT DISTINCT link as link
                    FROM {self.table_name}
                    WHERE DATETIME(published) >= ?
                    AND source = '{self.source}'
                """
        df = pd.read_sql(query, self.conn, params=(last_date,))
        return df['link'].tolist() if not df.empty else []

    def save_to_db(self, df: pd.DataFrame):
        """Append new records to the database."""
        if not df.empty:
            df.to_sql(self.table_name, self.conn, if_exists="append", index=False)

    def add_metadata(self, df: pd.DataFrame):
        """Add metadata columns for data warehouse."""
        df['source'] = self.source
        df['DW_LOAD_TS'] = pd.to_datetime('now')
        df['DW_LOAD_TS'] = df['DW_LOAD_TS'].dt.tz_localize('Asia/Jakarta')
        df['DW_MODIFY_TS'] = pd.to_datetime('now')
        df['DW_MODIFY_TS'] = df['DW_MODIFY_TS'].dt.tz_localize('Asia/Jakarta')
        return df

    @abstractmethod
    def scrape(self, last_date, links) -> pd.DataFrame:
        """
        Child classes must implement this method.
        Should return a DataFrame with columns:
        'published', 'link', 'title', 'content'
        """
        pass

    def run(self):
        """Main execution flow."""
        last_date = self.get_last_date()
        links = self.get_scraped_links(last_date)
        new_data = self.scrape(last_date, links)
        if new_data.empty:
            print("No new data to save.")
            return
        new_data = self.add_metadata(new_data)
        self.save_to_db(new_data)
        print(f"Saved {len(new_data)} new records.")

    def __del__(self):
        self.conn.close()