from .base import BaseScraper
from utils import clean_text
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup
import random
import time
from tqdm import tqdm

class CNBCScraper(BaseScraper):
    def __init__(self, db_path, table_name, base_url, delay_request_range):
        super().__init__(db_path=db_path, table_name=table_name, source="cnbc")
        self.base_url = base_url
        self.delay_request_range = delay_request_range

    def fetch_rss(self):
        """Fetch the RSS feed from CNBC Indonesia."""
        feed = feedparser.parse(self.base_url)
        df = pd.DataFrame([{
            "title": e.title,
            "link": e.link,
            "published": pd.to_datetime(e.published).strftime('%Y-%m-%d %H:%M:%S')
        } for e in feed.entries])
        df["published"] = pd.to_datetime(df["published"])
        return df

    def fetch_article_content(self, link) -> str:
        """Fetch the full article content from the given link."""
        resp = requests.get(link)
        soup = BeautifulSoup(resp.text, "html.parser")
        div = soup.find("div", class_="detail-text")
        if not div:
            return ""
        return " ".join(p.get_text(strip=True) for p in div.find_all("p"))

    def scrape(self, last_date, links=None) -> pd.DataFrame:
        """Scrape CNBC Indonesia RSS feed and fetch full article content."""
        df = self.fetch_rss()
        df = df[df["published"] > last_date]
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])
        for i, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Scraping {self.source}"):
            df.at[i, "content"] = self.fetch_article_content(row["link"])
            time.sleep(random.uniform(*self.delay_request_range))
        df['content'] = df['content'].apply(clean_text)
        return df[["published", "link", "title", "content"]]