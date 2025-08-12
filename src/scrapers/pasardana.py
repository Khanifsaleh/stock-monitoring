from .base import BaseScraper
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup
import random
import time
from tqdm import tqdm
from urllib.parse import urlparse

class PasarDanaScraper(BaseScraper):
    def __init__(self, conn, table_name, base_url, delay_request_range):
        super().__init__(conn=conn, table_name=table_name, source="pasardana")
        self.base_url = base_url
        self.delay_request_range = delay_request_range

    def fetch_links(self):
        """
        Fetch article metadata from PasarDana RSS feed.

        Returns:
            pd.DataFrame: Columns ['published', 'link', 'title']
        """
        try:
            feed = feedparser.parse(self.base_url)
        except Exception as e:
            print(f"[ERROR] Failed to parse RSS: {e}")
            return pd.DataFrame(columns=["published", "link", "title"])

        data = []
        for entry in feed.entries:
            data.append({
                "title": entry.title,
                "link": entry.link,
                "published": pd.to_datetime(entry.published).strftime('%Y-%m-%d %H:%M:%S')
            })

        df = pd.DataFrame(data)
        if not df.empty:
            df["published"] = pd.to_datetime(df["published"])

        return df

    def fetch_article_content(self, link) -> str:
        """
        Fetch article content from PasarDana.

        Args:
            link (str): Article URL.

        Returns:
            str: Cleaned article text.
        """
        try:
            resp = requests.get(link, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[ERROR] Failed to fetch {link}: {e}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        section = soup.find('section', class_='entry-content')
        if not section:
            return None

        paragraphs = section.find_all('p')
        content = " ".join([p.get_text() for p in paragraphs])
        return content

    def scrape(self, last_date, scraped_links):
        """
        Main scraping routine for PasarDana.

        Args:
            scraped_links (list/set): Links already scraped.
            last_date (datetime): Only scrape articles after this date.

        Returns:
            pd.DataFrame: Columns ['published', 'link', 'title', 'content']
        """
        df = self.fetch_links()
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])

        # Filter new articles
        df = df[(df["published"] > last_date) & (~df["link"].isin(set(scraped_links)))]
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])

        for i, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Scraping {self.source}"):
            content = self.fetch_article_content(row["link"])
            df.at[i, "content"] = content
            time.sleep(random.uniform(*self.delay_request_range))
        df = df.dropna(subset=["content"])
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])
        return df