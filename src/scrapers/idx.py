from .base import BaseScraper
from utils import clean_text
import pandas as pd
import requests
import feedparser
from bs4 import BeautifulSoup
import random
import time
from tqdm import tqdm
from urllib.parse import urlparse

class IDXScraper(BaseScraper):
    def __init__(self, db_path, table_name, base_url, delay_request_range):
        super().__init__(db_path=db_path, table_name=table_name, source="idx")
        self.base_url = base_url
        self.delay_request_range = delay_request_range

    def fetch_rss(self):
        """
        Fetch article metadata from IDX  RSS feed.
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
            link = entry.link
            category = urlparse(link).path.strip("/").split("/")[0]
            if category != "market-news":
                continue
            data.append({
                "title": entry.title,
                "link": link,
                "published": pd.to_datetime(entry.published).strftime('%Y-%m-%d %H:%M:%S')
            })

        df = pd.DataFrame(data)
        if df.empty:
            return df

        df["published"] = pd.to_datetime(df["published"])
        return df

    def fetch_article_content(self, link) -> str:
        """
        Fetch full multi-page article content from IDX .
        
        Args:
            link (str): Article URL.
        
        Returns:
            str: Cleaned combined article text.
        """
        page = 1
        whole_content = ""

        while True:
            link_with_page = f"{link}/{page}"
            try:
                resp = requests.get(link_with_page, timeout=15)
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"[ERROR] Failed to fetch {link_with_page}: {e}")
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            container1 = soup.find('div', class_='article--content')
            if not container1:
                break
            container2 = container1.find('div', class_='content')
            if not container2:
                break

            paragraphs = container2.find_all('p')
            content = " ".join([p.get_text() for p in paragraphs])
            content = clean_text(content)
            whole_content += " " + content

            page += 1
            time.sleep(random.uniform(1, 10))

        return whole_content.strip()

    def scrape(self, last_date, scraped_links):
        """
        Main scraping routine for IDX .
        
        Args:
            scraped_links (list/set): Links already scraped.
        
        Returns:
            pd.DataFrame: Columns ['published', 'link', 'title', 'content']
        """
        df = self.fetch_rss()
        df = df[(df["published"] > last_date) & (~df["link"].isin(set(scraped_links)))]
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])
        for i, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Scraping {self.source}"):
            content = self.fetch_article_content(row["link"])
            df.at[i, "content"] = content
            time.sleep(random.uniform(*self.delay_request_range))

        df['content'] = df['content'].apply(clean_text)
        return df[["published", "link", "title", "content"]]