from .base import BaseScraper
from utils import clean_text
import pandas as pd
import requests
from bs4 import BeautifulSoup
import random
import time
from tqdm import tqdm
from datetime import datetime

class KontanScraper(BaseScraper):
    def __init__(self, db_path, table_name, base_url, delay_request_range):
        super().__init__(db_path=db_path, table_name=table_name, source="kontan")
        self.base_url = base_url
        self.delay_request_range = delay_request_range

    def fetch_links(self, last_date, scraped_links):
        """
        Fetch news article links from Kontan between last_date and today.
        
        Args:
            last_date (datetime): Start date for fetching.
            scraped_links (set/list): Links already scraped to skip.

        Returns:
            pd.DataFrame: Columns ['published', 'link']
        """
        scraped_links = set(scraped_links)  # fast lookups
        all_links = []

        date_range = pd.date_range(start=last_date, end=datetime.now(), freq='D')

        for date in tqdm(date_range, desc="Fetching links"):
            day, month, year = date.strftime('%d'), date.strftime('%m'), date.strftime('%Y')
            per_page = 0

            while True:
                url = self.base_url.format(day=day, month=month, year=year, per_page=str(per_page))
                try:
                    resp = requests.get(url, timeout=15)
                    resp.raise_for_status()
                except requests.RequestException as e:
                    print(f"[ERROR] Failed to fetch {url}: {e}")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                berita_div = soup.find('div', class_='list-berita')
                if not berita_div:
                    break

                berita_items = berita_div.find_all('div', class_='sp-hl linkto-black')
                if not berita_items:
                    break

                new_links = []
                for item in berita_items:
                    link = item.find('a').get('href')
                    if link and link not in scraped_links:
                        scraped_links.add(link)
                        new_links.append(link)

                if not new_links:
                    break  # No more new links → stop pagination

                all_links.extend([(date, link) for link in new_links])
                per_page += 20
                time.sleep(random.uniform(*self.delay_request_range))

        df = pd.DataFrame(all_links, columns=['published', 'link'])
        df['published'] = pd.to_datetime(df['published'])
        return df

    def fetch_article_content(self, link):
        """
        Fetch the full article content and title from a given link.
        Filters out unwanted phrases.
        """
        exclude_phrases = {
            'Reporter', 
            'Editor', 
            'Baca Juga', 
            'Cek Berita dan Artikel yang lain', 
            'Menarik Dibaca', 
            'Selanjutnya:'
        }
        try:
            resp = requests.get(link, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[ERROR] Failed to fetch article {link}: {e}")
            return "", ""

        soup = BeautifulSoup(resp.text, "html.parser")

        title_tag = soup.find('title')
        title = title_tag.get_text(strip=True) if title_tag else ""

        content = ""
        body_element = soup.find(attrs={"itemprop": 'articleBody'})
        if body_element:
            paragraphs = body_element.find_all('p')
            content_parts = [
                p.get_text(strip=True)
                for p in paragraphs
                if not any(phrase in p.get_text() for phrase in exclude_phrases)
            ]
            content = clean_text(" ".join(content_parts))

        return title, content

    def scrape(self, last_date, links):
        """Scrape Kontan links and fetch full article content."""
        df = self.fetch_links(last_date, links)
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])

        titles = []
        contents = []

        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Scraping {self.source}"):
            title, content = self.fetch_article_content(row["link"])
            titles.append(title)
            contents.append(content)
            time.sleep(random.uniform(*self.delay_request_range))

        df["title"] = titles
        df["content"] = contents
        return df[["published", "link", "title", "content"]]
