from .base import BaseScraper
from utils import clean_text
import pandas as pd
import requests
from bs4 import BeautifulSoup
import random
import time
from tqdm import tqdm
from datetime import datetime

class BisnisScraper(BaseScraper):
    def __init__(self, db_path, table_name, base_url, delay_request_range):
        super().__init__(db_path=db_path, table_name=table_name, source="bisnis")
        self.base_url = base_url
        self.delay_request_range = delay_request_range

    def fetch_links(self, last_date, scraped_links):
        """
        Fetch news links from Bisnis.com between last_date and today.
        
        Args:
            last_date (datetime): The start date for scraping.
            scraped_links (set/list): Already scraped links to skip.

        Returns:
            pd.DataFrame: Columns ['published', 'link', 'title']
        """
        scraped_links = set(scraped_links)
        all_links = []

        for date in pd.date_range(start=last_date, end=datetime.now(), freq="D"):
            page = 1
            while True:
                url = self.base_url.format(date=date.strftime('%Y-%m-%d'), page=page)
                try:
                    resp = requests.get(url, timeout=15)
                    resp.raise_for_status()
                except requests.RequestException as e:
                    print(f"[ERROR] Failed to fetch {url}: {e}")
                    break

                soup = BeautifulSoup(resp.text, "html.parser")
                container = soup.find("div", id="indeksListView")
                if not container:
                    break

                elements = container.find_all('div', class_='artContent')
                if not elements:
                    break

                new_data = []
                for element in elements:
                    link_tag = element.find('a', class_='artLink')
                    if not link_tag:
                        continue
                    link = link_tag['href']
                    title_tag = link_tag.find(class_='artTitle')
                    title = title_tag.get_text(strip=True) if title_tag else ""
                    if link and link not in scraped_links:
                        scraped_links.add(link)
                        new_data.append((date, link, title))

                if not new_data:
                    break

                all_links.extend(new_data)
                page += 1
                time.sleep(random.uniform(1, 10))

        df = pd.DataFrame(all_links, columns=["published", "link", "title"])
        df["published"] = pd.to_datetime(df["published"])
        return df

    def fetch_article_content(self, link) -> str:
        """
        Fetch the full article content from Bisnis.com.
        
        Args:
            link (str): URL to the article.
        
        Returns:
            str: Cleaned article text.
        """
        try:
            resp = requests.get(link, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"[ERROR] Failed to fetch article {link}: {e}")
            return ""

        soup = BeautifulSoup(resp.text, "html.parser")
        article_body = soup.find('article', class_='detailsContent')
        if not article_body:
            return ""

        paragraphs = article_body.find_all('p')
        content = clean_text(' '.join([
            clean_text(p.get_text(separator=" ", strip=True))
            for p in paragraphs
            if p.get_text(strip=True) and not p.get_text(strip=True).startswith('#')
        ]))
        return content

    def scrape(self, last_date, scraped_links):
        """
        Main scraping method.
        
        Args:
            last_date (datetime): Start date for scraping.
            scraped_links (list/set): Links already scraped.
        
        Returns:
            pd.DataFrame: Columns ['published', 'link', 'title', 'content']
        """
        df = self.fetch_links(last_date, scraped_links)
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])

        for i, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Scraping {self.source}"):
            content = self.fetch_article_content(row["link"])
            df.at[i, "content"] = content
            time.sleep(random.uniform(*self.delay_request_range))

        return df[["published", "link", "title", "content"]]