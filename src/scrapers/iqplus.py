from .base import BaseScraper
from utils import clean_text
import pandas as pd
import requests
from bs4 import BeautifulSoup
import random
import time
from tqdm import tqdm
from datetime import datetime

class IQPlusScraper(BaseScraper):
    def __init__(self, db_path, table_name, base_url, delay_request_range):
        super().__init__(db_path=db_path, table_name=table_name, source="iqplus")
        self.base_url = base_url
        self.delay_request_range = delay_request_range
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
            'Referer': 'http://www.iqplus.info'
        }

    def fetch_links(self, last_date, scraped_links):
        """
        Fetch news article links from IQPlus between last_date and today.
        Args:
            last_date (datetime): The start date for scraping.
            scraped_links (set/list): Already scraped links to skip.

        Returns:
            pd.DataFrame: Columns ['published', 'link', 'title']
        """
        df = pd.DataFrame(columns=['published', 'link', 'title'])
        page = 1
        scraped_links = set(scraped_links) 
        while True:
            url = self.base_url.format(page=page)
            try:
                resp = requests.get(url, headers=self.headers, timeout=15)
                resp.raise_for_status()
            except requests.RequestException as e:
                print(f"[ERROR] Failed to fetch {url}: {e}")
                break

            if resp.status_code != 200:
                break

            soup = BeautifulSoup(resp.text, "html.parser")
            lis = soup.find_all('li', style='text-transform:capitalize;')
            if not lis:
                break

            new_data = []
            for li in lis:
                published = li.find("b").get_text(strip=True)
                a_tag = li.find("a")
                link = a_tag["href"]
                title = a_tag.get_text(strip=True)

                if link and link not in scraped_links:
                    new_data.append({
                        'published': published,
                        'link': link,
                        'title': title
                    })
                    scraped_links.add(link)

            if not new_data:
                break
            df_current_page = pd.DataFrame(new_data)
            df_current_page['published'] = pd.to_datetime(df_current_page['published'], format='%d/%m/%y - %H:%M')

            # Stop if older than last_date
            if df_current_page['published'].min().normalize() < last_date.normalize():
                break

            df = pd.concat([df, df_current_page], ignore_index=True)

            time.sleep(random.uniform(1, 10))
            page += 1

        return df

    def fetch_article_content(self, link):
        resp = requests.get(link, headers=self.headers)
        if resp.status_code != 200:
            return ""

        soup = BeautifulSoup(resp.text, "html.parser")
        zoom_div = soup.find("div", id="zoomthis")
        if not zoom_div:
            return ""

        # Remove unwanted tags
        for tag in zoom_div.find_all(["small", "h3"]):
            tag.decompose()

        news_content = zoom_div.get_text(separator="\n", strip=True)
        news_content = self.clean_text(news_content)

        return news_content

    def scrape(self, last_date, scraped_links):
        df = self.fetch_links(last_date, scraped_links)
        df = df[(df['published'] > last_date)]
        if df.empty:
            return pd.DataFrame(columns=["published", "link", "title", "content"])
        
        for i, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Scraping {self.source}"):
            content = self.fetch_article_content(row["link"])
            df.at[i, 'content'] = content
            time.sleep(random.uniform(*self.delay_request_range))
        
        return df[["published", "link", "title", "content"]]