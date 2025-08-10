import os
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "db", "stock_news.db")

TABLE_NEWS = "news"
SCRAPER_CONFIG = {
    "delay_request_range": [1, 20],
    "cnbc": {
        "base_url": "https://www.cnbcindonesia.com/market/rss"
    },
    "kontan": {
        "base_url": "https://www.kontan.co.id/search/indeks?kanal=investasi&tanggal={day}&bulan={month}&tahun={year}&pos=indeks&per_page={per_page}"
    }
}