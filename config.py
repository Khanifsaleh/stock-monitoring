TABLE_NEWS = "news"
SCRAPER_CONFIG = {
    "delay_request_range": [1, 20],
    "cnbc": {
        "base_url": "https://www.cnbcindonesia.com/market/rss"
    },
    "kontan": {
        "base_url": "https://www.kontan.co.id/search/indeks?kanal=investasi&tanggal={day}&bulan={month}&tahun={year}&pos=indeks&per_page={per_page}"
    },
    "bisnis": {
        "base_url": "https://www.bisnis.com/index?categoryId=194&date={date}&type=indeks&page={page}"
    },
    "idx": {
        "base_url": "https://www.idxchannel.com/rss"
    },
    "pasardana": {
        "base_url": "https://www.pasardana.id/rss"
    },
    "iqplus": {
        "base_url": "http://www.iqplus.info/box_listnews_more.php?csection=stock_news&id={page}"
    }
}