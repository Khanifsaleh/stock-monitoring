from scrapers.cnbc import CNBCScraper
from scrapers.kontan import KontanScraper
from scrapers.bisnis import BisnisScraper
from scrapers.idx import IDXScraper
from scrapers.pasardana import PasarDanaScraper
from scrapers.iqplus import IQPlusScraper
import sys
import random

from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import DB_PATH, SCRAPER_CONFIG, TABLE_NEWS

class RunScrapers:
    def __init__(self, source):
        if source != "all":
            self.run(source)
        else:
            self.run_all()

    def set_params(self, source):
        return  {
            "db_path": DB_PATH,
            "table_name": TABLE_NEWS,
            "base_url": SCRAPER_CONFIG[source]["base_url"],
            "delay_request_range": SCRAPER_CONFIG["delay_request_range"]
        }

    def run(self, source):
        params = self.set_params(source)
        if source == "cnbc":
            scraper = CNBCScraper(**params)
            scraper.run()
        elif source == "kontan":
            scraper = KontanScraper(**params)
            scraper.run()
        elif source == "bisnis":
            scraper = BisnisScraper(**params)
            scraper.run()
        elif source == "idx":
            scraper = IDXScraper(**params)
            scraper.run()
        elif source == "pasardana":
            scraper = PasarDanaScraper(**params)
            scraper.run()
        elif source == "iqplus":
            scraper = IQPlusScraper(**params)
            scraper.run()

    def run_all(self):
        sources = list(SCRAPER_CONFIG.keys())
        random.shuffle(sources)
        for source in sources:
            if source in ["delay_request_range"]:
                continue
            self.run(source)

if __name__ == "__main__":
    import sys
    source = sys.argv[1]
    runner = RunScrapers(source)