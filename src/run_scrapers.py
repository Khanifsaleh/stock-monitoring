from scrapers.cnbc import CNBCScraper
from scrapers.kontan import KontanScraper
import sys
from pathlib import Path
sys.path.append(str(Path(__file__).resolve().parents[1]))
from config import DB_PATH, SCRAPER_CONFIG, TABLE_NEWS

class RunScrapers:
    def __init__(self, source):
        self.source = source
        self.params = {
            "db_path": DB_PATH,
            "table_name": TABLE_NEWS,
            "base_url": SCRAPER_CONFIG[source]["base_url"],
            "delay_request_range": SCRAPER_CONFIG["delay_request_range"]
        }

    def run(self):
        if self.source == "cnbc":
            scraper = CNBCScraper(**self.params)
            scraper.run()
        elif self.source == "kontan":
            scraper = KontanScraper(**self.params)
            scraper.run()
    
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python run_scrapers.py <source>")
        sys.exit(1)
    
    source = sys.argv[1]
    runner = RunScrapers(source)
    runner.run()