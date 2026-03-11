import sys
import logging
from pathlib import Path

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

sys.path.append(str(Path(__file__).parent))

from src.models import SpeechDB
from src.scrapers.frb import FRBScraper
from src.scrapers.ecb import ECBScraper
from src.scrapers.boe import BOEScraper
from src.scrapers.boj import BOJScraper
from src.scrapers.rba import RBAScraper
from src.scrapers.boc import BOCScraper

def run_collection():
    print("=== Central Bank Speech Archiving Started ===")
    db = SpeechDB()
    
    scrapers = [
        FRBScraper(db=db),
        ECBScraper(db=db),
        BOEScraper(db=db),
        BOJScraper(db=db),
        RBAScraper(db=db),
        BOCScraper(db=db)
    ]
    
    for scraper in scrapers:
        try:
            print(f"\n[*] Collecting {scraper.BANK_CODE} ({scraper.BANK_NAME})...")
            # 2025년부터 현재까지의 데이터 수집
            new_count = scraper.collect_new_speeches(start_year=2025)
            print(f"[+] {scraper.BANK_CODE}: {new_count} new speeches added.")
        except Exception as e:
            logger.error(f"Failed to collect {scraper.BANK_CODE}: {e}")

    stats = db.get_stats()
    print("\n=== Collection Summary ===")
    for bank_code in db.BANKS:
        count = stats.get(bank_code, 0)
        print(f"- {bank_code}: {count} speeches")
    print(f"Total speeches in Archive: {stats.get('total', 0)}")
    print("===========================")

if __name__ == "__main__":
    run_collection()
