import sys
import logging
import json
from pathlib import Path
from datetime import datetime

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

def run_collection(start_year=2025):
    print(f"=== Central Bank Speech Archiving Started (Since {start_year}) ===")
    db = SpeechDB()
    
    started_at = datetime.now().isoformat()
    bank_stats = {}
    total_new = 0
    overall_status = "success"
    error_msg = None

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
            # 지정된 연도부터 현재까지의 데이터 수집
            new_count = scraper.collect_new_speeches(start_year=start_year)
            bank_stats[scraper.BANK_CODE] = new_count
            total_new += new_count
            print(f"[+] {scraper.BANK_CODE}: {new_count} new speeches added.")
        except Exception as e:
            logger.error(f"Failed to collect {scraper.BANK_CODE}: {e}")
            bank_stats[scraper.BANK_CODE] = -1
            overall_status = "partial"
            error_msg = f"{error_msg} | " if error_msg else ""
            error_msg += f"{scraper.BANK_CODE}: {str(e)}"

    finished_at = datetime.now().isoformat()
    
    # 로그 기록
    conn = db._get_conn()
    try:
        conn.execute("""
            INSERT INTO collection_logs 
            (started_at, finished_at, status, bank_stats_json, error_message, total_new_speeches)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (started_at, finished_at, overall_status, json.dumps(bank_stats), error_msg, total_new))
        conn.commit()
    finally:
        conn.close()

    stats = db.get_stats()
    print("\n=== Collection Summary ===")
    for bank_code in db.BANKS:
        count = stats.get(bank_code, 0)
        print(f"- {bank_code}: {count} speeches")
    print(f"Total speeches in Archive: {stats.get('total', 0)}")
    print(f"New speeches added this run: {total_new}")
    print("===========================")

if __name__ == "__main__":
    # 실행 시 연도를 인자로 넘길 수 있음: python3 main.py 2019
    year = 2025
    if len(sys.argv) > 1 and sys.argv[1].isdigit():
        year = int(sys.argv[1])
    
    run_collection(start_year=year)
