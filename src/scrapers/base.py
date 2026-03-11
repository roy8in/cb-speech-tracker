"""
Base scraper class for central bank speeches.
All 6 scrapers inherit from this.
"""

import requests
import logging
import time
import urllib3
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup
from datetime import datetime

# Suppress SSL warnings for corporate proxy environments
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

logger = logging.getLogger(__name__)


class BaseScraper(ABC):
    """Abstract base class for central bank speech scrapers."""

    BANK_CODE = None  # Override in subclasses: 'FRB', 'ECB', etc.
    BANK_NAME = None  # Override: 'Federal Reserve', etc.
    BASE_URL = None   # Override: base URL of the speeches page

    # Polite scraping defaults
    REQUEST_DELAY = 2.0  # seconds between requests
    REQUEST_TIMEOUT = 30
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'Cache-Control': 'max-age=0',
    }

    def __init__(self, db=None):
        from src.models import SpeechDB
        self.db = db or SpeechDB()
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def _get(self, url, **kwargs):
        """Make a GET request with delay and error handling."""
        time.sleep(self.REQUEST_DELAY)
        try:
            resp = self.session.get(url, timeout=self.REQUEST_TIMEOUT, verify=False, **kwargs)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            logger.error(f"[{self.BANK_CODE}] Request failed for {url}: {e}")
            return None

    def _parse_html(self, html_text):
        """Parse HTML with BeautifulSoup."""
        return BeautifulSoup(html_text, 'html.parser')

    @abstractmethod
    def fetch_speech_list(self, year=None):
        """
        Fetch list of speeches from the central bank website.

        Returns list of dicts:
        [
            {
                'title': str,
                'date': str (YYYY-MM-DD),
                'url': str (full URL),
                'speaker': str or None,
            },
            ...
        ]
        """
        pass

    @abstractmethod
    def fetch_speech_text(self, url):
        """
        Fetch the full text of a speech from its URL.

        Returns: str (full text) or None
        """
        pass

    def get_all_speeches(self, start_year=None, end_year=None):
        """
        Fetch ALL available speeches across all years.
        Override in subclasses if year-based pagination is needed.
        """
        current_year = datetime.now().year
        start = start_year or 2000
        end = end_year or current_year

        all_speeches = []
        for year in range(end, start - 1, -1):  # newest first
            try:
                speeches = self.fetch_speech_list(year=year)
                if speeches:
                    all_speeches.extend(speeches)
                    logger.info(f"[{self.BANK_CODE}] {year}: {len(speeches)} speeches found")
            except Exception as e:
                logger.warning(f"[{self.BANK_CODE}] Failed to fetch {year}: {e}")
                continue
        return all_speeches

    def collect_new_speeches(self, start_year=None, fetch_text=True):
        """
        Main collection method: fetch new speeches and save to DB.
        Returns count of new speeches added.
        """
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)
        speech_list = self.get_all_speeches(start_year=start_year)

        new_count = 0
        for speech_info in speech_list:
            url = speech_info['url']
            if url in existing_urls:
                continue

            full_text = None
            if fetch_text:
                full_text = self.fetch_speech_text(url)
                
                # Check for embedded metadata from specific scrapers
                if full_text:
                    lines = full_text.split("\n")
                    new_text_lines = []
                    for line in lines:
                        if line.startswith("__DATE__:"):
                            speech_info['date'] = line.replace("__DATE__:", "").strip()
                        elif line.startswith("__SPEAKER__:"):
                            speech_info['speaker'] = line.replace("__SPEAKER__:", "").strip()
                        else:
                            new_text_lines.append(line)
                    full_text = "\n".join(new_text_lines).strip()

                if full_text:
                    logger.info(f"[{self.BANK_CODE}] Fetched: {speech_info['title'][:60]}...")

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get('speaker'),
                title=speech_info['title'],
                date=speech_info['date'],
                url=url,
                full_text=full_text,
            )

            if speech_id:
                new_count += 1

        logger.info(f"[{self.BANK_CODE}] Collection complete: {new_count} new speeches added")
        return new_count

    def collect_recent(self, fetch_text=True):
        """Collect only the current year's speeches (for daily runs)."""
        current_year = datetime.now().year
        existing_urls = self.db.get_existing_urls(self.BANK_CODE)

        speech_list = self.fetch_speech_list(year=current_year)
        if not speech_list:
            return 0

        new_count = 0
        for speech_info in speech_list:
            url = speech_info['url']
            if url in existing_urls:
                continue

            full_text = speech_info.pop('_full_text', None)
            if fetch_text and not full_text:
                full_text = self.fetch_speech_text(url)

            speech_id = self.db.insert_speech(
                bank_code=self.BANK_CODE,
                speaker=speech_info.get('speaker'),
                title=speech_info['title'],
                date=speech_info['date'],
                url=url,
                full_text=full_text,
            )
            if speech_id:
                new_count += 1

        return new_count
