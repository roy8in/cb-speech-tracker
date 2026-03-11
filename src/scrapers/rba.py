"""
Reserve Bank of Australia (RBA) Speech Scraper

Source: https://www.rba.gov.au/speeches/
Note: RBA blocks direct requests to year subpages (403).
      Uses Playwright to bypass bot detection.
"""

import re
import logging
import time
from playwright.sync_api import sync_playwright
from .base import BaseScraper

logger = logging.getLogger(__name__)

class RBAScraper(BaseScraper):
    BANK_CODE = 'RBA'
    BANK_NAME = 'Reserve Bank of Australia'
    BASE_URL = 'https://www.rba.gov.au'

    def _get_playwright(self, url):
        """Use Playwright to get page content."""
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.HEADERS['User-Agent'])
            page = context.new_page()
            try:
                page.goto(url, wait_until='networkidle')
                # Wait a bit for potential JS redirects
                time.sleep(2)
                content = page.content()
                return content
            except Exception as e:
                logger.error(f"[{self.BANK_CODE}] Playwright failed for {url}: {e}")
                return None
            finally:
                browser.close()

    def fetch_speech_list(self, year=None):
        """Fetch list of RBA speeches using Playwright."""
        url = f"{self.BASE_URL}/speeches/"
        html = self._get_playwright(url)
        if not html:
            return []

        soup = self._parse_html(html)
        speeches = []

        # RBA lists speeches in a table or list
        for link in soup.find_all('a', href=True):
            href = link['href']
            title = link.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            if '/speeches/' not in href:
                continue
            if not href.endswith('.html') and not href.endswith('.htm'):
                continue
            if href.endswith('index.html') or href == '/speeches/':
                continue

            # Build absolute URL
            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                speech_url = f"{self.BASE_URL}/speeches/{href}"

            # Extract date from URL
            date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', href)
            date = ''
            if date_match:
                date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

            if year and date and not date.startswith(str(year)):
                continue

            # Extract speaker (Governor, Deputy Governor, etc.)
            speaker = None
            speaker_match = re.search(r'(sp|mc)-(gov|dg|ag|so)-', href)
            if speaker_match:
                speaker_code = speaker_match.group(2)
                speaker_map = {
                    'gov': 'Governor',
                    'dg': 'Deputy Governor',
                    'ag': 'Assistant Governor',
                    'so': 'Senior Officer',
                }
                speaker = speaker_map.get(speaker_code, speaker_code)

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': speaker,
            })

        # Deduplicate
        seen = set()
        unique = []
        for s in speeches:
            if s['url'] not in seen:
                seen.add(s['url'])
                unique.append(s)

        return unique

    def fetch_speech_text(self, url):
        """Fetch the full text of an RBA speech."""
        html = self._get_playwright(url)
        if not html:
            return None

        soup = self._parse_html(html)
        content = (
            soup.find('div', id='content') or
            soup.find('article') or
            soup.find('div', class_='rba-content') or
            soup.find('main')
        )

        if content:
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside']):
                tag.decompose()
            return content.get_text(separator='\n', strip=True)

        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        all_speeches = self.fetch_speech_list()
        if start_year:
            all_speeches = [s for s in all_speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            all_speeches = [s for s in all_speeches if s['date'] <= f"{end_year}-12-31"]
        return all_speeches
