"""
Bank of Canada (BOC) Speech Scraper - Final Attempt
Uses Playwright to grab all links containing '/press/speeches/'
"""

import re
import logging
import time
from playwright.sync_api import sync_playwright
from .base import BaseScraper

logger = logging.getLogger(__name__)

class BOCScraper(BaseScraper):
    BANK_CODE = 'BOC'
    BANK_NAME = 'Bank of Canada'
    BASE_URL = 'https://www.bankofcanada.ca'

    def _get_playwright(self, url):
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(user_agent=self.HEADERS['User-Agent'])
            page = context.new_page()
            try:
                page.goto(url, wait_until='networkidle', timeout=60000)
                time.sleep(5) # Give more time for dynamic content
                return page.content()
            except Exception as e:
                logger.error(f"[{self.BANK_CODE}] Playwright failed for {url}: {e}")
                return None
            finally:
                browser.close()

    def fetch_speech_list(self, year=None):
        url = f"{self.BASE_URL}/press/speeches/"
        if year:
            url = f"{url}?filter_year={year}"
            
        html = self._get_playwright(url)
        if not html:
            return []

        soup = self._parse_html(html)
        speeches = []

        # Find ALL links on the page and filter by pattern
        for link in soup.find_all('a', href=True):
            href = link['href']
            title = link.get_text(strip=True)
            
            # BOC speech links pattern: /press/speeches/YYYY/MM/title/ or /press/speeches/YYYY/title/
            if '/press/speeches/' in href and len(title) > 10:
                # Skip index or general pages
                if href.endswith('/speeches/') or 'filter_year' in href:
                    continue
                
                # Extract date from URL if possible
                date_match = re.search(r'/speeches/(\d{4})/(\d{2})/(\d{2})/', href)
                date = ''
                if date_match:
                    date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
                else:
                    # Try year only
                    year_match = re.search(r'/speeches/(\d{4})/', href)
                    if year_match:
                        date = f"{year_match.group(1)}-01-01"

                speeches.append({
                    'title': title,
                    'date': date,
                    'url': href if href.startswith('http') else f"{self.BASE_URL}{href}",
                    'speaker': None,
                })

        # Deduplicate by URL
        unique_speeches = {s['url']: s for s in speeches}.values()
        return list(unique_speeches)

    def fetch_speech_text(self, url):
        resp = self._get(url)
        if not resp:
            return None
        soup = self._parse_html(resp.text)
        # Content is usually in a div with some entry/content class
        content = soup.find('div', class_='entry-content') or soup.find('article') or soup.find('main')
        if content:
            for tag in content.find_all(['nav', 'script', 'style', 'footer', 'header']):
                tag.decompose()
            return content.get_text(separator='\n', strip=True)
        return None
