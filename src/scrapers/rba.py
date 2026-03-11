"""
Reserve Bank of Australia (RBA) Speech Scraper

Source: https://www.rba.gov.au/speeches/
Note: RBA blocks direct requests to year subpages (403).
      We scrape the main speeches page which lists all speeches by year.
"""

import re
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)


class RBAScraper(BaseScraper):
    BANK_CODE = 'RBA'
    BANK_NAME = 'Reserve Bank of Australia'
    BASE_URL = 'https://www.rba.gov.au'

    def fetch_speech_list(self, year=None):
        """Fetch list of RBA speeches from the main speeches page."""
        # RBA blocks year-specific pages, so always fetch the main page
        url = f"{self.BASE_URL}/speeches/"
        resp = self._get(url)
        if not resp:
            return []

        soup = self._parse_html(resp.text)
        speeches = []

        # RBA lists speeches as links with URLs like /speeches/YYYY/sp-xxx-YYYY-MM-DD.html
        for link in soup.find_all('a', href=True):
            href = link['href']
            title = link.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            # Only match actual speech links
            if '/speeches/' not in href:
                continue
            if not href.endswith('.html') and not href.endswith('.htm'):
                continue
            # Skip index/navigation links
            if href.endswith('index.html') or href == '/speeches/':
                continue

            # Build absolute URL
            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                speech_url = f"{self.BASE_URL}/speeches/{href}"

            # Extract date from URL: sp-xxx-YYYY-MM-DD.html or mc-xxx-YYYY-MM-DD.html
            date_match = re.search(r'(\d{4})-(\d{2})-(\d{2})', href)
            date = ''
            if date_match:
                date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

            # Filter by year if specified
            if year and date and not date.startswith(str(year)):
                continue

            # Extract speaker code from URL: sp-gov, sp-dg, sp-ag, sp-so, mc-gov
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
        resp = self._get(url)
        if not resp:
            return None

        soup = self._parse_html(resp.text)

        content = (
            soup.find('div', id='content') or
            soup.find('article') or
            soup.find('div', class_='rba-content') or
            soup.find('main')
        )

        if content:
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style',
                                          'aside', 'button', 'form']):
                tag.decompose()
            return content.get_text(separator='\n', strip=True)

        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        """
        RBA main page lists all speeches across years.
        Override to fetch from main page and filter.
        """
        all_speeches = self.fetch_speech_list()
        if start_year:
            all_speeches = [s for s in all_speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            all_speeches = [s for s in all_speeches if s['date'] <= f"{end_year}-12-31"]
        return all_speeches
