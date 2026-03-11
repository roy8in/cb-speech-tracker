"""
Bank of England (BOE) Speech Scraper

Source: https://www.bankofengland.co.uk/news/speeches
Sitemap: https://www.bankofengland.co.uk/sitemap/speeches
"""

import re
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOEScraper(BaseScraper):
    BANK_CODE = 'BOE'
    BANK_NAME = 'Bank of England'
    BASE_URL = 'https://www.bankofengland.co.uk'

    def fetch_speech_list(self, year=None):
        """Fetch list of BOE speeches from sitemap."""
        # BOE main speeches page uses JS rendering; use sitemap instead
        url = f"{self.BASE_URL}/sitemap/speeches"
        resp = self._get(url)
        if not resp:
            # Fallback to main page
            resp = self._get(f"{self.BASE_URL}/news/speeches")
            if not resp:
                return []

        soup = self._parse_html(resp.text)
        speeches = []

        # Find all speech links
        for link in soup.find_all('a', href=True):
            href = link['href']
            title = link.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            # BOE speech URLs: /speech/YYYY/speech-title or /news/YYYY/month/speech-title
            if not any(pattern in href for pattern in ['/speech/', '/speeches/']):
                continue
            # Skip sitemap/navigation links
            if href == '/sitemap/speeches' or href == '/news/speeches':
                continue

            # Build absolute URL
            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                speech_url = f"{self.BASE_URL}/{href}"

            # Extract date from URL or text
            date = self._extract_date_from_url(href, year)

            # Filter by year if specified
            if year and date and not date.startswith(str(year)):
                continue

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': None,  # BOE doesn't always include speaker in list
            })

        # Deduplicate
        seen = set()
        unique = []
        for s in speeches:
            if s['url'] not in seen:
                seen.add(s['url'])
                unique.append(s)

        return unique

    def _extract_date_from_url(self, href, default_year):
        """Extract date from BOE URL patterns."""
        from datetime import datetime

        # Pattern: /speech/YYYY/month-day/title or /YYYY/month/title
        match = re.search(r'/(\d{4})/(\w+)', href)
        if match:
            year = match.group(1)
            month_str = match.group(2)
            # Try to parse month name
            for fmt in ['%B', '%b']:
                try:
                    month = datetime.strptime(month_str, fmt).month
                    return f"{year}-{month:02d}-01"
                except ValueError:
                    continue
            # If just digits
            if month_str.isdigit():
                return f"{year}-{month_str.zfill(2)}-01"
            return f"{year}-01-01"

        if default_year:
            return f"{default_year}-01-01"
        return ''

    def fetch_speech_text(self, url):
        """Fetch the full text of a BOE speech."""
        resp = self._get(url)
        if not resp:
            return None

        soup = self._parse_html(resp.text)

        content = (
            soup.find('div', class_='page-content') or
            soup.find('article') or
            soup.find('div', class_='content-block') or
            soup.find('main')
        )

        if content:
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style',
                                          'aside', 'button']):
                tag.decompose()
            return content.get_text(separator='\n', strip=True)

        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        """BOE sitemap lists all speeches."""
        all_speeches = self.fetch_speech_list()
        if start_year:
            all_speeches = [s for s in all_speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            all_speeches = [s for s in all_speeches if s['date'] <= f"{end_year}-12-31"]
        return all_speeches
