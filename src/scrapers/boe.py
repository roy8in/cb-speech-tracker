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
        url = f"{self.BASE_URL}/sitemap/speeches"
        resp = self._get(url)
        if not resp:
            resp = self._get(f"{self.BASE_URL}/news/speeches")
            if not resp:
                return []

        soup = self._parse_html(resp.text)
        speeches = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            title = link.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            if not any(pattern in href for pattern in ['/speech/', '/speeches/']):
                continue
            if href == '/sitemap/speeches' or href == '/news/speeches':
                continue

            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                speech_url = f"{self.BASE_URL}/{href}"

            date = self._extract_date_from_url(href, year)

            if year and date and not date.startswith(str(year)):
                continue

            # Advanced Speaker Extraction
            speaker = self.extract_speaker_from_title(title)

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': speaker,
            })

        seen = set()
        unique = []
        for s in speeches:
            if s['url'] not in seen:
                seen.add(s['url'])
                unique.append(s)

        return unique

    @staticmethod
    def extract_speaker_from_title(title):
        """
        Extract speaker name from BOE title using common patterns.
        """
        # Clean title from (pdf ...) info
        clean_title = re.sub(r'\(pdf\s*.*\)', '', title, flags=re.IGNORECASE).strip()
        
        # Pattern 1: Title [dash] speech/remarks/slides by Name
        m = re.search(r'.+[−–-]\s*(?:speech|remarks|slides|panel remarks|address)\s+by\s+([^−–-]+)$', clean_title, re.IGNORECASE)
        if m:
            return m.group(1).strip()
            
        # Pattern 2: Name: Title
        if ':' in clean_title:
            potential = clean_title.split(':')[0].strip()
            # Names are usually 2-4 words
            if 1 < len(potential.split()) < 5 and not any(w in potential.lower() for w in ['at', 'the', 'meeting', 'update']):
                return potential
                
        # Pattern 3: Slides from Name's ...
        m = re.search(r'Slides\s+from\s+([^’\']+)[’\']s', clean_title, re.IGNORECASE)
        if m:
            return m.group(1).strip()
            
        return None

    def _extract_date_from_url(self, href, default_year):
        """Extract date from BOE URL patterns."""
        from datetime import datetime
        match = re.search(r'/(\d{4})/(\w+)', href)
        if match:
            year = match.group(1)
            month_str = match.group(2)
            for fmt in ['%B', '%b']:
                try:
                    month = datetime.strptime(month_str, fmt).month
                    return f"{year}-{month:02d}-01"
                except ValueError:
                    continue
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
        all_speeches = self.fetch_speech_list()
        if start_year:
            all_speeches = [s for s in all_speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            all_speeches = [s for s in all_speeches if s['date'] <= f"{end_year}-12-31"]
        return all_speeches
