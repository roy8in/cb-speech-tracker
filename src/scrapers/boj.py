"""
Bank of Japan (BOJ) Speech Scraper

Source: https://www.boj.or.jp/en/about/press/koen_year/index.htm
"""

import re
import logging
from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOJScraper(BaseScraper):
    BANK_CODE = 'BOJ'
    BANK_NAME = 'Bank of Japan'
    BASE_URL = 'https://www.boj.or.jp'

    def fetch_speech_list(self, year=None):
        """Fetch list of BOJ speeches for a given year."""
        year_str = str(year) if year else "2026"
        url = f"{self.BASE_URL}/en/about/press/koen_{year_str}/index.htm"
        
        resp = self._get(url)
        if not resp:
            return []

        soup = self._parse_html(resp.text)
        speeches = []

        # BOJ lists speeches in a section with class 'section' or 'main'
        container = soup.find('div', id='main') or soup.find('div', class_='section')
        if not container:
            return []

        for li in container.find_all('li'):
            link = li.find('a', href=True)
            if not link:
                continue

            title = link.get_text(strip=True)
            href = link['href']

            if not title or len(title) < 5:
                continue

            # BOJ speech links usually start with /en/about/press/koen_YYYY/
            if '/koen_' not in href:
                continue

            # Build absolute URL
            speech_url = f"{self.BASE_URL}{href}" if href.startswith('/') else href

            # Extract date from the list item text (e.g., "Mar.  3, 2026")
            date_text = li.get_text(strip=True)
            date = self._parse_boj_date(date_text)
            
            # Extract speaker from title parentheses if present
            speaker = None
            # Pattern: (Speech by Governor UEDA Kazuo)
            m = re.search(r'\((?:Speech|Remarks|Address)\s+by\s+(?:Governor\s+|Deputy Governor\s+)?([^)]+)\)', title, re.IGNORECASE)
            if m:
                speaker = m.group(1).strip()

            speeches.append({
                'title': title,
                'date': date or f"{year_str}-01-01",
                'url': speech_url,
                'speaker': speaker,
            })

        return speeches

    def _parse_boj_date(self, text):
        """Parse BOJ date format (e.g., 'Mar. 3, 2026')."""
        from datetime import datetime
        # Regex to find something like "Mar. 3, 2026"
        match = re.search(r'([A-Za-z]+\.?\s+\d{1,2},\s+\d{4})', text)
        if match:
            date_str = match.group(1).replace('.', '')
            for fmt in ['%b %d, %Y', '%B %d, %Y']:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        return None

    def fetch_speech_text(self, url):
        """Fetch the full text and extract speaker from the body."""
        resp = self._get(url)
        if not resp:
            return None

        soup = self._parse_html(resp.text)
        content = soup.find('div', id='main') or soup.find('div', class_='section') or soup.find('main')
        
        if content:
            # 1. Extract speaker from the first few paragraphs
            # Pattern: UEDA Kazuo Governor of the Bank of Japan
            speaker = None
            first_p = content.find('p')
            if first_p:
                p_text = first_p.get_text(strip=True)
                # Look for name before "Governor" or "Deputy Governor"
                m = re.search(r'^([^,]+?)\s+(?:Governor|Deputy Governor)', p_text)
                if m:
                    speaker = m.group(1).strip()
            
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style', 'aside']):
                tag.decompose()
            
            text = content.get_text(separator='\n', strip=True)
            if speaker:
                return f"__SPEAKER__:{speaker}\n{text}"
            return text
            
        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        from datetime import datetime
        current_year = datetime.now().year
        start = start_year or 2019
        end = end_year or current_year

        all_speeches = []
        for year in range(end, start - 1, -1):
            speeches = self.fetch_speech_list(year=year)
            if speeches:
                all_speeches.extend(speeches)
        return all_speeches
