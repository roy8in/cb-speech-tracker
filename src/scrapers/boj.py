"""
Bank of Japan (BOJ) Speech Scraper — English version

Source: https://www.boj.or.jp/en/announcements/press/koen.htm
Year index: https://www.boj.or.jp/en/announcements/press/koen/koen{YYYY}.htm
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
        """Fetch list of BOJ speeches (English) for a given year."""
        if year is None:
            from datetime import datetime
            year = datetime.now().year

        url = f"{self.BASE_URL}/en/about/press/koen_{year}/index.htm"
        resp = self._get(url)
        if not resp:
            return []

        soup = self._parse_html(resp.text)
        speeches = []

        # BOJ uses table or list format for speeches
        # Try table rows first
        rows = soup.select('table tr, .list-data li, .list01 li, div.what_new li')

        if not rows:
            # Alternative: look for all links in the main content
            content = soup.find('div', id='main') or soup.find('main') or soup
            rows = content.find_all(['tr', 'li'])

        for row in rows:
            try:
                link = row.find('a', href=True)
                if not link:
                    continue

                title = link.get_text(strip=True)
                if not title or len(title) < 5:
                    continue

                href = link['href']
                if href.startswith('/'):
                    speech_url = f"{self.BASE_URL}{href}"
                elif href.startswith('http'):
                    speech_url = href
                else:
                    speech_url = f"{self.BASE_URL}/en/about/press/koen_{year}/{href}"

                # Skip non-speech links
                if not any(x in href.lower() for x in ['koen', 'ko2', 'press', 'speech']):
                    if not href.endswith('.htm') and not href.endswith('.html'):
                        continue

                # Date extraction
                date = self._extract_date(row, href, year)

                # Speaker extraction
                speaker = self._extract_speaker(row, title)

                speeches.append({
                    'title': title,
                    'date': date,
                    'url': speech_url,
                    'speaker': speaker,
                })
            except Exception as e:
                logger.warning(f"[BOJ] Error parsing entry: {e}")
                continue

        return speeches

    def _extract_date(self, row, href, year):
        """Extract date from row text or URL."""
        from datetime import datetime

        # Try date element
        date_tag = row.find('time') or row.find(class_=re.compile(r'date'))
        if date_tag:
            date_str = date_tag.get('datetime', '') or date_tag.get_text(strip=True)
            for fmt in ['%Y-%m-%d', '%B %d, %Y', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(date_str.strip()[:10], fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue

        # Try from URL: ko{YYMMDD} or ko{YYYYMMDD}
        match = re.search(r'ko(\d{6,8})', href)
        if match:
            d = match.group(1)
            if len(d) == 6:
                return f"20{d[:2]}-{d[2:4]}-{d[4:6]}"
            elif len(d) == 8:
                return f"{d[:4]}-{d[4:6]}-{d[6:8]}"

        # Try text content for date patterns like "March 5, 2026"
        text = row.get_text()
        date_match = re.search(
            r'(\w+)\s+(\d{1,2}),?\s+(\d{4})', text)
        if date_match:
            try:
                dt = datetime.strptime(
                    f"{date_match.group(1)} {date_match.group(2)}, {date_match.group(3)}",
                    "%B %d, %Y")
                return dt.strftime('%Y-%m-%d')
            except ValueError:
                pass

        return f"{year}-01-01"

    def _extract_speaker(self, row, title):
        """Extract speaker from row or title."""
        text = row.get_text()

        # Common BOJ speaker patterns
        patterns = [
            r'(?:Governor|Deputy Governor|Board Member|Chairman)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
            r'by\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)',
        ]
        for p in patterns:
            match = re.search(p, text)
            if match:
                return match.group(1).strip()

        return None

    def fetch_speech_text(self, url):
        """Fetch the full text of a BOJ speech (English)."""
        resp = self._get(url)
        if not resp:
            return None

        soup = self._parse_html(resp.text)

        content = (
            soup.find('div', id='main') or
            soup.find('div', class_='content') or
            soup.find('article') or
            soup.find('main')
        )

        if content:
            for tag in content.find_all(['nav', 'header', 'footer', 'script', 'style',
                                          'aside', 'button', 'form']):
                tag.decompose()
            return content.get_text(separator='\n', strip=True)

        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        """BOJ English speeches available from ~2000."""
        return super().get_all_speeches(
            start_year=start_year or 2000,
            end_year=end_year,
        )
