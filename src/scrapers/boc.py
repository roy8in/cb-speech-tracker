"""
Bank of Canada (BOC) Speech Scraper

Source: https://www.bankofcanada.ca/press/speeches/
List page uses pagination via ?mt_page=N
Speech URLs: /YYYY/MM/slug/
Multimedia (webcasts) URLs: /multimedia/slug/ (excluded)
"""

import re
import logging
from datetime import datetime
from .base import BaseScraper

logger = logging.getLogger(__name__)


class BOCScraper(BaseScraper):
    BANK_CODE = 'BOC'
    BANK_NAME = 'Bank of Canada'
    BASE_URL = 'https://www.bankofcanada.ca'
    SPEECHES_URL = f'{BASE_URL}/press/speeches/'

    def fetch_speech_list(self, year=None):
        """
        Fetch list of BOC speeches with pagination support.
        If year is given, filter results to that year.
        """
        all_speeches = []
        page = 1
        max_pages = 50  # safety limit

        while page <= max_pages:
            url = self.SPEECHES_URL
            if page > 1:
                url = f"{self.SPEECHES_URL}?mt_page={page}"

            resp = self._get(url)
            if not resp:
                break

            soup = self._parse_html(resp.text)
            speeches_on_page = self._parse_speech_list_page(soup)

            if not speeches_on_page:
                break  # no more results

            all_speeches.extend(speeches_on_page)
            page += 1

            # Check if there's a next page
            if not self._has_next_page(soup):
                break

        # Filter by year if specified
        if year:
            year_str = str(year)
            all_speeches = [s for s in all_speeches if s['date'].startswith(year_str)]

        # Deduplicate by URL
        unique = {s['url']: s for s in all_speeches}
        logger.info(f"[{self.BANK_CODE}] Found {len(unique)} speeches")
        return list(unique.values())

    def _parse_speech_list_page(self, soup):
        """Parse a single page of the speech list."""
        speeches = []

        # Each speech entry has an <h3> with a link
        for h3 in soup.find_all('h3'):
            link = h3.find('a', href=True)
            if not link:
                continue

            href = link['href']
            title = link.get_text(strip=True)

            if not title or len(title) < 10:
                continue

            # Skip multimedia/webcast links - only collect text speeches
            if '/multimedia/' in href:
                continue

            # Only collect links matching /YYYY/MM/ pattern (actual speech pages)
            if not re.search(r'/\d{4}/\d{2}/', href):
                continue

            # Build absolute URL
            if href.startswith('/'):
                speech_url = f"{self.BASE_URL}{href}"
            elif href.startswith('http'):
                speech_url = href
            else:
                continue

            # Extract date from URL: /YYYY/MM/slug/
            date = ''
            date_match = re.search(r'/(\d{4})/(\d{2})/', href)
            if date_match:
                date = f"{date_match.group(1)}-{date_match.group(2)}-01"

            # Extract speaker from nearby /profile/ link
            speaker = self._extract_speaker(h3)

            speeches.append({
                'title': title,
                'date': date,
                'url': speech_url,
                'speaker': speaker,
            })

        return speeches

    def _extract_speaker(self, h3_tag):
        """Extract speaker name from the /profile/ link near the h3 tag."""
        # Look in the parent container for a profile link
        parent = h3_tag.parent
        if not parent:
            return None

        # Search up to 2 levels of parents
        for _ in range(2):
            profile_link = parent.find('a', href=re.compile(r'/profile/'))
            if profile_link:
                return profile_link.get_text(strip=True)
            parent = parent.parent
            if not parent:
                break

        return None

    def _has_next_page(self, soup):
        """Check if there's a next page in pagination."""
        # BOC uses .page-numbers for pagination
        pagination = soup.find('a', class_='next') or soup.find('a', string=re.compile(r'Next|›|»'))
        if pagination:
            return True

        # Also check for page-numbers links
        page_links = soup.find_all('a', class_='page-numbers')
        if page_links:
            # If there are page number links, check if any is 'next'
            for link in page_links:
                if 'next' in link.get('class', []):
                    return True
                # If current page number exists and there's a higher one
                text = link.get_text(strip=True)
                if text.isdigit():
                    return True  # There are more pages available

        return False

    def fetch_speech_text(self, url):
        """Fetch the full text of a BOC speech."""
        resp = self._get(url)
        if not resp:
            return None

        soup = self._parse_html(resp.text)

        # Try multiple content selectors
        content = (
            soup.find('div', class_='page-content') or
            soup.find('div', class_='post-content') or
            soup.find('article') or
            soup.find('main')
        )

        if content:
            # Remove non-content elements
            for tag in content.find_all(['nav', 'script', 'style', 'footer',
                                          'header', 'aside']):
                tag.decompose()
            # Remove related-info sidebars
            for tag in content.find_all('div', class_='related-info'):
                tag.decompose()

            return content.get_text(separator='\n', strip=True)

        return None

    def get_all_speeches(self, start_year=None, end_year=None):
        """
        BOC uses pagination, not year-based URLs.
        Fetch all and filter by year range.
        """
        all_speeches = self.fetch_speech_list()

        if start_year:
            all_speeches = [s for s in all_speeches if s['date'] >= f"{start_year}-01-01"]
        if end_year:
            all_speeches = [s for s in all_speeches if s['date'] <= f"{end_year}-12-31"]

        return all_speeches
