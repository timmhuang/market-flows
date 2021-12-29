from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import requests


DEFAULT_USER_AGENT = {'User-Agent': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'}
DEFAULT_RETRY_STRAT = Retry(
    total=3,
    status_forcelist=[429, 500, 502, 503, 504],
    method_whitelist=["HEAD", "GET", "OPTIONS"]
)


class SinglePageScraper:
    """
    Scraper for a single web page
    """
    def __init__(self, url):
        self.url = url
        self._scraped_html = None

    @property
    def scraped_html(self):
        if not self._scraped_html:
            session = requests.Session()
            session.mount(self.url, HTTPAdapter(max_retries=DEFAULT_RETRY_STRAT))
            response = session.get(self.url, headers=DEFAULT_USER_AGENT)
            self.scraped_html = response.text

        return self._scraped_html

    @scraped_html.setter
    def scraped_html(self, html):
        self._scraped_html = html
