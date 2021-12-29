import time
import logging
import json
from datetime import datetime
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
from flows.datasources.base import DataSource, TokenAuth
from iexfinance.stocks import get_historical_data

from enum import Enum

logger = logging.getLogger('iex')
BASE_URL = 'https://cloud.iexapis.com/v1'

class eIEXAPIRange(Enum):
    eNext           = "next"
    eOneMonth       = "1m"
    eThreeMonth     = "3m"
    eSixMonth       = "6m"
    eYearToDate     = "ytd"
    eOneYear        = "1y"
    eTwoYear        = "2y"
    eFiveYear       = "5y"

class IEXClient(TokenAuth):
    def __init__(self, token_api_key='token', token_env_key='IEX_TOKEN', token_value=None):
        self.logger = logging.getLogger(str(self.__class__))
        super().__init__(token_api_key, token_env_key, token_value)
        self.session = requests.Session()
        self.session.mount(BASE_URL, HTTPAdapter(max_retries=Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )))

    def get_last_trade_date(self, from_date: datetime):
        """
        https://iexcloud.io/docs/api/#u-s-holidays-and-trading-dates
        Takes YYYYMMDD format for from_date parameter
        :return: Response
        """
        endpoint = 'ref-data/us/dates/trade/last/1/{}'.format(from_date.strftime('%Y%m%d'))
        res = self.session.get('/'.join([BASE_URL, endpoint]), params=self.params)
        return datetime.strptime(json.loads(res.text)[0]['date'], '%Y-%m-%d').date()

    def get_daily_price(self, symbol: str):
        endpoint = 'stock/{}/previous'.format(symbol)
        res = self.session.get('/'.join([BASE_URL, endpoint]), params=self.params)
        return res

    def get_split(self, symbol: str, eRange: eIEXAPIRange = eIEXAPIRange.eOneMonth):
        endpoint = "stock/{}/splits/{}".format(symbol, eRange.value)
        res = self.session.get('/'.join([BASE_URL, endpoint]), params=self.params)
        return res

    def get_historical_price(self, symbol: str, range: str = '5y'):
        endpoint = '/stock/{}/chart/{}'.format(symbol, range)
        res = self.session.get('/'.join([BASE_URL, endpoint]), params=self.params)
        return res

    def get_dividends(self, symbol: str, eRange: eIEXAPIRange = eIEXAPIRange.eOneMonth):
        endpoint = '/stock/{}/dividends/{}'.format(symbol, eRange.value)
        res = self.session.get('/'.join([BASE_URL, endpoint]), params=self.params)
        return res

class IEXPriceHistory(DataSource):
    def __init__(self, start, end, ticker):
        self.start_date = start
        self.end_date = end
        self.ticker = ticker

    def get_data(self):
        start_time = time.time()
        data = get_historical_data(self.ticker, self.start_date, self.end_date, close_only=True, output_format='pandas')

        logger.info("Successfully fetch {days} days prices for {ticker} \t in {ms:.2f}ms".format(
            days=len(data.index),
            ticker=self.ticker,
            ms=(time.time() - start_time) * 1000
        ))
        return data.astype({'close': 'float32', 'volume': 'int32'})
