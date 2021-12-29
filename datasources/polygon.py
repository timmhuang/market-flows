import json
import logging
import pandas as pd
from polygon import RESTClient


BASE_URL = 'https://api.polygon.io/v2'


class PolygonRESTClientWrapper:
    def __init__(self, api_key=None, client=None):
        self.logger = logging.getLogger(str(self.__class__))
        if api_key:
            self.client = RESTClient(api_key)
        elif client:
            self.client = client
        else:
            raise ValueError("PolyRESTClient requires either an 'api_key' or 'client' object to be passed in")

    def get_daily_data(self, symbol, start_date, end_date):
        resp = self.client.stocks_equities_aggregates(symbol, 1, 'day', start_date, end_date)
        if not resp.results or len(resp.results) == 0:
            raise RuntimeError("No price history data return for {}".format(symbol))
        return parse_candle(resp)


def parse_candle(resp, daily=True):
    df = pd.read_json(json.dumps(resp.results))

    df['date'] = pd.to_datetime(df['t'], unit='ms')
    # df['date'] = df['date'].dt.localize('America/New_York')
    if daily:
        df['date'] = pd.to_datetime(df['date'].dt.date)
    df = df.drop(columns=['t', 'n'], errors='ignore')
    df.set_index('date', inplace=True)
    df = df.rename(columns={
        'v': 'volume', 'vw': 'vwap', 'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close'
    })
    return df

