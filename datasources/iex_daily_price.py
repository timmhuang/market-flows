"""
Simple data struct to save daily price structure on iex data
Created by Kenneth Nursalim on 12/6/2020
"""

from pathlib import Path
import pandas as pd
import datetime as dt
import dateutil as dateutil
import numpy as np
import pytz

from flows.util.util import IEXUtil

utc = pytz.UTC

class IEXDailyPrice:
    def __init__(self, date=dt.date.today(), date_last_adjusted=dt.date.today(), symbol="", open=0, close=0, high=0, low=0, volume=0,\
                unadjusted_open=0, unadjusted_close=0, unadjusted_high=0, unadjusted_low=0, unadjusted_volume=0):
        self.date               : dt.date   = date
        self.date_last_adjusted : dt.date   = date_last_adjusted
        self.symbol             : str       = symbol
        self.open               : float     = open
        self.close              : float     = close
        self.high               : float     = high
        self.low                : float     = low
        self.volume             : int       = volume
        self.unadjusted_open    : float     = unadjusted_open
        self.unadjusted_close   : float     = unadjusted_close
        self.unadjusted_high    : float     = unadjusted_high
        self.unadjusted_low     : float     = unadjusted_low
        self.unadjusted_volume  : int       = unadjusted_volume
    
    def initialize_from_dict(self, data_dict: dict = {}):
        if  not "date" in data_dict or\
            not "symbol" in data_dict or\
            not "open" in data_dict or\
            not "close" in data_dict or\
            not "high" in data_dict or\
            not "low" in data_dict or\
            not "volume" in data_dict or\
            not "uOpen" in data_dict or\
            not "uClose" in data_dict or\
            not "uHigh" in data_dict or\
            not "uLow" in data_dict or\
            not "uVolume" in data_dict:
            return False

        iso_date_last_adjusted = data_dict["date"]
        if "date_last_adjusted" in data_dict:
            iso_date_last_adjusted = data_dict["date_last_adjusted"]
        
        self.date = IEXUtil.convert_to_date(data_dict['date'])
        self.date_last_adjusted = IEXUtil.convert_to_date(iso_date_last_adjusted)

        self.symbol = data_dict['symbol']
        self.open = data_dict['open']
        self.close = data_dict['close']
        self.high = data_dict['high']
        self.low = data_dict['low']
        self.volume = data_dict['volume']
        self.unadjusted_open = data_dict['uOpen']
        self.unadjusted_close = data_dict['uClose']
        self.unadjusted_high = data_dict['uHigh']
        self.unadjusted_low = data_dict['uLow']
        self.unadjusted_volume = data_dict['uVolume']

        return True

    # E.g. ratio of 0.5 means 1 share is split into 2; '2' means vice-versa
    def apply_split(self, ratio, split_date: dt.date):
        if ratio <= 0:
            return False

        if split_date <= self.date_last_adjusted:
            return False

        self.open =  float(np.round(self.open * ratio, 2))
        self.close = float(np.round(self.close * ratio, 2))
        self.high = float(np.round(self.high * ratio, 2))
        self.low = float(np.round(self.low * ratio, 2))
        self.volume *= int(1 / ratio)

        self.date_last_adjusted = split_date
        return True

    def to_dict(self):
        data: dict = {
            "date"              : self.date.isoformat(),
            "date_last_adjusted": self.date_last_adjusted.isoformat(),
            "symbol"            : self.symbol,
            "open"              : self.open,
            "close"             : self.close,
            "high"              : self.high,
            "low"               : self.low,
            "volume"            : self.volume,
            "uOpen"             : self.unadjusted_open,
            "uClose"            : self.unadjusted_close,
            "uHigh"             : self.unadjusted_high,
            "uLow"              : self.unadjusted_low,
            "uVolume"           : self.unadjusted_volume
        }
        return data

    def to_parquet(self, save_path: str = "", is_local_disk_directory: bool = True):
        df = pd.DataFrame(self.to_dict(), index=[0])
        if is_local_disk_directory:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path = save_path.absolute()
        df.to_parquet(save_path, engine='fastparquet', compression='gzip')
        return True

    def from_parquet(self, parquet_path: str = ""):
        df = pd.read_parquet(parquet_path, engine='fastparquet')
        data = df.to_dict('records')[0]
        return self.initialize_from_dict(data)

    def __eq__(self, other):
        if not isinstance(other, IEXDailyPrice):
            return False
        return  self.date == other.date and\
                self.date_last_adjusted == other.date_last_adjusted and\
                self.symbol == other.symbol and\
                self.open == other.open and\
                self.close == other.close and\
                self.high == other.high and\
                self.low == other.low and\
                self.volume == other.volume and\
                self.unadjusted_open == other.unadjusted_open and\
                self.unadjusted_close == other.unadjusted_close and\
                self.unadjusted_high == other.unadjusted_high and\
                self.unadjusted_low == other.unadjusted_low and\
                self.unadjusted_volume == other.unadjusted_volume
