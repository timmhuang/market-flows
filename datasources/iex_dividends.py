"""
Class to wrap dividends data.
Allows to do usert/conditional insert

Created by Kenneth Nursalim on 12/15/2020
"""
import json

import bisect
from typing import List

from flows.datasources.iex_dividend import IEXDividend
from flows.util.parquet_util import ParquetUtil

class IEXDividends:
    def __init__(self):
        self.dividends: list = []

    def initialize_from_iex_response(self, response_text: str):
        response_json = json.loads(response_text)
        return self.initialize_from_dict(response_json, True)

    def initialize_from_dict(self, data_dict: list, epoch_in_ms=False):
        count = len(data_dict)
        for i in range(count):
            data = data_dict[i]
            dividend: IEXDividend = IEXDividend()
            if not dividend.initialize_from_dict(data, epoch_in_ms):
                return False
            self.add_dividend_if_not_exist(dividend)
        return True

    def to_dict(self):
        data_dict: list = []
        dividend_count = self.get_count()
        for i in range(dividend_count):
            dividend: IEXDividend = self.get_dividend(i)
            data = dividend.to_dict()
            data_dict.append(data)
        return data_dict

    def add_dividends(self, dividends):
        count = dividends.get_count()
        for i in range(count):
            dividend: IEXDividend = dividends.get_dividend(i)
            self.add_dividend_if_not_exist(dividend)

    def add_dividend_if_not_exist(self, dividend: IEXDividend):
        if self.contains_dividend(dividend):
            return False
        bisect.insort(self.dividends, dividend)
        return True

    def contains_dividend(self, dividend: IEXDividend):
        index = bisect.bisect_left(self.dividends, dividend)
        return index != self.get_count() and self.get_dividend(index) == dividend

    def get_count(self):
        return len(self.dividends)

    def get_dividend(self, index):
        if index >= self.get_count():
            return None
        return self.dividends[index]
    
    def to_parquet(self, save_path: str = "", is_local_disk_directory: bool = True):
        return ParquetUtil.save_dict_to_parquet(self.to_dict(), save_path, is_local_disk_directory)

    def from_parquet(self, parquet_path: str = ""):
        dividends_dict = ParquetUtil.load_dict_from_parquet(parquet_path)
        if dividends_dict is None:
            return False
        return self.initialize_from_dict(dividends_dict)

    def __eq__(self, other):
        if not isinstance(other, IEXDividends):
            return False
        count = self.get_count()
        for i in range(count):
            if self.dividends[i] != other.get_dividend(i):
                return False
        return True
