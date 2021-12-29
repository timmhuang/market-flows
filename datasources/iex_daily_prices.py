"""
This is the class to store 1 year worth of parquet
Created by Kenneth Nursalim on 12/7/2020
"""

from pathlib import Path
import pandas as pd
import datetime as dt
import dateutil as dateutil

from .iex_daily_price import IEXDailyPrice
from .iex_splits import IEXSplits

class IEXDailyPrices:
    def __init__(self):
        self.data = []

    def initialize_from_dict(self, data_dict: list = []):
        for obj in data_dict:
            daily_price = IEXDailyPrice()
            if not daily_price.initialize_from_dict(obj):
                return False
            self.add_daily_price(daily_price)
        return True

    def get_count(self):
        return len(self.data)

    def get_daily_price(self, index):
        if index >= self.get_count():
            return None
        return self.data[index]

    def add_daily_price(self, daily_price: IEXDailyPrice):
        self.data.append(daily_price)

    def apply_split(self, ratio: float, split_date:dt.date):
        for daily_price in self.data:
            daily_price.apply_split(ratio, split_date)

    def apply_splits(self, splits: IEXSplits):
        count_splits = splits.get_count()
        for i in range(count_splits):
            split = splits.get_split(i)
            self.apply_split(split.ratio, split.execution_date)

    def to_dict(self):
        data_dict = []
        for daily_price in self.data:
            data_dict.append(daily_price.to_dict())
        return data_dict

    def to_parquet(self, save_path: str = "", is_local_disk_directory: bool = True):
        df = pd.DataFrame(self.to_dict())
        if is_local_disk_directory:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path = save_path.absolute()
        df.to_parquet(save_path, engine='fastparquet', compression='gzip')
        return True

    def from_parquet(self, parquet_path: str = ""):
        df = pd.read_parquet(parquet_path, engine='fastparquet')
        data = df.to_dict('records')
        return self.initialize_from_dict(data)

    def __eq__(self, other):
        if not isinstance(other, IEXDailyPrices):
            return False
        if self.get_count() != other.get_count():
            return False

        data_count = self.get_count()
        for i in range(data_count):
            if self.data[i] != other.data[i]:
                return False
        return True
