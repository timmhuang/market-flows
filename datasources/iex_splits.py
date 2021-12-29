"""
Class to sort the splits that may have occurred on a certain range
Created by Kenneth Nursalim on 12/9/2020
"""

import json
import bisect

from typing import List
from flows.datasources.iex_split import IEXSplit
from flows.util.parquet_util import ParquetUtil

class IEXSplits:
    def __init__(self):
        self.splits: List[IEXSplit] = []

    def initialize_from_iex_response(self, response_text):
        response_json = json.loads(response_text)
        return self.initialize_from_dict(response_json, True)

    def initialize_from_dict(self, data_dict: list, epoch_in_ms=False):
        count = len(data_dict)
        for i in range(count):
            data = data_dict[i]
            split: IEXSplit = IEXSplit()
            if not split.initialize_from_dict(data, epoch_in_ms):
                return False
            self.add_split_if_not_exist(split)
        return True

    def to_dict(self):
        data: list = []
        count = self.get_count()
        for i in range(count):
            split: IEXSplit = self.get_split(i)
            split_dict: dict = split.to_dict()
            data.append(split_dict)
        return data

    def add_splits(self, splits):
        count = splits.get_count()
        for i in range(count):
            split: IEXSplit = splits.get_split(i)
            self.add_split_if_not_exist(split)

    def get_count(self):
        return len(self.splits)

    def get_split(self, index):
        if index >= self.get_count():
            return None
        return self.splits[index]

    def add_split_if_not_exist(self, split: IEXSplit):
        if self.contains_split(split):
            return False
        bisect.insort(self.splits, split)
        return True

    def contains_split(self, split: IEXSplit):
        index = bisect.bisect_left(self.splits, split)
        return index != self.get_count() and self.get_split(index) == split

    def to_parquet(self, save_path: str = "", is_local_disk_directory: bool = True):
        return ParquetUtil.save_dict_to_parquet(self.to_dict(), save_path, is_local_disk_directory)

    def from_parquet(self, parquet_path: str = ""):
        splits_dict = ParquetUtil.load_dict_from_parquet(parquet_path)
        if splits_dict is None:
            return False
        return self.initialize_from_dict(splits_dict)

    def __eq__(self, other):
        if not isinstance(other, IEXSplits):
            return False
        count = self.get_count()
        for i in range(count):
            if self.splits[i] != other.get_split(i):
                return False
        return True
