import os
from abc import abstractmethod
import pandas as pd


class TickerProvider:
    @abstractmethod
    def get_tickers(self) -> list:
        pass


class DataProvider:
    @abstractmethod
    def get_dataframe(self, **kwargs) -> pd.DataFrame:
        pass


class DataTransformer:
    @abstractmethod
    def transform(self, df, **kwargs) -> pd.DataFrame:
        pass


class DataLoader:
    def __init__(self, context):
        self.context = context

    @abstractmethod
    def load(self, df, **kwargs) -> None:
        pass
