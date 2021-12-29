"""
General Util file
Created by Kenneth Nursalim on 12/15/2020
"""

import datetime as dt
import dateutil.parser
import pandas as pd

class IEXUtil:
    @staticmethod
    def convert_to_date(data) -> dt.date:
        return IEXUtil.convert_to_datetime(data).date()

    @staticmethod
    def convert_to_datetime(data) -> dt.datetime:
        if isinstance(data, str):
            datetime = dateutil.parser.parse(data)
        elif isinstance(data, int) or isinstance(data, float):
            datetime = dt.datetime.fromtimestamp(data, tz=dt.timezone.utc)
        elif isinstance(data, pd.Timestamp):
            datetime = pd.to_datetime(data)
        elif data is None:
            datetime = datetime = dt.datetime.fromtimestamp(0, tz=dt.timezone.utc)  # Epoch 0
        else:
            raise Exception("Unknown datetime format")
        return datetime
