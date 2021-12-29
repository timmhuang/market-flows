"""
Class to wrap 1 dividend struct and its related util
Created by Kenneth Nursalim on 12/15/2020
"""

import datetime as dt

from flows.util.util import IEXUtil
from flows.util.parquet_util import ParquetUtil

class IEXDividend:
    def __init__(self, amount: float = 0.0, currency: str = "", declared_date: dt.date = dt.date.today(),\
                description: str = "", execution_date: dt.date = dt.date.today(), flag: str = "", frequency: str = "",\
                payment_date: dt.date = dt.date.today(), record_date: dt.date = dt.date.today(), refid : int = 0,\
                symbol: str = "", id : str = "", key: str = "", subkey: str = "", date: dt.datetime = dt.datetime.today(), updated: dt.datetime = dt.datetime.today()):
        self.amount: float = amount
        self.currency: str = currency
        self.declared_date: dt.date = declared_date
        self.description: str = description
        self.execution_date: dt.date = execution_date
        self.flag: str = flag
        self.frequency: str = frequency
        self.payment_date: dt.date = payment_date
        self.record_date: dt.date = record_date
        self.refid: int = refid
        self.symbol: str = symbol
        self.id: str = id
        self.key: str = key
        self.subkey: str = subkey
        self.date: dt.datetime = date
        self.updated: dt.datetime = updated

    def initialize_from_dict(self, data_dict: dict, epoch_in_ms=False):
        if  not "amount" in data_dict or\
            not "currency" in data_dict or\
            not "declaredDate" in data_dict or\
            not "description" in data_dict or\
            not "exDate" in data_dict or\
            not "flag" in data_dict or\
            not "frequency" in data_dict or\
            not "paymentDate" in data_dict or\
            not "recordDate" in data_dict or\
            not "refid" in data_dict or\
            not "symbol" in data_dict or\
            not "id" in data_dict or\
            not "key" in data_dict or\
            not "subkey" in data_dict or\
            not "date" in data_dict or\
            not "updated" in data_dict:
            return False
        
        if epoch_in_ms:
            data_dict['date'] /= 1e3
            data_dict['updated'] /= 1e3

        self.__init__(amount=data_dict['amount'], currency=data_dict['currency'], declared_date=IEXUtil.convert_to_date(data_dict['declaredDate']),\
                      description=data_dict['description'], execution_date=IEXUtil.convert_to_date(data_dict['exDate']), flag=data_dict['flag'],\
                      frequency=data_dict['frequency'], payment_date=IEXUtil.convert_to_date(data_dict['paymentDate']), record_date=IEXUtil.convert_to_date(data_dict['recordDate']),\
                      refid=data_dict['refid'], symbol=data_dict['symbol'], id=data_dict['id'], key=data_dict['key'],\
                      subkey=data_dict['subkey'], date=IEXUtil.convert_to_datetime(data_dict['date']), updated=IEXUtil.convert_to_datetime(data_dict['updated']))
        return True

    def to_dict(self) -> dict:
        data = {
            "amount"                : self.amount,
            "currency"              : self.currency,
            "declaredDate"          : self.declared_date.isoformat(),
            "description"           : self.description,
            "exDate"                : self.execution_date.isoformat(),
            "flag"                  : self.flag,
            "frequency"             : self.frequency,
            "paymentDate"           : self.payment_date.isoformat(),
            "recordDate"            : self.record_date.isoformat(),
            "refid"                 : self.refid,
            "symbol"                : self.symbol,
            "id"                    : self.id,
            "key"                   : self.key,
            "subkey"                : self.subkey,
            "date"                  : self.date.timestamp(),
            "updated"               : self.updated.timestamp(),
        }
        return data

    def from_parquet(self, parquet_path: str):
        data_dict = ParquetUtil.load_dict_from_parquet(parquet_path)[0]
        return self.initialize_from_dict(data_dict)

    def to_parquet(self, save_path, save_to_local_folder: bool = True):
        return ParquetUtil.save_dict_to_parquet(self.to_dict(), save_path, save_to_local_folder, index=[0])

    def get_unique_key(self):
        return "{}.{}".format(self.key, self.subkey)

    def __eq__(self, other):
        if not isinstance(other, IEXDividend):
            return False
        return  self.amount == other.amount and\
                self.currency == other.currency and\
                self.declared_date == other.declared_date and\
                self.description == other.description and\
                self.execution_date == other.execution_date and\
                self.flag == other.flag and\
                self.frequency == other.frequency and\
                self.payment_date == other.payment_date and\
                self.record_date == other.record_date and\
                self.refid == other.refid and\
                self.symbol == other.symbol and\
                self.id == other.id and\
                self.key == other.key and\
                self.subkey == other.subkey and\
                self.date == other.date and\
                self.updated == other.updated

    def __lt__(self, other):
        return self.date < other.date
