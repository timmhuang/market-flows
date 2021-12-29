"""
Class to store split information on a symbol
Created by Kenneth Nursalim on 12/9/2020
"""

import datetime as dt
import dateutil as dateutil

from flows.util.util import IEXUtil 

class IEXSplit:
    def __init__(self, declared_date: dt.date=dt.date.today(), description: str="", execution_date: dt.date=dt.date.today(),\
                from_factor: int=1, refid: int="", symbol: str="", to_factor: int=1, id: str="",\
                key: str="", subkey: str="", updated: dt.datetime=dt.datetime.now()):
        self.declared_date: dt.date = declared_date
        self.description: str = description
        self.execution_date: dt.date = execution_date
        self.from_factor: int = from_factor
        self.refid: int = refid
        self.symbol: str = symbol
        self.to_factor: int = to_factor
        self.id: str = id
        self.key: str = key
        self.subkey: str = subkey
        self.updated: dt.datetime = updated

        self.ratio: float = from_factor / to_factor          # in general, we will compute ratio ourselves from to and from factor since data might be truncated
    
    def initialize_from_dict(self, data_dict: dict = {}, epoch_in_ms=False):
        if  not "declaredDate" in data_dict or\
            not "description" in data_dict or\
            not "exDate" in data_dict or\
            not "fromFactor" in data_dict or\
            not "refid" in data_dict or\
            not "symbol" in data_dict or\
            not "toFactor" in data_dict or\
            not "id" in data_dict or\
            not "key" in data_dict or\
            not "subkey" in data_dict or\
            not "updated" in data_dict:
            return False 

        updated = data_dict['updated']
        if epoch_in_ms:
            updated /= 1e3

        self.__init__(declared_date=IEXUtil.convert_to_date(data_dict['declaredDate']), description=data_dict['description'],\
            execution_date=IEXUtil.convert_to_date(data_dict['exDate']), from_factor=data_dict['fromFactor'], refid=data_dict['refid'],\
            symbol=data_dict['symbol'], to_factor=data_dict['toFactor'], id=data_dict['id'], key=data_dict['key'], subkey=data_dict['subkey'],\
            updated=IEXUtil.convert_to_datetime(updated))
        return True

    def to_dict(self):
        data_dict: dict = {
            "declaredDate"  : self.declared_date.isoformat(),
            "description"   : self.description,
            "exDate"        : self.execution_date.isoformat(),
            "fromFactor"    : self.from_factor,
            "ratio"         : self.ratio,
            "refid"         : self.refid,
            "symbol"        : self.symbol,
            "toFactor"      : self.to_factor,
            "id"            : self.id,
            "key"           : self.key,
            "subkey"        : self.subkey,
            "updated"       : self.updated.timestamp()
        }
        return data_dict

    def __eq__(self, other):
        if not isinstance(other, IEXSplit):
            return False
        return  self.declared_date == other.declared_date and\
                self.description == other.description and\
                self.execution_date == other.execution_date and\
                self.from_factor == other.from_factor and\
                self.refid == other.refid and\
                self.symbol == other.symbol and\
                self.to_factor == other.to_factor and\
                self.id == other.id and\
                self.key == other.key and\
                self.subkey == other.subkey and\
                self.updated == other.updated

    def __lt__(self, other):
        return self.execution_date < other.execution_date
