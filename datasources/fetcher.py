"""
Base class for fetcher

Created on 12/9/2020 by Reno Septa Pradana
"""

import json
import logging
import os
import pandas as pd

from abc import abstractmethod

from flows.util.parquet_util import ParquetUtil
from namespace.constants import WriteMode
from flows.datasources.iex import IEXClient
from google.cloud.storage.blob import Blob
from google.cloud import storage
from flows.util.dataframe_util import parse_dated_dataframe
from pathlib import Path

logging.basicConfig(
    format='%(asctime)-15s [%(name)s] %(message)s',
    level=logging.INFO
)


class ETLFetcher:

    DEFAULT_FILENAME = 'data.parquet'

    def __init__(self):
        self._root_dir = None
        self._root_dir_is_gs = None
        self._gs_client = None
        self._iex_client = None

    def initialize(self, root_dir, root_dir_is_gs=False, iex_token=None, *args, **kwargs):
        self._root_dir = root_dir
        self._root_dir_is_gs = root_dir_is_gs
        if self._root_dir_is_gs:
            self._gs_client = storage.Client()
        self._iex_client = IEXClient(token_value=iex_token)
        return True

    def finalize(self):
        self._root_dir = None
        self._root_dir_is_gs = None
        self._gs_client = None
        self._iex_client.session.close()
        self._iex_client = None
        logging = None
        return True

    @abstractmethod
    def start(self):
        pass

    def _save_response_to_storage(self, symbol: str, response: str):
        try:
            response = json.loads(response)
        except ValueError:
            logging.error('failed to loads json')
            return False
        year = self._get_year(response)
        data_frame = parse_dated_dataframe(pd.DataFrame.from_dict([response]))
        save_path = self._create_save_path(symbol, year)
        save_mode = WriteMode.UPSERT if self._is_file_exist(save_path) else WriteMode.OVERWRITE
        return ParquetUtil.write_parquet(data_frame, save_path, save_mode)

    def _create_save_path(self, symbol: str, year: str):
        path = '{}/{}/{}'.format(self._root_dir, symbol, year)
        if not self._root_dir_is_gs:
            path = Path(path)
            path.mkdir(parents=True, exist_ok=True)
            path = str(path.absolute())
        return '/'.join([path, self.DEFAULT_FILENAME])

    def _get_year(self, response):
        if not isinstance(response, dict):
            raise RuntimeError('Response is not dictionary')
        return response.get('date').split('-')[0]

    def _is_file_exist(self, file_path):
        if self._root_dir_is_gs:
            return Blob.from_string(file_path).exists(self._gs_client)
        return os.path.exists(file_path)
