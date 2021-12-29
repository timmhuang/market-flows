import logging
import os
import pandas as pd
from flows.util.parquet_util import ParquetUtil
from google.cloud import storage
from google.cloud.storage.blob import Blob
from flows.postgres.client import PostgresClient
from flows.pipeline.base import DataLoader
from namespace.constants import WriteMode


class PostgresDataLoader(DataLoader):

    TABLE_SCHEMA = 'table_schema'
    DB_SCHEMA = 'db_schema'
    TABLE = 'table'
    KEYS = 'keys'
    VALUES = 'values'

    def __init__(self, context, client=None):
        super().__init__(context)
        if not client:
            self.postgres_client = PostgresClient(
                host=context[PostgresClient.CFG_HOST],
                database=context[PostgresClient.CFG_DATABASE],
                user=context[PostgresClient.CFG_USER],
                password=context[PostgresClient.CFG_PASSWORD],
            )
        else:
            self.postgres_client = client

    def load(self, df, **kwargs):
        self.postgres_client.upsert_from_df(
            df,
            self.context[PostgresDataLoader.TABLE_SCHEMA],
            self.context[PostgresDataLoader.DB_SCHEMA],
            self.context[PostgresDataLoader.TABLE],
            self.context[PostgresDataLoader.KEYS],
            self.context[PostgresDataLoader.VALUES]
        )


class GStorageDataLoader(DataLoader):
    NAME_BUCKET = 'bucket'
    NAME_FOLDER = 'folder'
    NAME_PARTITION = 'partition'
    WRITE_MODE = 'write_mode'

    def __init__(self, context):
        super().__init__(context)
        self._gs_client = storage.Client()

    def load(self, df: pd.DataFrame, **kwargs):
        path = 'gs://{bucket}/{folder}/{partition}/data.parquet'.format(
            bucket=self.context[GStorageDataLoader.NAME_BUCKET],
            folder=self.context[GStorageDataLoader.NAME_FOLDER],
            partition=self.context[GStorageDataLoader.NAME_PARTITION],
        )
        if Blob.from_string(path).exists(self._gs_client):
            mode = WriteMode.UPSERT
        else:
            mode = WriteMode.OVERWRITE
        return ParquetUtil.write_parquet(df, path, mode=mode)
