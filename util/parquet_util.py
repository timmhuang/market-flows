"""
Util class to simplify saving and reading from parquets
Created by Kenneth Nursalim on 12/15/2020
"""
import logging
from pathlib import Path
import pandas as pd
from namespace.constants import WriteMode


class ParquetUtil:
    @staticmethod
    def save_dict_to_parquet(data_dict: dict, save_path: str, save_to_local_folder: bool = True, *args, **kwargs):
        df = pd.DataFrame(data_dict, *args, **kwargs)
        engine = 'pyarrow'
        if 'engine' in kwargs:
            engine = kwargs['engine']

        if save_to_local_folder:
            save_path = Path(save_path)
            save_path.parent.mkdir(parents=True, exist_ok=True)
            save_path = save_path.absolute()
        df.to_parquet(save_path, engine=engine, compression='gzip')
        return True

    @staticmethod
    def load_dict_from_parquet(parquet_path: str, *args, **kwargs):
        try:
            df = ParquetUtil.read_parquet(parquet_path, *args, **kwargs)
            return df.to_dict('records')
        except FileNotFoundError:
            return None

    @staticmethod
    def read_parquet(parquet_path: str, engine='pyarrow', *args, **kwargs):
        return pd.read_parquet(parquet_path, engine=engine, *args, **kwargs)

    @staticmethod
    def write_parquet(data_frame, save_path, mode: WriteMode = WriteMode.OVERWRITE, engine='pyarrow', index=None):
        message = 'writing new file to {} in {} mode using {}'
        logging.info(message.format(save_path, str(mode), engine))
        if mode == WriteMode.OVERWRITE:
            data_frame.to_parquet(
                save_path,
                index=index,
                engine=engine,
                compression='gzip'
            )
        elif mode == WriteMode.APPEND:
            ParquetUtil.append_file(data_frame, save_path, engine, index)
        else:
            ParquetUtil.upsert_file(data_frame, save_path, engine, index)
        return True

    @staticmethod
    def append_file(data_frame, save_path, engine, index):
        '''
        Append new dataframe to existing dataframe at save_path
        :param data_frame: new data
        :param save_path: file location
        :param engine: parquet engine (pyarrow or fastparquet)
        :param index: pandas DF index flag
        :return:
        '''
        existing_df = pd.read_parquet(save_path, engine=engine)
        output_df = pd.concat([existing_df, data_frame])
        output_df.to_parquet(save_path, engine=engine, compression='gzip', index=index)

    @staticmethod
    def upsert_file(data_frame, save_path, engine, index):
        '''
        Load existing data at save_path, concatenate with new data, and drop all duplicates
        before writing back to the same location
        :param data_frame: new data
        :param save_path: file location
        :param engine: parquet engine (pyarrow or fastparquet)
        :param index: pandas DF index flag
        '''
        existing_df = pd.read_parquet(save_path, engine=engine)
        output_df = pd.concat([existing_df, data_frame]).drop_duplicates()
        output_df = output_df.loc[~output_df.index.duplicated(keep='last')]
        output_df.to_parquet(save_path, engine=engine, compression='gzip', index=index)
