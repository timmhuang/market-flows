import logging
import os
import psycopg2
import pandas as pd
from functools import wraps
from flows.postgres.schemas import Schema
from flows.postgres.queries import *
from flows.util.dataframe_util import rearrange_columns
from flows.util.string_iterator_io import StringIteratorIO


def get_postgres_destination(schema, table):
    return '%s.%s' % (schema, table)


def get_data_stream(data):
    gen = (row + '\n' for row in data.split('\n'))
    return StringIteratorIO(gen)


def transaction(method):
    @wraps(method)
    def _impl(self, *args, **kwargs):
        method(self, *args, **kwargs)
        self.conn.commit()
        self.logger.info("Transaction successfully completed!")
    return _impl


class PostgresClient:
    CFG_HOST = 'db_host'
    CFG_DATABASE = 'db_name'
    CFG_USER = 'db_user'
    CFG_PASSWORD = 'db_password'

    def __init__(self, host, database, user, password):
        self.logger = logging.getLogger('{}@{}'.format(user, host))
        self.logger.setLevel(logging.INFO)
        self._dsn = "host=%s dbname=%s user=%s password=%s" % (host, database, user, password)
        self.conn = psycopg2.connect(self._dsn)
        self.cursor = self.conn.cursor()

    def reconnect(self):
        if not self.conn.closed:
            return False
        self.conn = psycopg2.connect(self._dsn)
        self.cursor = self.conn.cursor()
        return True

    def _run_query(self, query):
        self.logger.info("Executing query: \n%s" % query)
        self.cursor.execute(query)

    def query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def create_staging_table(self, schema: Schema, table_name):
        staging_table_name = table_name + '_staging'
        query = create_temp_table_query(schema, staging_table_name)
        self.cursor.execute(query)
        return staging_table_name

    def create_table_if_not_exists(self, table_schema: Schema, db_schema: str, table_name):
        query = create_table_if_not_exists_query(table_schema, get_postgres_destination(db_schema, table_name))
        self._run_query(query)
        self.logger.info("Query successful")

    @transaction
    def truncate_load(self, df: pd.DataFrame, schema, table):
        self.logger.info("Initialzing truncate-load query for {}.{}".format(schema, table))
        query = truncate_query(schema, table)
        self._run_query(query)
        self.bulk_load_from_df(df, schema, table)

    @transaction
    def bulk_load_from_file(self, data, schema, table):
        self.cursor.copy_from(data, get_postgres_destination(schema, table), sep=',')

    def bulk_load_from_df(self, df: pd.DataFrame, schema, table, with_index=False):
        data = df.to_csv(index=with_index, header=False)[:-1]  # remove last newline character
        size = len(df.index)
        self.logger.info("Initializing bulk load {} rows to {}.{}".format(size, schema, table))
        self.bulk_load_from_string(data, schema, table)

    def bulk_load_from_string(self, data, schema, table):
        '''
        Load a CSV string into Postgres DB
        :param data: CSV string
        :param schema: target schema
        :param table: target table
        :return:
        '''
        data_iterator = get_data_stream(data)
        self.cursor.copy_expert("COPY {}.{} FROM STDIN WITH (FORMAT CSV)".format(schema, table), data_iterator)
        self.conn.commit()
        data_iterator.close()

    @transaction
    def upsert_from_df(self,
                       df: pd.DataFrame,
                       table_schema: Schema,
                       db_schema,
                       table,
                       keys,
                       values,
                       with_index=False):
        '''
        Upsert Pandas DF into target table.
        :param df:
        :param table_schema: Schema object
        :param db_schema: Database schema name
        :param table: Database table name
        :param keys: Upsert Key column names list
        :param values: Upsert value names list
        :param with_index: Flag for using Pandas DF index or not
        :return:
        '''
        df = rearrange_columns(df, table_schema)
        data = df.to_csv(index=with_index, header=False)[:-1]  # remove last newline character
        size = len(df.index)
        self.logger.info("Initializing bulk upsert {} rows to {}.{}".format(size, db_schema, table))
        self.upsert_from_string(data, table_schema, db_schema, table, keys, values)

    def upsert_from_string(self,
                           data: str,
                           table_schema: Schema,
                           db_schema,
                           table,
                           keys,
                           values):
        data_iterator = get_data_stream(data)
        target_table_name = get_postgres_destination(db_schema, table)
        staging_table_name = self.create_staging_table(table_schema, table)
        query = upsert_query(
            staging_table_name,
            target_table_name,
            selector_fields=keys,
            setter_fields=values)
        self.logger.info("Populating staging table: %s ..." % staging_table_name)
        self.cursor.copy_from(data_iterator, staging_table_name, sep=',')
        self.logger.info("Executing upsert query: %s ...: \n%s" % (staging_table_name, query))
        self.cursor.execute(query)
        self.cursor.execute('DROP TABLE %s' % staging_table_name)
        self.conn.commit()
        self.logger.info("Upsert successful!")

        data_iterator.close()
