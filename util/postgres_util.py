import os
from flows.postgres.client import PostgresClient

postgres_client = PostgresClient(
    os.environ.get('POSTGRES_HOST', ''),
    os.environ.get('POSTGRES_DATABASE', ''),
    os.environ.get('POSTGRES_USERNAME', ''),
    os.environ.get('POSTGRES_PASSWORD', ''),
)

SCHEMA = os.environ.get('POSTGRES_SCHEMA', '')
SYMBOLS_TABLE_NAME = os.environ.get('POSTGRES_SYMBOLS_TABLE_NAME', '')

query = """
        SELECT symbol FROM {}.{}
        """.format(SCHEMA, SYMBOLS_TABLE_NAME)


def get_symbols_from_db():
    if postgres_client.conn.closed:
        postgres_client.reconnect()
    result = postgres_client.query(query)
    return [record[0] for record in result]

