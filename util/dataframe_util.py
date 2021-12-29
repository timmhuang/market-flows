import pandas as pd
from flows.postgres.schemas import Schema


def rearrange_columns(df: pd.DataFrame, schema: Schema):
    ordered_cols = [k for k in list(schema.columns.keys())]
    return df[ordered_cols]


def parse_dated_dataframe(df: pd.DataFrame):
    df['date'] = pd.to_datetime(df['date'], errors='coerce')  # convert date into datetime
    return df.set_index(['date'])  # reindex on date column
