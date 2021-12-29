class UniversalColumns:
    F_DATE = 'date'
    F_TIME = 'time'
    F_OPEN = 'open'
    F_HIGH = 'high'
    F_LOW = 'low'
    F_CLOSE = 'close'
    F_VOLUME = 'volume'

    COLUMNS = [F_DATE, F_TIME, F_OPEN, F_HIGH, F_LOW, F_CLOSE, F_VOLUME]

    @classmethod
    def get_empty_schema_dict(cls):
        return {col: [] for col in cls.COLUMNS}


class HelperColumns:
    F_TICKER = 'ticker'
    F_TIMEFRAME = 'timeframe'


class DataTypes:
    INTEGER = 'INTEGER'
    BIGINT = 'BIGINT'
    TEXT = 'TEXT'
    FLOAT = 'FLOAT'
    DOUBLE = 'DOUBLE PRECISION'
    TIME = 'TIME'
    DATE = 'DATE'
    TIMESTAMP = 'TIMESTAMP'

    @classmethod
    def VARCHAR(cls, length: int):
        return 'VARCHAR(%d)' % length


class Schema:
    def __init__(self, columns: dict):
        self.columns = columns

    @property
    def sql_string(self):
        columns_str = ['{key} {value}'.format(key=key, value=self.columns[key]) for key in self.columns.keys()]
        return ',\n'.join(columns_str)


class EquityCandleSchema(Schema):
    def __init__(self, columns: dict):
        super().__init__(columns)


SIMPLE_CANDLE_SCHEMA = Schema({
    UniversalColumns.F_DATE: DataTypes.DATE,
    UniversalColumns.F_TIME: DataTypes.TIME,
    UniversalColumns.F_OPEN: DataTypes.DOUBLE,
    UniversalColumns.F_HIGH: DataTypes.DOUBLE,
    UniversalColumns.F_LOW: DataTypes.DOUBLE,
    UniversalColumns.F_CLOSE: DataTypes.DOUBLE,
    UniversalColumns.F_VOLUME: DataTypes.DOUBLE,
    HelperColumns.F_TICKER: DataTypes.VARCHAR(8),
    HelperColumns.F_TIMEFRAME: DataTypes.VARCHAR(16),
})