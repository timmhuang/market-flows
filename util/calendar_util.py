from datetime import date, datetime, timezone
import calendar
import pytz

DEFAULT_HISTORY_START = '1900-01'
DEFAULT_HISTORY_END = '2999-12'


def get_date_today(target_tz: str = None):
    today = datetime.now(timezone.utc)
    if target_tz:
        today = today.astimezone(pytz.timezone(target_tz))
    return today.date()


def get_start_of_next_month(cur_year, cur_month):
    if cur_month == 12:
        return date(cur_year + 1, 1, 1)
    else:
        return date(cur_year, cur_month + 1, 1)


def get_month_range(year, month):
    return calendar.monthrange(year, month)


def current_datetime():
    return datetime.now()


def get_year_month(datetime_str: str):
    return datetime.strptime(datetime_str, '%Y-%m')


def _is_valid_calendar_str(value: str, format: str) -> bool:
    try:
        datetime.strptime(value, format)
        return True
    except ValueError:
        return False


def is_valid_datetime_str(datetime_str: str) -> bool:
    return _is_valid_calendar_str(datetime_str, '%Y-%m-%d')


def is_valid_month_str(month_str: str) -> bool:
    return _is_valid_calendar_str(month_str, '%Y-%m')


class DateTimeRange:
    def __init__(self, start: datetime, end: datetime):
        self._start = start
        self._end = end

    @property
    def start(self):
        return '%4d-%02d' % (self._start.year, self._start.month)

    @start.setter
    def start(self, s: datetime):
        self._start = s

    @property
    def end(self):
        return '%4d-%02d' % (self._end.year, self._end.month)

    @end.setter
    def end(self, e: datetime):
        self._end = e

    def get_all_months_between(self):
        '''
        Get all year and month datetime objects from start to end, including end month
        :param start: start month
        :param end: end month
        :return: [datetime()]
        '''
        months = []

        cur_month = self._start
        while cur_month <= self._end:
            months.append(cur_month)
            if cur_month.month == 12:
                cur_month = datetime(year=cur_month.year + 1, month=1, day=1)
            else:
                cur_month = datetime(year=cur_month.year, month=cur_month.month + 1, day=1)
        return months

    def contains_month_str(self, month: str):
        if not is_valid_month_str(month):
            raise ValueError("Input parameter '%s' is not a valid yyyy-mm date definition" % month)
        return self._start <= datetime.strptime(month, '%Y-%m') <= self._end

    def is_default(self):
        return self._start == get_year_month(DEFAULT_HISTORY_START) and \
            self._end == get_year_month(DEFAULT_HISTORY_END)