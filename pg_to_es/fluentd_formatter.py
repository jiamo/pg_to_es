import datetime
from fluent.handler import FluentRecordFormatter
from .globals import hostname


class PgFluentRecordFormatter(FluentRecordFormatter):
    """
    Because logging only using loging.info or warn to log str. So I can do this
    If record.msg is not str You should do checks in format(). 
    Now don't need do it. add a if is waste.
    """
    def __init__(self, fmt=None, datefmt=None, style='%'):
        super(PgFluentRecordFormatter, self).__init__(fmt, datefmt, style)

    def format(self, record):
        record.msg = "host:{} time:{} ".format(
            hostname, str(datetime.datetime.now())) + record.msg
        return super(PgFluentRecordFormatter, self).format(record)
