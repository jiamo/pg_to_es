import logging
from fluent.handler import FluentHandler


class InfoFilter(logging.Filter):

    def filter(self, rec):
        return rec.levelno <= logging.INFO


class ErrorFilter(logging.Filter):

    def filter(self, rec):
        return rec.levelno > logging.INFO


class InfoHandler(FluentHandler):
    def __init__(self, *args, **kwargs):
        FluentHandler.__init__(self, *args, **kwargs)
        self.addFilter(InfoFilter())


class ErrorHandler(FluentHandler):
    def __init__(self, *args, **kwargs):
        FluentHandler.__init__(self, *args, **kwargs)
        self.addFilter(ErrorFilter())