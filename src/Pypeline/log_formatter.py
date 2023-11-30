from bisect import bisect
from logging import LogRecord, Formatter, DEBUG, ERROR, INFO
from typing import Dict


class LogFormatter(Formatter):
    def __init__(self, formats: Dict[int, str] = None, **kwargs):
        super().__init__()

        if 'fmt' in kwargs:
            raise ValueError(
                'Format string must be passed to level-surrogate formatters, '
                'not this one'
            )

        if formats is None:
            formats = {
                DEBUG: "[%(asctime)s - %(name)s:%(levelname)s - %(filename)s:L%(lineno)s] %(message)s",
                INFO: "[%(asctime)s - %(name)s:%(levelname)s] %(message)s",
                ERROR: "[%(asctime)s - %(name)s:%(levelname)s - %(filename)s:L%(lineno)s] %(message)s",
            }

        self.formats = sorted(
            (level, Formatter(fmt, **kwargs)) for level, fmt in formats.items()
        )

    def format(self, record: LogRecord) -> str:
        idx = bisect(self.formats, (record.levelno,), hi=len(self.formats)-1)
        level, formatter = self.formats[idx]
        return formatter.format(record)
