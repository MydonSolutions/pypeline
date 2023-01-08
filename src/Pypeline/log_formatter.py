import logging

class LogFormatter(logging.Formatter):

    debug_fmt = "[%(asctime)s - %(name)s:%(levelname)s - %(filename)s:L%(lineno)s] %(message)s"
    common_fmt = "[%(asctime)s - %(name)s:%(levelname)s] %(message)s"

    def __init__(self):
        logging.Formatter.__init__(self, LogFormatter.common_fmt)


    def format(self, record):
        '''
        Temporarily swap out the format for DEBUG and ERROR logs.
        '''

        format_orig = self._fmt
        if record.levelno == logging.DEBUG:
            self._fmt = MyFormatter.debug_fmt
        elif record.levelno == logging.ERROR:
            self._fmt = MyFormatter.debug_fmt

        result = logging.Formatter.format(self, record)
        self._fmt = format_orig

        return result