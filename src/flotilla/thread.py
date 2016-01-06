import logging
import threading
import time

logger = logging.getLogger('flotilla')


class RepeatingFunc(threading.Thread):
    """Executes a function with fixed frequency."""

    def __init__(self, name, func, interval):
        super(RepeatingFunc, self).__init__()
        self.name = name
        self._func = func
        self._interval = interval
        self._live = True

    def run(self):
        while self._live:
            start = time.time()
            try:
                self._func()
            except Exception as e:
                logger.exception(e)
            duration = time.time() - start
            interval = self._interval
            if duration > interval:
                logger.warn('Function took %f, this is too slow for %f.',
                            duration, interval)
            else:
                sleep_time = interval - duration
                logger.debug(
                        'Function took %f, sleeping for %f to maintain %f.',
                        duration, sleep_time, interval)
                time.sleep(sleep_time)
        logger.debug('Thread done.')

    def stop(self):
        self._live = False
