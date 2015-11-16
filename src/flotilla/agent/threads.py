import time
import logging
import threading

logger = logging.getLogger('flotilla')


class RepeatingFunc(threading.Thread):
    """Executes a function with fixed frequency."""

    def __init__(self, name, func, interval):
        super(RepeatingFunc, self).__init__()
        self.name = name
        self._func = func
        if not hasattr(interval, '__call__'):
            self._interval = lambda: interval
        else:
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
            interval = self._interval()
            if duration > interval:
                logger.warn('Function took %f, this is too slow for %f.',
                            duration, interval)
            else:
                sleep_time = interval - duration
                logger.debug(
                    'Function took %f, sleeping for %f to maintain %f.',
                    duration, sleep_time, interval)
                time.sleep(sleep_time)

    def stop(self):
        self._live = False


class FlotillaThreads(object):
    """Coordinates threads."""

    def __init__(self, agent, scheduler):
        self._funcs = [
            RepeatingFunc('AssignHeartbeat', agent.assignment_heartbeat, 5),
            RepeatingFunc('AssignLoop', agent.assignment, 15),
            RepeatingFunc('StatusLoop', agent.health, 5),

            RepeatingFunc('SchedulerElection', agent.scheduler_election,
                          lambda: scheduler.active and 15 or 30),
            RepeatingFunc('SchedulerLoop', agent.scheduler_loop,
                          lambda: scheduler.active and 5 or 10),
        ]

    def run(self):
        map(RepeatingFunc.start, self._funcs)

    def stop(self):
        map(RepeatingFunc.stop, self._funcs)
