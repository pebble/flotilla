import unittest
from mock import MagicMock
import time
from flotilla.thread import RepeatingFunc


class TestRepeatingFunc(unittest.TestCase):
    def test_run_on_time(self):
        instant_function = MagicMock()
        self.run_loop(instant_function)
        assert instant_function.call_count > 16
        assert instant_function.call_count <= 20

    def test_run_eat_exception(self):
        instant_function = MagicMock()
        instant_function.side_effect = Exception('kaboom')

        self.run_loop(instant_function)

    def test_run_too_slow(self):
        self.count = 0

        def slow_function():
            self.count += 1
            time.sleep(0.1)

        f = RepeatingFunc('test', slow_function, 0.01)
        f.start()
        time.sleep(0.2)
        f.stop()
        f.join()

        assert self.count > 1
        assert self.count < 3

    def test_run_no_interval(self):
        instant_function = MagicMock()
        f = RepeatingFunc('test', instant_function, 0)
        f.start()
        time.sleep(0.01)
        f.stop()
        f.join()
        assert instant_function.call_count > 10

    def run_loop(self, instant_function):
        f = RepeatingFunc('test', instant_function, 0.01)
        f.start()
        time.sleep(0.2)
        f.stop()
        f.join()
