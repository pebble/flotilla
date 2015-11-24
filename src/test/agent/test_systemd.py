import unittest
from mock import MagicMock
import os
import shutil
import tempfile
import time
from flotilla.model import FlotillaDockerService, UNIT_PREFIX
from flotilla.agent.systemd import SystemdUnits

UNIT_NAME = '%smock.service' % UNIT_PREFIX
ACTIVE_STATE = 'inactive'
SUB_STATE = 'running'
LOAD_STATE = 'loaded'
ACTIVE_TIMESTAMP = time.time()


class TestSystemdUnits(unittest.TestCase):
    def setUp(self):
        self.unit = MagicMock()
        self.unit.properties.Id = UNIT_NAME
        self.unit.properties.LoadState = LOAD_STATE
        self.unit.properties.ActiveState = ACTIVE_STATE
        self.unit.properties.SubState = SUB_STATE
        self.unit.properties.ActiveEnterTimestamp = ACTIVE_TIMESTAMP
        self.unit.properties.ActiveExitTimestamp = ACTIVE_TIMESTAMP

        self.loaded_unit = MagicMock()
        self.loaded_unit.properties.ActiveState = ACTIVE_STATE

        self.manager = MagicMock()
        self.manager.list_units.return_value = [self.unit]
        self.manager.load_unit.return_value = self.loaded_unit

        self.flotilla_unit = FlotillaDockerService('redis.service',
                                                   'redis:latest')



        self.unit_dir = tempfile.mkdtemp('flotilla-systemd-unit')
        self.env_dir = tempfile.mkdtemp('flotilla-systemd-env')
        self.systemd = SystemdUnits(self.manager, unit_dir=self.unit_dir,
                                    env_dir=self.env_dir)

    def tearDown(self):
        shutil.rmtree(self.unit_dir)
        shutil.rmtree(self.env_dir)

    def test_get_units_empty(self):
        self.manager.list_units.return_value = []
        units = self.systemd.get_units()
        self.assertEqual(len(units), 0)

    def test_get_units_ignore(self):
        self.unit.properties.Id = 'not-the-druid-you-are-looking-for'
        units = self.systemd.get_units()
        self.assertEqual(len(units), 0)

    def test_get_units(self):
        self.manager.list_units.return_value = [self.unit]

        units = self.systemd.get_units()
        self.assertEqual(len(units), 1)

    def test_stop_units(self):
        self.systemd.stop_units()
        self.unit.stop.assert_called_with('replace')

    def test_stop_units_eats_exception(self):
        self.unit.stop.side_effect = Exception('kaboom')
        self.systemd.stop_units()

    def test_start_units(self):
        self.systemd.start_units()
        self.unit.start.assert_called_with('replace')

    def test_get_unit_status(self):
        statuses = self.systemd.get_unit_status()
        self.assertEqual(len(statuses), 1)
        status = statuses[self.unit.properties.Id]
        self.assertEqual(LOAD_STATE, status['load_state'])
        self.assertEqual(ACTIVE_STATE, status['active_state'])
        self.assertEqual(SUB_STATE, status['sub_state'])
        self.assertEqual(ACTIVE_TIMESTAMP, status['active_enter_time'])
        self.assertEqual(ACTIVE_TIMESTAMP, status['active_exit_time'])

    def test_set_units(self):
        unit_path = '%s/%s' % (self.unit_dir, UNIT_NAME)
        with open(unit_path, 'w') as unit_out:
            unit_out.write('\n')
        env_path = '%s/%s' % (self.env_dir, UNIT_NAME)
        with open(env_path, 'w') as env_out:
            env_out.write('\n')

        self.systemd.set_units([self.flotilla_unit])

        # Existing units were stopped and deleted:
        self.unit.stop.assert_called_with('replace')
        self.assertFalse(os.path.exists(unit_path))
        self.assertFalse(os.path.exists(env_path))

        # New unit has been written:
        unit_path = '%s/%s' % (self.unit_dir, self.flotilla_unit.full_name)
        env_path = '%s/%s' % (self.env_dir, self.flotilla_unit.full_name)
        self.assertTrue(os.path.isfile(unit_path))
        self.assertFalse(os.path.exists(env_path))

        self.manager.reload.assert_called_with()
        self.loaded_unit.start.assert_called_with('replace')

    def test_set_units_environment(self):
        flotilla_unit = FlotillaDockerService('redis.service', 'redis:latest',
                                              environment={'FOO': 'bar'})

        self.systemd.set_units([flotilla_unit])

        env_path = '%s/%s' % (self.env_dir, flotilla_unit.full_name)
        self.assertTrue(os.path.isfile(env_path))

    def test_set_units_existing_wont_stop(self):
        self.unit.stop.side_effect = Exception('Mock stop failure')

        self.systemd.set_units([self.flotilla_unit])
        # Exception not thrown

    def test_set_units_wont_reload(self):
        self.manager.reload.side_effect = Exception('Mock reload failure')

        self.systemd.set_units([self.flotilla_unit])
        # Exception not thrown

    def test_set_units_already_started(self):
        self.loaded_unit.properties.ActiveState = 'active'

        self.systemd.set_units([self.flotilla_unit])

        self.loaded_unit.start.assert_not_called()
