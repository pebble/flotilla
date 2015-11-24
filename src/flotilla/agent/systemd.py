import logging
import os
from flotilla.model import UNIT_PREFIX

logger = logging.getLogger('flotilla')


class SystemdUnits(object):
    def __init__(self, manager, unit_dir='/etc/systemd/system',
                 env_dir='/etc/flotilla'):
        self._manager = manager
        self._unit_dir = unit_dir
        self._env_dir = env_dir

    def get_units(self):
        units = [unit for unit in self._manager.list_units() if
                 unit.properties.Id.startswith(UNIT_PREFIX)]
        logger.debug('Found %s units.', len(units))
        return units

    def stop_units(self):
        for unit in self.get_units():
            try:
                unit.stop('replace')
            except Exception as e:
                logger.exception(e)

    def start_units(self):
        for unit in self.get_units():
            unit.start('replace')

    def set_units(self, units):
        # Index units by deployed name:
        unit_names = {unit.full_name: unit for unit in units}
        logger.debug('Desired units: %s', unit_names.keys())

        # Remove any services not in the current set:
        for existing_unit in self.get_units():
            name = existing_unit.properties.Id
            active_state = existing_unit.properties.ActiveState
            sub_state = existing_unit.properties.SubState
            logger.debug('Existing unit: %s (%s/%s)', name, active_state,
                         sub_state)
            if name not in unit_names:
                logger.debug('Unit %s is unknown, stopping.' % name)
                try:
                    existing_unit.stop('replace')
                except Exception as e:
                    logger.exception(e)
                unit_path = os.path.join(self._unit_dir, name)
                if os.path.isfile(unit_path):
                    os.unlink(unit_path)
                env_path = os.path.join(self._env_dir, name)
                if os.path.isfile(env_path):
                    os.unlink(env_path)


        # Ensure desired units are loaded and started:
        for name, unit in unit_names.items():
            # Write unit file to disk:
            unit_path = os.path.join(self._unit_dir, name)
            if not os.path.exists(unit_path):
                logger.debug('Writing unit file: %s', unit_path)
                with open(unit_path, 'w') as unit_file:
                    unit_file.write(unit.unit_file)

            # Write environment file to disk:
            if unit.environment:
                env_path = os.path.join(self._env_dir, name)
                if not os.path.exists(env_path):
                    logger.debug('Writing environment file: %s', env_path)
                    # TODO: KMS decrypt
                    with open(env_path, 'w') as env_file:
                        for key, value in unit.environment.items():
                            env_file.write(key)
                            env_file.write('=')
                            env_file.write(value)
                            env_file.write('\n')

            try:
                self._manager.reload()
            except Exception as e:
                logger.exception(e)
            loaded_unit = self._manager.load_unit(name)
            active_state = loaded_unit.properties.ActiveState
            if active_state not in ['active', 'activating']:
                logger.debug('Unit %s is %s, starting...', name, active_state)
                loaded_unit.start('replace')
            else:
                sub_state = loaded_unit.properties.SubState
                logger.debug('Unit %s already started: %s', name, sub_state)

    def get_unit_status(self):
        unit_statuses = {}
        for unit in self.get_units():
            unit_status = {
                'load_state': unit.properties.LoadState,
                'active_state': unit.properties.ActiveState,
                'sub_state': unit.properties.SubState,
                'active_enter_time': unit.properties.ActiveEnterTimestamp,
                'active_exit_time': unit.properties.ActiveExitTimestamp
            }
            unit_statuses[unit.properties.Id] = unit_status
        return unit_statuses
