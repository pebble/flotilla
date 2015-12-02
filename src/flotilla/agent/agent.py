import logging

logger = logging.getLogger('flotilla')


class FlotillaAgent(object):
    def __init__(self, service, db, locks, systemd, elb):
        self._service = service
        self._db = db
        self._locks = locks
        self._systemd = systemd
        self._elb = elb
        self._assignment = None

    def assignment(self):
        """Check for active assignment and update if necessary."""
        assignment = self._db.get_assignment()
        if self._assignment != assignment:
            logger.debug('Updated assignment: %s (was %s)', assignment,
                         self._assignment)
            assigned_units = self._db.get_units()

            deploy_lock = '%s-deploy' % self._service
            if self._locks.try_lock(deploy_lock):
                try:
                    if self._elb:
                        self._elb.unregister()
                    self._systemd.set_units(assigned_units)

                    if not self._elb or self._elb.register():
                        self._assignment = assignment
                finally:
                    self._locks.release_lock(deploy_lock)

    def health(self):
        """Write health to systemd."""
        units_status = self._systemd.get_unit_status()
        self._db.store_status(units_status)
