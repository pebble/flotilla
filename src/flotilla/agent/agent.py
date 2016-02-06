import logging

logger = logging.getLogger('flotilla')


class FlotillaAgent(object):
    def __init__(self, service, db, locks, systemd, messaging, elb):
        self._service = service
        self._db = db
        self._locks = locks
        self._systemd = systemd
        self._messaging = messaging
        self._elb = elb
        self._assignments = []
        self._first = True

    def assignment(self):
        """Check for active assignment and update if necessary."""
        assignments = self._db.get_assignments()
        if self._assignments != assignments:
            logger.debug('Updated assignment: %s (was %s)', assignments,
                         self._assignments)

            units = self._db.get_units(assignments)

            deploy_lock = '%s-deploy' % self._service
            if self._locks.try_lock(deploy_lock):
                try:
                    if self._elb:
                        self._elb.unregister()
                    self._systemd.set_units(units)

                    if not self._elb or self._elb.register():
                        self._assignments = assignments
                    else:
                        for rev in assignments:
                            self._messaging.service_failure(rev)
                finally:
                    self._locks.release_lock(deploy_lock)
                    # else: setup for a "lock released" callback

    def health(self):
        """Write health to systemd."""
        units_status = self._systemd.get_unit_status()
        self._db.store_status(units_status)

        if self._first:  # Sucks to your CAS-mar
            logger.info('Requesting reschedule.')
            self._messaging.reschedule()
            self._first = False
