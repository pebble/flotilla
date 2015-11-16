import logging

logger = logging.getLogger('flotilla')


class FlotillaAgent(object):
    def __init__(self, service, db, scheduler, systemd, elb):
        self._service = service
        self._db = db
        self._scheduler = scheduler
        self._systemd = systemd
        self._elb = elb
        self._assignment = None

    def assignment_heartbeat(self):
        """Heartbeat as available for assignments."""
        self._db.heartbeat(self._service)

    def assignment(self):
        """Check for active assignment and update if necessary."""
        assignment = self._db.get_assignment(self._service)
        if self._assignment != assignment:
            logger.debug('Updated assignment: %s (was %s)', assignment,
                         self._assignment)
            assigned_units = self._db.get_units(self._service)

            lock_name = '%s-deploy' % self._service
            if self._db.try_lock(lock_name):
                self._elb.unregister()
                self._systemd.set_units(assigned_units)
                if self._elb.register():
                    self._db.release_lock(lock_name)
                    self._assignment = assignment

    def scheduler_election(self):
        """Verify scheduler is alive, takeover if necessary."""
        scheduler_ttl = 45
        scheduler_active = self._db.try_lock('scheduler', ttl=scheduler_ttl)
        if scheduler_active and not self._scheduler.active:
            logger.debug('We are now the active scheduler')
            self._scheduler.active = True
            return True
            self._election.interval = self.leader_interval / 2
        elif not scheduler_active and self._scheduler.active:
            logger.warn('We are no longer the active scheduler')
            self._scheduler.active = False
            return False

    def scheduler_loop(self):
        """Update assignments (no-op unless active)."""
        self._scheduler.loop()

    def health(self):
        """Write health to systemd."""
        # TODO: query systemd:
        units_status = self._systemd.get_unit_status()

        # TODO: query machine metrics? (are they useful for scheduling?)
        self._db.store_status(units_status)
