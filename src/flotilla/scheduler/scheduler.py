import logging
import thread

logger = logging.getLogger('flotilla')


class FlotillaScheduler(object):
    def __init__(self, db, locks, lock_ttl=30):
        self._db = db
        self._locks = locks
        self._lock_ttl = lock_ttl
        self.active = False
        self.__loop = thread.allocate_lock()

    def loop(self):
        if not self.active:
            return

        with self.__loop:
            service_weights = self._db.get_all_revision_weights()
            for service, revisions in service_weights.items():
                logger.debug('Balancing assignments: %s (%s revisions).',
                             service, len(revisions))
                if len(revisions) == 0:
                    continue

                # Get all instances in the service (assigned or not):
                current_assignments = self._db.get_instance_assignments(service)
                if not current_assignments:
                    logger.debug('No instances, can not assign %s.', service)
                    continue
                instance_count = sum(
                    [len(rev_assignments) for rev_assignments in
                     current_assignments.values()])
                logger.debug("Found %s assignable instances.", instance_count)

                # Determine ideal distribution of instances:
                target_counts = self._instance_targets(revisions,
                                                       instance_count)
                logger.debug('Target instance counts: %s', target_counts)

                # Instances without an assignment can be scheduled:
                assignable = current_assignments.get(None, [])
                unassigned = len(assignable)
                logger.debug("Found %s unassigned instances.", unassigned)

                # Remove instances from no longer provisioned revs:
                for rev, assigned_instances in current_assignments.items():
                    if not rev:
                        continue
                    if rev not in target_counts:
                        logger.debug('Unassigning %d instances from %s.',
                                     len(assigned_instances), rev)
                        assignable += assigned_instances

                # Remove instances from over-provisioned revs:
                for rev, instance_count in target_counts.items():
                    current_assignment = current_assignments.get(rev, [])
                    to_unschedule = len(current_assignment) - instance_count
                    if to_unschedule > 0:
                        logger.debug('Unassigning %d instances from %s.',
                                     to_unschedule, rev)
                        assignable += current_assignments[rev][
                                      to_unschedule * -1:]
                logger.debug('Found %s assignable instances (%s unassigned).',
                             len(assignable), unassigned)

                # Add instances to under-provisioned revs:
                reassigned = []
                for rev, instance_count in target_counts.items():
                    current_assignment = current_assignments[rev]
                    to_schedule = instance_count - len(current_assignment)
                    if to_schedule > 0:
                        logger.debug('Scheduling %d instances to %s.',
                                     to_schedule, rev)
                        scheduled = assignable[:to_schedule]
                        for assignment_item in scheduled:
                            assignment_item['assignment'] = rev
                            reassigned.append(assignment_item)
                        assignable = assignable[to_schedule:]

                # Store assignment updates:
                if reassigned and self.active:
                    logger.debug('Storing %d reassignments.', len(reassigned))
                    self._db.set_assignments(reassigned)

    def lock(self):
        if self._locks.try_lock('scheduler', ttl=self._lock_ttl, refresh=True):
            if not self.active:
                logger.info('Became active scheduler')
                self.active = True
                self.loop()
        elif self.active:
            logger.info('No longer active scheduler')
            self.active = False

    @staticmethod
    def _instance_targets(revisions, instance_count):
        total_weight = sum(revisions.values())
        instance_targets = {}
        for rev, weight in revisions.items():
            rev_pct = float(weight) / total_weight
            instance_targets[rev] = int(round(rev_pct * instance_count))

        # Detect rounding error, trim from the largest group:
        while sum(instance_targets.values()) != instance_count:
            max_value = max(instance_targets.values())
            for rev, value in instance_targets.items():
                if value == max_value:
                    instance_targets[rev] -= 1
                    break
        return instance_targets
