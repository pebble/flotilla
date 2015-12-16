import logging
from boto.dynamodb2.items import Item
from collections import defaultdict

logger = logging.getLogger('flotilla')


class FlotillaSchedulerDynamo(object):
    def __init__(self, assignments, regions, services, stacks, status):
        self._assignments = assignments
        self._regions = regions
        self._services = services
        self._stacks = stacks
        self._status = status

        # TODO: shard scan for multiple schedulers
        self._segments = 1
        self._segment = 0

    def get_revision_weights(self):
        """Load services, revisions and weights"""
        services = {}
        rev_count = 0
        for service in self.services():
            service = dict(service)
            name = service['service_name']
            del service['service_name']
            service.pop('regions', None)

            service_revs = {k: int(v) for k, v in service.items()}
            services[name] = service_revs
            rev_count += len(service_revs)

        logger.debug('Loaded %s services, %s revisions', len(services),
                     rev_count)
        return services

    def services(self):
        for service in self._services.scan(segment=self._segment,
                                           total_segments=self._segments):
            yield service

    def get_stacks(self):
        return [s for s in self._stacks.scan()]

    def set_stacks(self, stacks):
        with self._stacks.batch_write() as batch:
            for stack in stacks:
                batch.put_item(stack)

    def set_assignment(self, service, machine, assignment):
        self._assignments.put_item(data={
            'service_name': service,
            'instance_id': machine,
            'assignment': assignment
        }, overwrite=True)

    def set_assignments(self, assignments):
        """Store assignments in a batch.
        :param assignments: Assignments to store.
        """
        with self._assignments.batch_write() as batch:
            for assignment in assignments:
                batch.put_item(assignment)

    def get_instance_assignments(self, service):
        """Get instances and assignments for a service
        :param service:  Service name.
        :return: Map of instances of assignments (None if unassigned).
        """
        instances = [i['instance_id'] for i in
                     self._status.query_2(service__eq=service,
                                          attributes=('instance_id',))]

        assignments = defaultdict(list)
        if not instances:
            return assignments

        unassigned = set(instances)
        for assignment in self._assignments.batch_get(
                keys=[{'instance_id': i} for i in instances],
                attributes=('instance_id', 'assignment')):
            assigned = assignment['assignment']
            instance_id = assignment['instance_id']
            unassigned.remove(instance_id)
            assignments[assigned].append(assignment)

        assignments[None] = [Item(self._assignments, data={
            'instance_id': instance_id,
            'service': service
        }) for instance_id in unassigned]

        return assignments

    def get_region_params(self, regions):
        keys = [{'region_name': region} for region in regions]
        region_params = {region: {} for region in regions}
        for item in self._regions.batch_get(keys):
            region_params[item['region_name']] = dict(item)
        return region_params
