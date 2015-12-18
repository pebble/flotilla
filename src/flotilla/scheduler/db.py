import logging
import time
import boto.vpc
from boto.exception import EC2ResponseError
from boto.dynamodb2.items import Item
from collections import defaultdict

logger = logging.getLogger('flotilla')

INSTANCE_EXPIRY = 300


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
            name = service['service_name']

            service_revs = {k: int(v) for k, v in service.items()
                            if len(k) == 64}
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
        live_instances = []
        dead_instances = []
        dead_cutoff = time.time() - INSTANCE_EXPIRY
        for instance_status in self._status.query_2(service__eq=service,
                                                    attributes=('instance_id',
                                                                'status_time')):
            instance_id = instance_status['instance_id']
            if instance_status['status_time'] < dead_cutoff:
                dead_instances.append(instance_id)
            else:
                live_instances.append(instance_id)

        if dead_instances:
            logger.debug('Removing %d dead instances.', len(dead_instances))
            with self._status.batch_write() as status_batch:
                for dead_instance in dead_instances:
                    status_batch.delete_item(service=service,
                                             instance_id=dead_instance)
            with self._assignments.batch_write() as assignment_batch:
                for dead_instance in dead_instances:
                    assignment_batch.delete_item(instance_id=dead_instance)

        assignments = defaultdict(list)
        if not live_instances:
            return assignments

        unassigned = set(live_instances)
        keys = [{'instance_id': i} for i in live_instances]
        for assignment in self._assignments.batch_get(keys=keys, attributes=(
                'instance_id', 'assignment')):
            assigned = assignment['assignment']
            instance_id = assignment['instance_id']
            unassigned.remove(instance_id)
            assignments[assigned].append(assignment)

        assignments[None] = [Item(self._assignments, data={
            'instance_id': instance_id,
            'service': service
        }) for instance_id in unassigned]

        return assignments

    def get_availability_zones_for_region(self, region):
        vpc = boto.vpc.connect_to_region(region)
        az = region + '-zzz'
        azs = []
        response = {
            'region_name': region
        }

        try:
            vpcs = vpc.get_all_vpcs()
            vpc.create_subnet(vpcs[0].id,
                              '172.31.192.0/20',
                              availability_zone=az)
        except EC2ResponseError as e:
            if e:
                split_message = "Subnets can currently only be created in the " \
                                "following availability zones: "
                azs = str(e).split(split_message, 1)[1]
                azs = azs.split(".</Message>", 1)[0]
                azs = azs.split(", ")
        except:
            """Region does not exist"""

        for n, zone in enumerate(azs):
            response['az' + str(n + 1)] = zone

        return response

    def get_region_params(self, regions):
        keys = [{'region_name': region} for region in regions]
        region_params = {}
        for item in self._regions.batch_get(keys):
            region_params[item['region_name']] = dict(item)

        new_regions = []
        for region in regions:
            if region not in region_params:
                new_region = self.get_availability_zones_for_region(region)
                region_params[region] = new_region
                new_regions.append(region)
        if new_regions:
            with self._regions.batch_write() as region_batch:
                region_batch.put_item(new_region)

        return region_params
