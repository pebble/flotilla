import logging
from collections import defaultdict

logger = logging.getLogger('flotilla')


class FlotillaProvisioner(object):
    def __init__(self, environment, region, scheduler, db, cloudformation):
        self._environment = environment
        self._region = region
        self._scheduler = scheduler
        self._db = db
        self._cloudformation = cloudformation

    def provision(self):
        if not self._scheduler.active:
            return

        # Index services that desire stacks:
        services = {}
        service_stacks = {}
        for service in self._db.services():
            name = service['service_name']
            services[name] = dict(service)
            if service.get('provision', True):
                service_stacks[name] = None

        if not service_stacks:
            logger.debug('No stacks to be provisioned.')
            return

        # Load existing stacks from database:
        region_stack = None
        for stack in self._db.get_stacks():
            service_name = stack.get('service')
            if not service_name:
                region_stack = dict(stack)
                continue
            if service_name not in service_stacks:
                logger.warn('Service %s no longer required', service_name)
                # TODO: delete stack
                continue
            service_stacks[service_name] = dict(stack)

        changed_stacks = []

        # Create/update VPC in this region:
        region_item = self._db.get_region_params(self._region)
        vpc_stack = self._cloudformation.vpc(region_item, region_stack)
        if vpc_stack:
            changed_stacks.append(vpc_stack)
            region_stack = vpc_stack

        # Exit if VPC stack hasn't completed:
        region_outputs = region_stack and region_stack.get('outputs')
        if not region_outputs:
            logger.debug('Waiting on VPC in %s for: %s', self._region,
                         service_stacks.keys())
            self._db.set_stacks(changed_stacks)
            return

        for service_name, service_stack in service_stacks.items():
            service = services[service_name]

            new_service_stack = self._cloudformation.service(self._region,
                                                             service,
                                                             region_outputs,
                                                             service_stack)
            if new_service_stack:
                changed_stacks.append(new_service_stack)

        self._db.set_stacks(changed_stacks)
