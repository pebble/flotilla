import logging
from collections import defaultdict

logger = logging.getLogger('flotilla')


class FlotillaProvisioner(object):
    def __init__(self, environment, scheduler, db, cloudformation):
        self._environment = environment
        self._scheduler = scheduler
        self._db = db
        self._cloudformation = cloudformation

    def provision(self):
        if not self._scheduler.active:
            return

        # Map desired stacks by region/service:
        services = {}
        region_stacks = {}
        service_stacks = defaultdict(dict)
        for service in self._db.services():
            name = service['service_name']
            services[name] = dict(service)
            for region in service.get('regions', []):
                service_stacks[region][name] = None
                region_stacks[region] = None

        if not region_stacks:
            logger.debug('No regions defined, nothing to provision.')
            return

        # Load existing stacks from database:
        for stack in self._db.get_stacks():
            region = stack['region']
            service_name = stack.get('service')
            if not service_name:
                region_stacks[region] = dict(stack)
                continue
            if service_name not in service_stacks[region]:
                logger.warn('Service %s no longer required', service_name)
                # TODO: delete stack
                continue
            service_stacks[region][service_name] = dict(stack)

        # Dump initial state to logs:
        logger.debug('Service stacks: %s', dict(service_stacks))
        logger.debug('Region stacks: %s', region_stacks)

        changed_stacks = []

        # Create/update VPCs in each region:
        region_params = self._db.get_region_params(region_stacks.keys())
        for region_name, stack in region_stacks.items():
            region = region_params[region_name]
            vpc_params = self._cloudformation._vpc_params(region_name, region)
            vpc_hash = self._cloudformation.vpc_hash(vpc_params)
            if not self._complete(stack, vpc_hash):
                service_stack = self._cloudformation.vpc(region_name,
                                                         vpc_params)
                stack_outputs = {o.key: o.value for o in service_stack.outputs}
                stack = {'stack_arn': service_stack.stack_id,
                         'region': region_name,
                         'outputs': stack_outputs,
                         'stack_hash': vpc_hash}
                changed_stacks.append(stack)
                region_stacks[region_name] = stack
            else:
                logger.debug('Found up-to-date VPC in %s', region_name)

        # Create/update service stacks (when a VPC is available in the region)
        for region, region_services in service_stacks.items():
            vpc_outputs = region_stacks[region]['outputs']
            if not vpc_outputs:
                logger.debug('Waiting on VPC in %s for %s', region,
                             region_services.keys())
                continue

            for service_name, stack in region_services.items():
                service = services[service_name]
                service_hash = self._cloudformation.service_hash(service,
                                                                 vpc_outputs)
                if not self._complete(stack, service_hash):
                    service_stack = self._cloudformation.service(region,
                                                                 service,
                                                                 vpc_outputs)
                    stack_outputs = {o.key: o.value for o in
                                     service_stack.outputs}
                    stack = {'stack_arn': service_stack.stack_id,
                             'service': service_name,
                             'region': region,
                             'outputs': stack_outputs,
                             'stack_hash': service_hash}
                    changed_stacks.append(stack)
                    service_stacks[region][service_name] = stack
                else:
                    logger.debug('Found up-to-date %s in %s', service_name,
                                 region)

        # Store updates to DB:
        if changed_stacks:
            self._db.set_stacks(changed_stacks)

    @staticmethod
    def _complete(stack, expected_hash):
        if not stack:
            return False
        if stack.get('stack_hash') != expected_hash:
            # Exists but mismatch:
            return False
        elif not stack.get('outputs'):
            # Exists but not finished:
            return False
        return True
