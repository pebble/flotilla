import logging
from collections import defaultdict

logger = logging.getLogger('flotilla')

FORWARD_FIELDS = ['VpcId', 'NatSecurityGroup']
for i in range(1, 4):
    FORWARD_FIELDS.append('PublicSubnet0%d' % i)
    FORWARD_FIELDS.append('PrivateSubnet0%d' % i)


class FlotillaProvisioner(object):
    def __init__(self, environment, domain, scheduler, db, cloudformation,
                 coreos):
        self._environment = environment
        self._domain = domain
        self._scheduler = scheduler
        self._db = db
        self._cloudformation = cloudformation
        self._coreos = coreos

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
            vpc_params = self._vpc_params(region_name, region)
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
                stack_params = self._stack_params(region, service, vpc_outputs)
                service_hash = self._cloudformation.service_hash(stack_params)
                if not self._complete(stack, service_hash):
                    service_stack = self._cloudformation.service(region,
                                                                 service_name,
                                                                 stack_params)
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

    def _vpc_params(self, region_name, region):
        nat_coreos_channel = region.get('nat_coreos_channel', 'stable')
        nat_coreos_version = region.get('nat_coreos_version', 'current')
        nat_ami = self._coreos.get_ami(nat_coreos_channel, nat_coreos_version,
                                       region_name)
        nat_instance_type = region.get('nat_instance_type', 't2.micro')

        az1 = region.get('az1', '%sa' % region_name)
        az2 = region.get('az2', '%sb' % region_name)
        az3 = region.get('az3', '%sc' % region_name)

        return {
            'NatInstanceType': nat_instance_type,
            'NatAmi': nat_ami,
            'Az1': az1,
            'Az2': az2,
            'Az3': az3
        }

    def _stack_params(self, region, service, vpc_outputs):
        service_name = service['service_name']
        stack_params = {k: vpc_outputs.get(k) for k in FORWARD_FIELDS}
        stack_params['FlotillaEnvironment'] = self._environment
        stack_params['ServiceName'] = service_name
        stack_params['InstanceType'] = service.get('instance_type', 't2.micro')
        # FIXME: HA by default, don't be cheap
        stack_params['InstanceMin'] = service.get('instance_min', '1')
        stack_params['InstanceMax'] = service.get('instance_max', '1')

        dns_name = service.get('dns_name')
        if dns_name:
            domain = dns_name.split('.')
            domain = '.'.join(domain[-2:]) + '.'
            stack_params['VirtualHostDomain'] = domain
            stack_params['VirtualHost'] = dns_name + '.'
        else:
            stack_params['VirtualHostDomain'] = self._domain + '.'
            generated_dns = '%s-%s.%s.' % (service_name, self._environment,
                                           self._domain)
            stack_params['VirtualHost'] = generated_dns

        coreos_channel = service.get('coreos_channel', 'stable')
        coreos_version = service.get('coreos_version', 'current')
        ami = self._coreos.get_ami(coreos_channel, coreos_version, region)
        stack_params['Ami'] = ami
        return stack_params

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
