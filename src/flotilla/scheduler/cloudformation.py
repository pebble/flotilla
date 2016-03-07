import hashlib
import json
import logging
import time
import boto.cloudformation
from boto.cloudformation.stack import Stack
from boto.exception import BotoServerError
from copy import deepcopy

logger = logging.getLogger('flotilla')

DONE_STATES = ('CREATE_COMPLETE',
               'ROLLBACK_COMPLETE',
               'UPDATE_COMPLETE',
               'UPDATE_ROLLBACK_COMPLETE')

SERVICE_KEYS_STRINGS = ('coreos_channel',
                        'coreos_version',
                        'dns_name',
                        'elb_scheme',
                        'health_check',
                        'instance_max',
                        'instance_min',
                        'instance_type',
                        'kms_key')

SERVICE_KEYS_ITERABLE = ('private_ports',
                         'public_ports',
                         'regions')

FORWARD_FIELDS = ['VpcId', 'NatSecurityGroup']
for subnet_index in range(1, 4):
    FORWARD_FIELDS.append('PublicSubnet0%d' % subnet_index)
    FORWARD_FIELDS.append('PrivateSubnet0%d' % subnet_index)

CAPABILITIES = ('CAPABILITY_IAM',)


def sha256(val, params={}):
    hasher = hashlib.sha256()
    hasher.update(val)
    for k in sorted(params.keys()):
        hasher.update(k)
        hasher.update(str(params[k]))
    return hasher.hexdigest()


class FlotillaCloudFormation(object):
    def __init__(self, environment, domain, coreos, backoff=2.0):
        self._clients = {}
        self._environment = environment
        self._domain = domain
        self._coreos = coreos
        self._backoff = backoff
        self._templates = {}

    def _template(self, template):
        cached = self._templates.get(template)
        if cached:
            return cached

        with open('cloudformation/%s.template' % template) as template_in:
            loaded = template_in.read()
            self._templates[template] = loaded
            return loaded

    def vpc(self, region, stack):
        """
        Create VPC for hosting services in region.
        :param region:  Region item.
        :param stack: Existing (flotilla internal) stack.
        :return: Modified stack, none if already complete.
        """
        region_name = region['region_name']
        stack_params = self._vpc_params(region)
        template = self._template('vpc')
        stack_hash = sha256(template, stack_params)
        if self._complete(stack, stack_hash):
            logger.debug('VPC stack complete in %s.', region_name)
            return None
        stack_name = 'flotilla-{0}-vpc'.format(self._environment)

        new_stack = self._stack(region_name, stack_name, template, stack_params)
        stack_outputs = {o.key: o.value for o in new_stack.outputs}
        return {'stack_arn': new_stack.stack_id,
                'region': region_name,
                'outputs': stack_outputs,
                'stack_hash': stack_hash}

    def _vpc_params(self, region):
        region_name = region.get('region_name')
        nat_coreos_channel = region.get('nat_coreos_channel', 'stable')
        nat_coreos_version = region.get('nat_coreos_version', 'current')
        nat_ami = self._coreos.get_ami(nat_coreos_channel, nat_coreos_version,
                                       region_name)
        nat_instance_type = region.get('nat_instance_type', 't2.nano')

        az1 = region.get('az1', '%sa' % region_name)
        az2 = region.get('az2', '%sb' % region_name)
        az3 = region.get('az3', '%sc' % region_name)

        container = region.get('flotilla_container')

        params = {
            'FlotillaEnvironment': self._environment,
            'NatInstanceType': nat_instance_type,
            'NatAmi': nat_ami,
            'Az1': az1,
            'Az2': az2,
            'Az3': az3
        }
        if container:
            params['FlotillaContainer'] = container
        return params

    def service(self, region, service, vpc_outputs, stack):
        """
        Create stack for service.
        :param region: Region name.
        :param service: Service.
        :param vpc_outputs: VPC stack outputs.
        :return: CloudFormation Stack
        """
        region_name = region['region_name']
        service_name = service['service_name']
        service_hash = self._service_hash(service, vpc_outputs)
        if self._complete(stack, service_hash):
            logger.debug('Service stack for %s complete in %s.', service_name,
                         region_name)
            return None

        name = 'flotilla-{0}-worker-{1}'.format(self._environment, service_name)

        service_params = self._service_params(region, service, vpc_outputs)
        template = self._template('service-elb')

        json_template = json.loads(template)
        resources = json_template['Resources']

        # Public ports are exposed to ELB, as a listener by ELB:
        public_ports = service.get('public_ports')
        if public_ports:
            listeners = []
            elb_ingress = []
            instance_ingress = [{
                'IpProtocol': 'tcp',
                'FromPort': 22,
                'ToPort': 22,
                'SourceSecurityGroupId': {'Ref': 'NatSecurityGroup'}
            }]
            for port, protocol in public_ports.items():
                listeners.append({
                    'InstancePort': port,
                    'LoadBalancerPort': port,
                    'Protocol': protocol,
                    'InstanceProtocol': "HTTP"
                })
                # TODO: support proto=HTTPS

                elb_ingress.append({
                    'IpProtocol': 'tcp',
                    'FromPort': port,
                    'ToPort': port,
                    'CidrIp': '0.0.0.0/0'
                })
                instance_ingress.append({
                    'IpProtocol': 'tcp',
                    'FromPort': port,
                    'ToPort': port,
                    'SourceSecurityGroupId': {'Ref': 'ElbSg'}
                })

            resources['Elb']['Properties']['Listeners'] = listeners
            elb_sg = resources['ElbSg']['Properties']
            elb_sg['SecurityGroupIngress'] = elb_ingress
            instance_sg = resources['InstanceSg']['Properties']
            instance_sg['SecurityGroupIngress'] = instance_ingress

        # Private ports can be used between instances:
        private_ports = service.get('private_ports')
        if private_ports:
            for private_port, protocols in private_ports.items():
                for protocol in protocols:
                    port_resource = 'PrivatePort%s%s' % (private_port, protocol)
                    resources[port_resource] = {
                        'Type': 'AWS::EC2::SecurityGroupIngress',
                        'Properties': {
                            'GroupId': {'Ref': 'InstanceSg'},
                            'IpProtocol': protocol,
                            'FromPort': private_port,
                            'ToPort': private_port,
                            'SourceSecurityGroupId': {'Ref': 'InstanceSg'}
                        }
                    }

        service_stack = self._stack(region_name, name,
                                    json.dumps(json_template, indent=2),
                                    service_params)

        stack_outputs = {o.key: o.value for o in
                         service_stack.outputs}
        stack = {'stack_arn': service_stack.stack_id,
                 'service': service_name,
                 'region': region_name,
                 'outputs': stack_outputs,
                 'stack_hash': service_hash}
        return stack

    def _service_params(self, region, service, vpc_outputs):
        region_name = region['region_name']
        service_name = service['service_name']
        params = {k: vpc_outputs.get(k) for k in FORWARD_FIELDS}
        params['FlotillaEnvironment'] = self._environment
        params['ServiceName'] = service_name
        # FIXME: HA by default, don't be cheap
        params['InstanceType'] = service.get('instance_type', 't2.nano')
        instance_min = service.get('instance_min', '1')
        params['InstanceMin'] = instance_min
        params['InstanceMax'] = service.get('instance_max', instance_min)
        params['HealthCheckTarget'] = service.get('health_check', 'TCP:80')
        params['ElbScheme'] = service.get('elb_scheme', 'internet-facing')
        params['KmsKey'] = service.get('kms_key', '')

        dns_name = service.get('dns_name')
        if dns_name:
            domain = dns_name.split('.')
            domain = '.'.join(domain[-2:]) + '.'
            params['VirtualHostDomain'] = domain
            params['VirtualHost'] = dns_name
        else:
            params['VirtualHostDomain'] = self._domain + '.'
            generated_dns = '%s-%s.%s' % (service_name, self._environment,
                                          self._domain)
            params['VirtualHost'] = generated_dns
        coreos_channel = service.get('coreos_channel', 'stable')
        coreos_version = service.get('coreos_version', 'current')
        ami = self._coreos.get_ami(coreos_channel, coreos_version, region_name)
        params['Ami'] = ami

        container = region.get('flotilla_container')
        if container:
            params['FlotillaContainer'] = container
        return params

    def tables(self, regions):
        """
        Create table for stack in every hosted region.
        :param regions: Regions.
        """
        name = 'flotilla-{0}-tables'.format(self._environment)
        params = {
            'FlotillaEnvironment': self._environment
        }

        # Create/update stack in each region:
        table_stacks = {
            region: self._stack(region, name, self._template('tables'), params)
            for region in regions}

        self._wait_for_stacks(table_stacks)
        logger.debug('Finished creating tables in %s', regions)

    def schedulers(self, region_params):
        """
        Create scheduler stack in each region.
        :param region_params: Map of region_name -> parameter map.
        """
        name = 'flotilla-{0}-scheduler'.format(self._environment)
        base_params = {
            'FlotillaEnvironment': self._environment
        }

        # If there are regions without a local scheduler, hack IAM Role
        template = self._template('scheduler')
        for params in region_params.values():
            if not params.get('scheduler'):
                regions = region_params.keys()
                template = self._scheduler_for_regions(regions)
                base_params['FlotillaRegion'] = ' '.join(regions)
                break

        # Create scheduler stacks:
        scheduler_stacks = {}
        for region, params in region_params.items():
            if not params.get('scheduler'):
                continue

            scheduler_params = base_params.copy()
            scheduler_params['InstanceType'] = params['scheduler_instance_type']
            scheduler_params['Ami'] = self._coreos.get_ami(
                    params['scheduler_coreos_channel'],
                    params['scheduler_coreos_version'],
                    region)
            for i in range(1, 4):
                scheduler_params['Az%d' % i] = params['az%d' % i]

            container = params.get('flotilla_container')
            if container:
                scheduler_params['FlotillaContainer'] = container

            if 'FlotillaRegion' not in scheduler_params:
                scheduler_params['FlotillaRegion'] = region

            scheduler_stacks[region] = self._stack(region, name, template,
                                                   scheduler_params)
        self._wait_for_stacks(scheduler_stacks)

    def _scheduler_for_regions(self, regions):
        """
        Doctor scheduler template for operating in multiple regions.
        :param regions: Region list.
        :return: Customized template.
        """
        template_json = json.loads(self._template('scheduler'))
        resources = template_json['Resources']
        for role_policy in resources['Role']['Properties']['Policies']:
            policy_name = role_policy['PolicyName']
            if policy_name not in ('FlotillaDynamo', 'FlotillaQueue'):
                continue

            statements = role_policy['PolicyDocument']['Statement']
            for statement in statements:
                # Replace "this region" reference with every managed region:
                new_resources = []
                for region in regions:
                    region_resource = deepcopy(statement['Resource'])
                    region_resource['Fn::Join'][1][1] = region
                    new_resources.append(region_resource)
                statement['Resource'] = new_resources

        return json.dumps(template_json, indent=2)

    def _stack(self, region, name, template, params):
        """
        Create/update CloudFormation stack if possible.
        :param region: Region.
        :param name: Stack name.
        :param template: Template body.
        :param params: Template parameters.
        :return: CloudFormation Stack
        """
        params = [(k, v) for k, v in params.items()]
        client = self._client(region)

        try:
            logger.debug('Describing stack %s in %s...', name, region)
            existing = client.describe_stacks(name)[0]
        except BotoServerError:
            existing = None

        if existing:
            if existing.stack_status not in DONE_STATES:
                logger.debug('Stack %s is %s', name, existing.stack_status)
                return existing

            # Attempt update:
            try:
                stack_id = client.update_stack(name,
                                               capabilities=CAPABILITIES,
                                               template_body=template,
                                               parameters=params)
                logger.debug('Updated stack %s in %s', name, region)
                stack = Stack()
                stack.stack_id = stack_id
                return stack
            except BotoServerError as e:
                if e.message == 'No updates are to be performed.':
                    return existing
                raise e
        else:  # not existing
            logger.debug('Creating stack %s in %s', name, region)
            stack_id = client.create_stack(name,
                                           capabilities=CAPABILITIES,
                                           template_body=template,
                                           parameters=params)
            stack = Stack()
            stack.stack_id = stack_id
            return stack

    def _wait_for_stacks(self, stacks):
        done = False
        while not done:
            done = True
            for region, stack in stacks.items():
                if stack.stack_status not in DONE_STATES:
                    done = False
                    logger.info('Waiting for stack in %s', region)

                    client = self._client(region)
                    stacks[region] = client.describe_stacks(stack.stack_id)[0]
            if not done:
                time.sleep(self._backoff)

    def _service_hash(self, service, vpc_outputs):
        """
        Get hash for service template with given parameters.
        :param service: Service item.
        :param vpc_outputs: Parent VPC outputs.
        :return: Hash.
        """
        params = dict(vpc_outputs)

        for key in SERVICE_KEYS_ITERABLE:
            value = service.get(key)
            if value:
                params[key] = sorted(value)

        for key in SERVICE_KEYS_STRINGS:
            value = service.get(key)
            if value:
                params[key] = value
        return sha256(self._template('service-elb'), params)

    def _client(self, region):
        client = self._clients.get(region)
        if not client:
            client = boto.cloudformation.connect_to_region(region)
            self._clients[region] = client
        return client

    @staticmethod
    def _complete(stack, expected_hash):
        if not stack:
            logger.debug('Stack not defined.')
            return False
        existing_hash = stack.get('stack_hash')
        if existing_hash != expected_hash:
            logger.debug('Stack exists but hash does not match: %s vs %s',
                         existing_hash, expected_hash)
            # Exists but mismatch:
            return False
        elif not stack.get('outputs'):
            logger.debug('Stack exists but is not complete.')
            # Exists but not finished:
            return False
        logger.debug('Stack is completed.')
        return True
