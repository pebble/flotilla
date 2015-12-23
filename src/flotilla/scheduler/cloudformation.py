import hashlib
import json
import logging
import boto.cloudformation
from boto.cloudformation.stack import Stack
from boto.exception import BotoServerError

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
                        'instance_type')

SERVICE_KEYS_ITERABLE = ('private_ports',
                         'public_ports',
                         'regions')

FORWARD_FIELDS = ['VpcId', 'NatSecurityGroup']
for i in range(1, 4):
    FORWARD_FIELDS.append('PublicSubnet0%d' % i)
    FORWARD_FIELDS.append('PrivateSubnet0%d' % i)


def sha256(val, params={}):
    hasher = hashlib.sha256()
    hasher.update(val)
    for k in sorted(params.keys()):
        hasher.update(k)
        hasher.update(str(params[k]))
    return hasher.hexdigest()


class FlotillaCloudFormation(object):
    def __init__(self, environment, domain, coreos):
        self._clients = {}
        self._environment = environment
        self._domain = domain
        self._coreos = coreos
        # FIXME: input as param
        self._db_region = 'us-east-1'
        with open('cloudformation/vpc.template') as template_in:
            self._vpc = template_in.read()
        with open('cloudformation/service-elb.template') as template_in:
            self._service_elb = template_in.read()

    def vpc(self, region, params=None):
        """
        Create VPC in for hosting services in region.
        :param region: Region.
        :param params: VPC stack parameters.
        :return: CloudFormation Stack.
        """
        name = 'flotilla-{0}-vpc'.format(self._environment)
        return self._stack(region, name, self._vpc, params)

    def _vpc_params(self, region_name, region):
        nat_coreos_channel = region.get('nat_coreos_channel', 'stable')
        nat_coreos_version = region.get('nat_coreos_version', 'current')
        nat_ami = self._coreos.get_ami(nat_coreos_channel, nat_coreos_version,
                                       region_name)
        nat_instance_type = region.get('nat_instance_type', 't2.nano')

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

    def service(self, region, service, vpc_outputs):
        """
        Create stack for service.
        :param region: Region.
        :param service: Service.
        :param vpc_outputs: VPC stack outputs.
        :return: CloudFormation Stack
        """
        name = 'flotilla-{0}-{1}'.format(self._environment,
                                         service['service_name'])
        service_params = self._service_params(region, service, vpc_outputs)
        json_template = json.loads(self._service_elb)
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

        return self._stack(region, name, json.dumps(json_template),
                           service_params)

    def _service_params(self, region, service, vpc_outputs):
        service_name = service['service_name']
        params = {k: vpc_outputs.get(k) for k in FORWARD_FIELDS}
        params['FlotillaEnvironment'] = self._environment
        params['DynamoDbRegion'] = self._db_region
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
        ami = self._coreos.get_ami(coreos_channel, coreos_version, region)
        params['Ami'] = ami
        return params

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
            logger.debug('Describing stack %s...', name)
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
                                               capabilities=['CAPABILITY_IAM'],
                                               template_body=template,
                                               parameters=params)
                logger.debug('Updated stack %s', name)
                stack = Stack()
                stack.stack_id = stack_id
                return stack
            except BotoServerError as e:
                if e.message == 'No updates are to be performed.':
                    return existing
                raise e
        else:  # not existing
            logger.debug('Created stack %s', name)
            stack_id = client.create_stack(name,
                                           capabilities=['CAPABILITY_IAM'],
                                           template_body=template,
                                           parameters=params)
            stack = Stack()
            stack.stack_id = stack_id
            return stack

    def vpc_hash(self, params):
        """
        Get hash for VPC template with given parameters.
        :param params: VPC parameters
        :return: Hash.
        """
        return sha256(self._vpc, params)

    def service_hash(self, service, vpc_outputs):
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
        return sha256(self._service_elb, params)

    def _client(self, region):
        client = self._clients.get(region)
        if not client:
            client = boto.cloudformation.connect_to_region(region)
            self._clients[region] = client
        return client
