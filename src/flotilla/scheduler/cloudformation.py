import hashlib
import logging
import boto.cloudformation
from boto.cloudformation.stack import Stack
from boto.exception import BotoServerError

logger = logging.getLogger('flotilla')

DONE_STATES = ('CREATE_COMPLETE',
               'ROLLBACK_COMPLETE',
               'UPDATE_COMPLETE',
               'UPDATE_ROLLBACK_COMPLETE')


def sha256(val, params={}):
    hasher = hashlib.sha256()
    hasher.update(val)
    for k in sorted(params.keys()):
        hasher.update(k)
        hasher.update(params[k])
    return hasher.hexdigest()


class FlotillaCloudFormation(object):
    def __init__(self, environment):
        self._clients = {}
        self._environment = environment
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

    def service(self, region, name, params):
        """
        Create stack for service.
        :param region: Region.
        :param name: Service name.
        :param params: Service stack parameters.
        :return: CloudFormation Stack
        """
        name = 'flotilla-{0}-{1}'.format(self._environment, name)
        return self._stack(region, name, self._service_elb, params)

    def _stack(self, region, name, template, params=None):
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

    def service_hash(self, params):
        """
        Get hash for service template with given parameters.
        :param params: Service parameters
        :return: Hash.
        """
        return sha256(self._service_elb, params)

    def _client(self, region):
        client = self._clients.get(region)
        if not client:
            client = boto.cloudformation.connect_to_region(region)
            self._clients[region] = client
        return client
