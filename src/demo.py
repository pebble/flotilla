# #!/usr/bin/env python

import logging
import boto.cloudformation
import boto.dynamodb2
from boto.exception import BotoServerError
from flotilla.model import *
from flotilla.db import DynamoDbTables
from flotilla.client import FlotillaClientDynamo

logger = logging.getLogger('flotilla-demo')
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(threadName)s - %(message)s')
logging.getLogger('boto').setLevel(logging.CRITICAL)
logging.getLogger('requests').setLevel(logging.CRITICAL)

stack_name = 'flotilla-develop'

with open('../scheduler.template') as template_in:
    stack_body = template_in.read()


def sync_cloudformation(cloudformation):
    logger.debug('Creating CloudFormation stack...')
    try:
        cloudformation.update_stack(stack_name, template_body=stack_body,
                                    capabilities=['CAPABILITY_IAM'])
    except BotoServerError as e:
        if e.error_code == 'ValidationError':
            if e.message == 'No updates are to be performed.':
                logger.debug('CloudFormation already in sync.')
            else:
                cloudformation.create_stack(stack_name,
                                            template_body=stack_body,
                                            capabilities=['CAPABILITY_IAM'])
        else:
            raise e
    stack = cloudformation.describe_stacks(stack_name)[0]
    while stack.stack_status not in ('UPDATE_COMPLETE', 'CREATE_COMPLETE'):
        logger.debug('Waiting for CloudFormation: %s', stack.stack_status)
        time.sleep(2)
        stack = cloudformation.describe_stacks(stack_name)[0]
    return stack


if __name__ == '__main__':
    # dynamo = boto.dynamodb2.connect_to_region('us-east-1')
    # tables = DynamoDbTables(dynamo, environment='develop')
    # tables.setup(['regions', 'revisions', 'services', 'units'])
    # db = FlotillaClientDynamo(tables.regions, tables.revisions, tables.services,
    #                           tables.units)
    #
    # v3_noconfig = FlotillaDockerService('echo.service',
    #                                     'pwagner/http-env-echo:3.0.0',
    #                                     ports={80: 8080},
    #                                     environment={'MESSAGE': 'test'})
    # db.add_revision('hello', FlotillaServiceRevision(label='initial',
    #                                                    units=[v3_noconfig]))

    # db.configure_regions(['us-west-2', 'us-east-1'], nat_coreos_channel='stable')

    cloudformation = boto.cloudformation.connect_to_region('us-east-1')
    cf_stack = sync_cloudformation(cloudformation)
    # outputs = {output.key: output.value for output in cf_stack.outputs}
    # elb_address = 'http://{0}'.format(outputs['ElbAddress'])
    # print elb_address
