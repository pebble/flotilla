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
    dynamo = boto.dynamodb2.connect_to_region('us-east-1')
    tables = DynamoDbTables(dynamo, environment='develop')
    tables.setup(['assignments', 'regions', 'revisions', 'services', 'units'])
    db = FlotillaClientDynamo(tables.assignments, tables.regions,
                              tables.revisions, tables.services, tables.units)

    # Autoprovisioned ElasticSearch service:
    elasticsearch_dns = 'elasticsearch-develop.mycloudand.me'
    db.configure_service('elasticsearch', {
        'regions': ['us-east-1'],
        'public_ports': {9200: 'HTTP'},
        'private_ports': {9300: ['TCP']},
        'health_check': 'HTTP:9200/',
        'instance_type': 't2.small',
        'elb_scheme': 'internal',
        'dns_name': elasticsearch_dns,
        'log_driver': 'fluentd'
    })
    elasticsearch = FlotillaDockerService('elasticsearch.service',
                                          'pwagner/elasticsearch-aws:latest',
                                          ports={9200: 9200, 9300: 9300})
    db.add_revision('elasticsearch', FlotillaServiceRevision(label='initial',
                                                             units=[
                                                                 elasticsearch
                                                             ]))

    # Autoprovisioned Kibana frontend:
    db.configure_service('kibana', {
        'regions': ['us-east-1'],
        'public_ports': {80: 'HTTP'},
        'health_check': 'HTTP:80/',
        'instance_type': 't2.micro',
        'log_driver': 'fluentd'
    })
    es_url = 'http://%s:9200' % elasticsearch_dns
    kibana = FlotillaDockerService('kibana.service',
                                   'kibana:latest',
                                   ports={80: 5601},
                                   environment={
                                       'ELASTICSEARCH_URL': es_url
                                   })
    db.add_revision('kibana', FlotillaServiceRevision(label='initial',
                                                      units=[
                                                          kibana
                                                      ]))

    # Global units forward journald via fluentd:
    fluentd = FlotillaDockerService('fluentd-forwarder.service',
                                    'pwagner/fluentd-elasticsearch:latest',
                                    ports={24224: 24224, 24225: 24225,
                                           24226: 24226},
                                    environment={
                                        'ELASTICSEARCH_HOST': elasticsearch_dns
                                    })

    # https://github.com/ianblenke/docker-fluentd/blob/master/systemd/journald-fluentd.service
    journald = FlotillaUnit('journald-fluent.service', '''[Unit]
Description=Send journald logs to fluentd
After=systemd-journald.service
After=fluentd-forwarder.service

[Service]
Restart=always
RestartSec=30s

ExecStart=/bin/bash -c 'journalctl -o json --since=now -f | ncat 127.0.0.1 24225'
''')

    db.set_global(FlotillaServiceRevision(label='initial', units=[
        fluentd,
        journald
    ]))

    # db.configure_regions(['us-west-2', 'us-east-1'], nat_coreos_channel='stable')

    # cloudformation = boto.cloudformation.connect_to_region('us-east-1')
    # cf_stack = sync_cloudformation(cloudformation)
