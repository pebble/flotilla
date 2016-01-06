# #!/usr/bin/env python

import logging
import boto.dynamodb2
import boto.kms
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

if __name__ == '__main__':
    db_region = 'us-east-1'

    dynamo = boto.dynamodb2.connect_to_region(db_region)
    kms = boto.kms.connect_to_region(db_region)
    tables = DynamoDbTables(dynamo, environment='develop')
    tables.setup(['assignments', 'regions', 'revisions', 'services', 'units'])
    db = FlotillaClientDynamo(tables.assignments, tables.regions,
                              tables.revisions, tables.services, tables.units,
                              kms)

    # Autoprovisioned ElasticSearch service:
    elasticsearch_dns = 'elasticsearch-develop.mycloudand.me'
    db.configure_service('elasticsearch', {
        'regions': ['us-east-1', 'us-west-2'],
        'public_ports': {9200: 'HTTP'},
        'private_ports': {9300: ['TCP']},
        'health_check': 'HTTP:9200/',
        'instance_type': 't2.small',
        'elb_scheme': 'internal',
        'dns_name': elasticsearch_dns,
    })
    elasticsearch = FlotillaDockerService('elasticsearch.service',
                                          'pwagner/elasticsearch-aws:latest',
                                          logdriver='fluentd',
                                          ports={9200: 9200, 9300: 9300})
    db.add_revision('elasticsearch', FlotillaServiceRevision(label='initial',
                                                             units=[
                                                                 elasticsearch
                                                             ]))

    # Autoprovisioned Kibana frontend:
    db.configure_service('kibana', {
        'regions': ['us-east-1', 'us-west-2'],
        'public_ports': {80: 'HTTP'},
        'health_check': 'HTTP:80/',
        'instance_type': 't2.micro',
    })
    es_url = 'http://%s:9200' % elasticsearch_dns
    kibana = FlotillaDockerService('kibana.service',
                                   'kibana:latest',
                                   ports={80: 5601},
                                   logdriver='fluentd',
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
                                    ports={24224: 24224, 24225: 24225},
                                    logdriver='fluentd',
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
