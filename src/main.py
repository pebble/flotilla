#!/usr/bin/env python

import logging
import time
import boto.dynamodb2
import boto.ec2.elb
import boto.utils


def setup_logging():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(threadName)s - %(message)s')
    logging.getLogger('boto').setLevel(logging.CRITICAL)


def get_instance_id():
    # try:
    #     metadata = boto.utils.get_instance_metadata(timeout=2, num_retries=2)
    #     return metadata['instance-id']
    # except:
    #     return 'i-%s' % str(time.time()).replace('.', '')
    return 'i-123456'


if __name__ == '__main__':
    setup_logging()

    # # Localizing information:
    # instance_id = get_instance_id()
    # service = os.environ['FLOTILLA_SERVICE']
    #
    # elb_name = os.environ.get('FLOTILLA_LB')
    # elb_region = os.environ.get('FLOTILLA_LB_REGION', 'us-west-2')
    # if elb_name:
    #     elb = boto.ec2.elb.connect_to_region(elb_region)
    # else:
    #     elb = None
    #
    # dynamo = boto.dynamodb2.connect_to_region('us-east-1')
    # manager = Manager()
    #
    # db = DynamoDbFlotillaStorage(instance_id, dynamo)
    # scheduler = FlotillaScheduler(db)
    # systemd = SystemdUnits(manager)
    # lb = LoadBalancer(instance_id, elb_name, elb)
    #
    # agent = FlotillaAgent(service, db, scheduler, systemd, lb)
    #
    # threads = FlotillaThreads(agent, scheduler)
    # threads.run()
