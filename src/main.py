import logging
import time
import boto.utils


def setup_logging():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(threadName)s - %(message)s')
    logging.getLogger('boto').setLevel(logging.CRITICAL)


def get_instance_id():
    try:
        metadata = boto.utils.get_instance_metadata(timeout=2, num_retries=2)
        return metadata['instance-id']
    except:
        return 'i-%s' % str(time.time()).replace('.', '')


REGIONS = ('ap-northeast-1',
           'ap-northeast-2',
           'ap-southeast-1',
           'ap-southeast-2',
           'cn-north-1',
           'eu-central-1',
           'eu-west-1',
           'sa-east-1',
           'us-east-1',
           'us-west-1',
           'us-west-2')

INSTANCE_TYPES = ('t2.nano',
                  't2.micro',
                  't2.small')

DEFAULT_REGIONS = ('us-east-1',)
DEFAULT_ENVIRONMENT = 'develop'
