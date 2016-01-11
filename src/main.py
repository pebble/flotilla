import logging
import time
import boto.utils


def setup_logging():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
    logging.getLogger('boto').setLevel(logging.CRITICAL)


def get_instance_id():
    try:
        metadata = boto.utils.get_instance_metadata(timeout=2, num_retries=2)
        return metadata['instance-id']
    except:
        return 'i-%s' % str(time.time()).replace('.', '')
