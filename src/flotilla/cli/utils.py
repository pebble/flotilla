import logging
from botocore.exceptions import ClientError

QUEUE_NOT_FOUND = 'AWS.SimpleQueueService.NonExistentQueue'

logger = logging.getLogger('flotilla')


def get_queue(sqs, queue_name):
    try:
        return sqs.get_queue_by_name(QueueName=queue_name)
    except ClientError as e:
        error_code = e.response['Error'].get('Code')
        if error_code != QUEUE_NOT_FOUND:
            raise e

        logger.info('Queue %s not found.', queue_name)
        return None
