# #!/usr/bin/env python
#
# import logging
# from collections import defaultdict
# import time
# import boto.cloudformation
# import boto.dynamodb2
# from boto.exception import BotoServerError
# from flotilla.service import *
# from flotilla.db import DynamoDbFlotillaStorage
# import requests
# import threading
#
# logger = logging.getLogger('flotilla-demo')
# logging.basicConfig(level=logging.DEBUG,
#                     format='%(asctime)s - %(threadName)s - %(message)s')
# logging.getLogger('boto').setLevel(logging.CRITICAL)
# logging.getLogger('requests').setLevel(logging.CRITICAL)
#
# stack_name = 'flotilla-test'
#
# with open('../cloudformation.template') as template_in:
#     stack_body = template_in.read()
#
#
# def sync_cloudformation(cloudformation):
#     logger.debug('Creating CloudFormation stack...')
#     try:
#         cloudformation.update_stack(stack_name, template_body=stack_body,
#                                     capabilities=['CAPABILITY_IAM'])
#     except BotoServerError as e:
#         if e.error_code == 'ValidationError':
#             if e.message == 'No updates are to be performed.':
#                 logger.debug('CloudFormation already in sync.')
#             else:
#                 cloudformation.create_stack(stack_name,
#                                             template_body=stack_body,
#                                             capabilities=['CAPABILITY_IAM'])
#         else:
#             raise e
#     stack = cloudformation.describe_stacks(stack_name)[0]
#     while stack.stack_status not in ('UPDATE_COMPLETE', 'CREATE_COMPLETE'):
#         logger.debug('Waiting for CloudFormation: %s', stack.stack_status)
#         time.sleep(2)
#         stack = cloudformation.describe_stacks(stack_name)[0]
#     return stack
#
#
# def sample_elb(elb_address, count=10, sleep=0.1):
#     versions = defaultdict(lambda: 0)
#     messages = defaultdict(lambda: 0)
#     statuses = defaultdict(lambda: 0)
#
#     for _ in range(count):
#         try:
#             r = requests.get(elb_address)
#         except:
#             statuses['NONE'] += 1
#             versions['NONE'] += 1
#             messages['NONE'] += 1
#             continue
#         statuses[r.status_code] += 1
#         try:
#             response = r.json()
#             versions[str(response['version'])] += 1
#             messages[str(response['message'])] += 1
#         except ValueError:
#             versions['NOT_JSON'] += 1
#             messages['NOT_JSON'] += 1
#         time.sleep(sleep)
#
#     logger.debug("Samples: %d", count)
#     logger.debug('Statuses: %s', dict(statuses))
#     logger.debug('Versions: %s', dict(versions))
#     logger.debug('Messages: %s', dict(messages))
#     return versions, messages, statuses
#
#
# SYNC_TIME = 60
#
# if __name__ == '__main__':
#     dynamo = boto.dynamodb2.connect_to_region('us-east-1')
#     db = DynamoDbFlotillaStorage(None, dynamo)
#
#     for rev in ['initial', 'configured', 'upgraded']:
#         db.del_revision('testapp', rev)
#
#     # START TALKING FROM HERE:
#
#     logger.info('Storing initial configuration...')
#     v3_noconfig = FlotillaDockerService('echo.service',
#                                         'pwagner/http-env-echo:3.0.0',
#                                         ports={80: 8080})
#     db.add_revision('testapp', FlotillaServiceRevision(label='initial',
#                                                        units=[v3_noconfig]))
#
#     cloudformation = boto.cloudformation.connect_to_region('us-east-1')
#     cf_stack = sync_cloudformation(cloudformation)
#     outputs = {output.key: output.value for output in cf_stack.outputs}
#     elb_address = 'http://{0}'.format(outputs['ElbAddress'])
#
#     def background_poll():
#         sample_elb(elb_address, 500, 1)
#     background_thread = threading.Thread(target=background_poll)
#     background_thread.start()
#
#     logger.info('Waiting for ELB...')
#     r = requests.get(elb_address)
#     while r.status_code != 200:
#         logger.debug('Waiting for ELB: %s', r.status_code)
#         time.sleep(2)
#         r = requests.get(elb_address)
#
#     logger.info('Sampling initial configuration -  100% v3 and no message:')
#     sample_elb(elb_address, 100, 0.5)
#
#     logger.info('Adding second configuration...')
#     v3_config = FlotillaDockerService('echo.service',
#                                       'pwagner/http-env-echo:3.0.0',
#                                       ports={80: 8080},
#                                       environment={'MESSAGE': 'hella whirled'})
#     db.add_revision('testapp', FlotillaServiceRevision(label='configured',
#                                                        units=[v3_config]))
#
#     # Sleep for sync (TODO: more push, less pull)
#     time.sleep(SYNC_TIME)
#
#     logger.info('Sampling testing configuration -  50% v3 and no message,' +
#                 ' 50% v3 and message:')
#     sample_elb(elb_address, 100, 0.5)
#
#     # Remove the first revision
#     logger.debug('Removing initial configuration...')
#     db.del_revision('testapp', 'initial')
#     time.sleep(SYNC_TIME)
#
#     logger.info('Sampling second configuration -  100% v3 and message:')
#     sample_elb(elb_address, 100, 0.5)
#
#     logger.debug('Adding upgraded configuration...')
#     v4_noconfig = FlotillaDockerService('echo.service',
#                                         'pwagner/http-env-echo:4.0.0',
#                                         ports={80: 8080})
#     db.add_revision('testapp', FlotillaServiceRevision(label='upgraded',
#                                                        units=[v4_noconfig]))
#     db.del_revision('testapp', 'configured')
#     time.sleep(SYNC_TIME)
#
#     logger.info('Sampling upgraded configuration - 100% v4 and no message:')
#     sample_elb(elb_address, 100, 0.5)
#
#     background_thread.join()
