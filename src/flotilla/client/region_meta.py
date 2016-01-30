import logging

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger('flotilla')


class RegionMetadata(object):
    def __init__(self, environment):
        self._environment = environment

    def store_regions(self, regions, per_region, instance_type, coreos_channel,
                      coreos_version):
        region_params = {region: self._region_params(region)
                         for region in regions}

        scheduler_regions = region_params.values()
        print scheduler_regions
        if not per_region:
            scheduler_regions = (region_params[regions[0]],)
        for scheduler_region in scheduler_regions:
            scheduler_region['scheduler'] = True
            scheduler_region['scheduler_instance_type'] = instance_type
            scheduler_region['scheduler_coreos_channel'] = coreos_channel
            scheduler_region['scheduler_coreos_version'] = coreos_version
        logger.debug('Prepared %d region records', len(regions))

        region_updates = {}
        for region_name, region_param in region_params.items():
            region_updates[region_name] = {
                k: {'Value': v, 'Action': 'PUT'}
                for k, v in region_param.items()}

        table_name = 'flotilla-%s-regions' % self._environment
        for region in regions:
            # Connect to dynamo in each region:
            dynamo = boto3.resource('dynamodb', region)
            region_table = dynamo.Table(table_name)

            # Store records:
            for region_name, region_update in region_updates.items():
                region_table.update_item(Key={'region_name': region_name},
                                         AttributeUpdates=region_update)
        logger.debug('Stored %d region records', len(regions))

        return region_params

    @staticmethod
    def _region_params(region):
        region_params = {}

        ec2 = boto3.client('ec2', region)
        try:
            vpcs = ec2.describe_vpcs()
            vpc_id = vpcs['Vpcs'][0]['VpcId']
            invalid_az = region + '-zzz'
            ec2.create_subnet(VpcId=vpc_id, CidrBlock='172.31.192.0/20',
                              AvailabilityZone=invalid_az)
        except ClientError as e:
            message = e.response['Error'].get('Message')
            if not message or 'Subnets can currently only be created in ' \
                              'the following availability zones' not in message:
                raise e
            message_split = message.split(region)
            # Invalid region is echoed back, every mention after that is an AZ:
            azs = sorted([s[0] for s in message_split[2:]])
            for az_index in range(3):
                az = az_index % len(azs)
                region_params['az%d' % (az_index + 1)] = region + azs[az]
        return region_params
