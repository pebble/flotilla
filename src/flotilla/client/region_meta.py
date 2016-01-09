import logging

import boto.vpc
import boto.dynamodb2
from boto.dynamodb2.table import Table
from boto.exception import BotoServerError

logger = logging.getLogger('flotilla')


class RegionMetadata(object):
    def __init__(self, environment):
        self._environment = environment

    def store_regions(self, regions, per_region, instance_type, coreos_channel,
                      coreos_version):
        region_items = [self._create_region_item(region)
                        for region in regions]

        scheduler_regions = per_region and region_items or (region_items[0],)
        for scheduler_region in scheduler_regions:
            scheduler_region['scheduler'] = True
            scheduler_region['scheduler_instance_type'] = instance_type
            scheduler_region['scheduler_coreos_channel'] = coreos_channel
            scheduler_region['scheduler_coreos_version'] = coreos_version
        logger.debug('Prepared %d region records', len(regions))

        table_name = 'flotilla-%s-regions' % self._environment
        for region in regions:
            dynamo = boto.dynamodb2.connect_to_region(region)

            region_table = Table(table_name, connection=dynamo)
            with region_table.batch_write() as batch:
                for region_item in region_items:
                    batch.put_item(region_item, overwrite=True)
        logger.debug('Stored %d region records', len(regions))

        return {r['region_name']: r for r in region_items}

    @staticmethod
    def _create_region_item(region):
        region_item = {
            'region_name': region
        }

        vpc = boto.vpc.connect_to_region(region)
        try:
            vpcs = vpc.get_all_vpcs()
            invalid_az = region + '-zzz'
            vpc.create_subnet(vpcs[0].id, '172.31.192.0/20',
                              availability_zone=invalid_az)
        except BotoServerError as e:
            if 'Subnets can currently only be created in ' \
               'the following availability zones' not in e.message:
                raise e
            message_split = e.message.split(region)
            # Invalid region is echoed back, every mention after that is an AZ:
            azs = sorted([s[0] for s in message_split[2:]])
            for az_index in range(3):
                az = az_index % len(azs)
                region_item['az%d' % (az_index + 1)] = region + azs[az]
        return region_item
