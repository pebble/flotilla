"""
Convert CoreOS version to usable AMI
"""

import json
import urllib2
from urllib2 import HTTPError
import logging

logger = logging.getLogger('git-deploy')


class CoreOsAmiIndex(object):
    """Look up CoreOS AMI."""

    def __init__(self):
        self.index_cache = {}

    def get_ami(self, channel, version, region):
        """Look up AMI.
        :param channel: CoreOS channel.
        :param version:  CoreOS version.
        :param region: AWS region.
        :return: HVM AMI, None on error/not found.
        """
        index_url = ('http://%s.release.core-os.net/amd64-usr/' +
                     '%s/coreos_production_ami_all.json') % (channel, version)

        try:
            index_data = self.index_cache.get(index_url)
            if not index_data:
                ami_query = urllib2.urlopen(index_url, timeout=5)
                index_data = ami_query.read()
                self.index_cache[index_url] = index_data
            amis = json.loads(index_data)
            for ami in amis['amis']:
                if ami['name'] == region:
                    return ami['hvm']
        except HTTPError as e:
            logger.debug(e)
        return None
