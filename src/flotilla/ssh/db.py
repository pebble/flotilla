from boto.dynamodb2.exceptions import ItemNotFound

SERVICE_ATTRIBUTES = ('admins',)


class FlotillaSshDynamo(object):
    def __init__(self, regions, services, users, region):
        self._regions = regions
        self._services = services
        self._users = users
        self._region = region

    def get_region_admins(self):
        try:
            region_item = self._regions.get_item(region_name=self._region)
        except ItemNotFound:
            return set()
        region_admins = region_item.get('admins', [])
        return set(region_admins)

    def get_service_admins(self, service):
        region_admins = self.get_region_admins()
        try:
            service_item = self._services.get_item(service_name=service,
                                                   attributes=SERVICE_ATTRIBUTES)
        except ItemNotFound:
            return region_admins

        service_admins = service_item.get('admins', [])
        region_admins |= set(service_admins)
        return region_admins

    def get_bastion_users(self):
        region_admins = self.get_region_admins()

        for service_item in self._services.scan(attributes=SERVICE_ATTRIBUTES,
                                                admins__null=False):
            service_admins = service_item.get('admins', [])
            region_admins |= set(service_admins)
        return region_admins

    def get_keys(self, users):
        ssh_keys = set()
        if not users:
            return ssh_keys

        user_keys = [{'username': u} for u in users]
        user_items = self._users.batch_get(keys=user_keys)
        for user_item in user_items:
            user_active = user_item.get('active', True)
            if not user_active:
                continue
            user_keys = user_item.get('ssh_keys', ())
            for ssh_key in user_keys:
                ssh_keys.add(ssh_key)
        return ssh_keys
