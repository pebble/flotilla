import logging
from boto.dynamodb2.exceptions import ItemNotFound
from collections import defaultdict
from flotilla.model import FlotillaServiceRevision, FlotillaUnit, \
    GLOBAL_ASSIGNMENT, GLOBAL_ASSIGNMENT_SHARDS
import json
from Crypto.Cipher import AES
from Crypto import Random

logger = logging.getLogger('flotilla')


def aes_pad(s):
    return s + (AES.block_size - len(s) % AES.block_size) * ' '


class FlotillaClientDynamo(object):
    """Database interaction for worker/agent component.

    Required table permissions:
    assignments:
        - PutItem
    regions:
        - BatchWriteItem
    revisions:
        - GetItem
        - PutItem
        - DeleteItem
    services:
        - GetItem
        - PutItem
        - UpdateItem

    units:
        - GetItem
        - BatchWriteItem
    """

    def __init__(self, assignments, regions, revisions, services, units, users,
                 kms):
        self._assignments = assignments
        self._regions = regions
        self._revisions = revisions
        self._services = services
        self._units = units
        self._users = users
        self._kms = kms

    def add_revision(self, service, revision):
        try:
            service_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            service_item = self._services.new_item(service)

        key = service_item.get('kms_key')
        rev_hash = self._store_revision(revision, key)

        service_item[rev_hash] = revision.weight
        service_item.partial_save()

    def _store_revision(self, revision, key):
        # Store units:
        with self._units.batch_write() as batch:
            for unit in revision.units:
                unit_hash = unit.unit_hash
                if self._units.has_item(unit_hash=unit_hash):
                    logger.debug('Unit %s exists.', unit_hash)
                    continue

                logger.debug('Adding unit %s.', unit_hash)
                unit_item = self._units.new_item(unit_hash)
                unit_item['name'] = unit.name
                unit_item['unit_file'] = unit.unit_file
                env = unit.environment
                if env:
                    if key:
                        self._encrypt_environment(key, env, unit_item)
                    else:
                        unit_item['environment'] = env
                batch.put_item(data=unit_item)

        # Link units to revision + label:
        rev_hash = revision.revision_hash
        if not self._revisions.has_item(rev_hash=rev_hash):
            rev_item = self._revisions.new_item(rev_hash)
            rev_item['label'] = revision.label
            rev_item['units'] = [unit.unit_hash for unit in revision.units]
            rev_item.save()

        return rev_hash

    def _encrypt_environment(self, key_id, environment, unit_item):
        kms_key = self._kms.generate_data_key(key_id, key_spec='AES_256')
        plaintext_key = kms_key['Plaintext']
        encrypted_key = kms_key['CiphertextBlob']

        iv = Random.new().read(AES.block_size)
        cipher = AES.new(plaintext_key, AES.MODE_CBC, iv)
        environment_json = json.dumps(environment)
        environment_encrypted = cipher.encrypt(aes_pad(environment_json))

        unit_item['environment_iv'] = iv.encode('base64')
        unit_item['environment_key'] = encrypted_key.encode('base64')
        unit_item['environment_data'] = environment_encrypted.encode('base64')

    def del_revision(self, service, rev_hash):
        try:
            service_item = self._services.get_item(service_name=service)
            if rev_hash in service_item:
                del service_item[rev_hash]
                service_item.partial_save()
        except ItemNotFound:
            logger.warning('Service %s not found, unable to delete %s', service,
                           rev_hash)

        try:
            rev_item = self._revisions.get_item(rev_hash=rev_hash)
            rev_item.delete()
        except ItemNotFound:
            logger.warning('Revision %s not found, unable to delete', rev_hash)

    def set_revision_weight(self, service, rev_hash, weight):
        try:
            service_item = self._services.get_item(service_name=service)
            service_item[rev_hash] = weight
            service_item.partial_save()
        except ItemNotFound:
            logger.warn('Service %s not found, unable to set weight of %s',
                        service, rev_hash)

    def get_revisions(self, service):
        try:
            service_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            return []

        # Select revisions and build weight-only return values:
        rev_hashes = [k for k in service_item.keys() if k != 'service_name']
        logger.debug('Found %d revisions in %s.', len(rev_hashes), service)
        flotilla_revisions = {}
        for rev_hash in rev_hashes:
            flotilla_revisions[rev_hash] = FlotillaServiceRevision(
                    weight=service_item[rev_hash])

        # Load revisions, collect units and index:
        unit_rev = defaultdict(list)
        revisions = self._revisions.batch_get(
                keys=[{'rev_hash': rev_hash} for rev_hash in rev_hashes])
        for revision in revisions:
            rev_hash = revision['rev_hash']
            for unit in revision['units']:
                unit_rev[unit].append(rev_hash)
            flotilla_revisions[rev_hash].label = revision['label']
        logger.debug('Mapped %d revisions to %s units.', len(rev_hashes),
                     len(unit_rev))

        # Load units, add to return values
        service_units = self._units.batch_get(
                keys=[{'unit_hash': unit_hash} for unit_hash in unit_rev.keys()]
        )
        for unit in service_units:
            flotilla_unit = FlotillaUnit(unit['name'], unit['unit_file'],
                                         unit['environment'])

            unit_revs = unit_rev[unit['unit_hash']]
            logger.debug('Adding to %d revisions.', len(unit_revs))
            for rev in unit_revs:
                flotilla_revisions[rev].units.append(flotilla_unit)
        return flotilla_revisions.values()

    def configure_region(self, region, updates):
        try:
            region_item = self._regions.get_item(region_name=region)
        except ItemNotFound:
            region_item = self._regions.new_item(region)
        for key, value in updates.items():
            region_item[key] = value
        region_item.save(overwrite=True)

    def configure_service(self, service, updates):
        try:
            service_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            service_item = self._services.new_item(service)
        for key, value in updates.items():
            service_item[key] = value
        service_item.save()

    def configure_user(self, username, updates):
        try:
            user_item = self._users.get_item(username=username)
        except ItemNotFound:
            user_item = self._users.new_item(username)
        for key, value in updates.items():
            user_item[key] = value
        user_item.save()

    def check_users(self, usernames):
        missing = set(usernames)
        user_items = self._users.batch_get(
                keys=[{'username': username} for username in usernames])
        for user_item in user_items:
            missing.remove(user_item['username'])
        return missing

    def set_global(self, revision):
        rev_hash = self._store_revision(revision, None)
        with self._assignments.batch_write() as batch:
            for i in range(GLOBAL_ASSIGNMENT_SHARDS):
                assignment_id = '%s_%d' % (GLOBAL_ASSIGNMENT, i)
                batch.put_item({
                    'instance_id': assignment_id,
                    'assignment': rev_hash
                }, overwrite=True)
