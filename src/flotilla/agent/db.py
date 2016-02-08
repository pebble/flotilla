import logging
import json
import time
from collections import defaultdict
from flotilla.model import FlotillaServiceRevision, FlotillaUnit, \
    GLOBAL_ASSIGNMENT, GLOBAL_ASSIGNMENT_SHARDS
from Crypto.Cipher import AES

logger = logging.getLogger('flotilla')


class FlotillaAgentDynamo(object):
    """Database interaction for worker/agent component.

    Required table permissions:
    status
        -PutItem
    assignments:
        - BatchGetItem
    revisions:
        - BatchGetItem
    units:
        - BatchGetItem
    """

    def __init__(self, instance_id, service_name, status_table,
                 assignments_table, revisions_table, units_table, kms):
        self._id = instance_id
        global_shard = hash(instance_id) % GLOBAL_ASSIGNMENT_SHARDS
        self._global_id = '%s_%d' % (GLOBAL_ASSIGNMENT, global_shard)
        self._service = service_name
        self._status = status_table
        self._assignments = assignments_table
        self._revisions = revisions_table
        self._units = units_table
        self._kms = kms

    def store_status(self, unit_status):
        """Store unit status.
        :param unit_status Unit statuses.
        """
        logger.debug('Storing status as %s...', self._id)
        data = dict(unit_status)
        data['service'] = self._service
        data['instance_id'] = self._id
        data['status_time'] = time.time()
        self._status.put_item(data=data, overwrite=True)
        logger.info('Stored status of %s units as %s.', len(unit_status),
                    self._id)

    def get_assignments(self):
        assignments = self._assignments.batch_get([
            {'instance_id': self._id}, {'instance_id': self._global_id}])

        assigned_revisions = [assignment['assignment'] for assignment in
                              assignments]
        return sorted(assigned_revisions)

    def get_units(self, assigned_revisions):
        """
        Get currently assigned FlotillaUnits.
        :param assigned_revisions: Assigned revisions
        :return: Revisions.
        """

        # Fetch every revision and index units:
        revisions = {}
        unit_revisions = defaultdict(list)
        revision_keys = [{'rev_hash': assigned_revision}
                         for assigned_revision in set(assigned_revisions)]
        for revision_item in self._revisions.batch_get(revision_keys):
            rev_hash = revision_item['rev_hash']
            revision = FlotillaServiceRevision(label=revision_item['label'])
            revisions[rev_hash] = revision
            for unit in revision_item['units']:
                unit_revisions[unit].append(rev_hash)

        # Fetch every unit:
        units = []
        unit_keys = [{'unit_hash': unit_hash}
                     for unit_hash in sorted(unit_revisions.keys())]
        logger.debug('Fetching %d units for %d/%d revisions.', len(unit_keys),
                     len(revisions), len(assigned_revisions))
        for unit_item in self._units.batch_get(unit_keys):
            env_key = unit_item.get('environment_key')
            if env_key:
                decrypted_key = self._kms.decrypt(env_key.decode('base64'))
                iv = unit_item['environment_iv'].decode('base64')
                aes = AES.new(decrypted_key['Plaintext'], AES.MODE_CBC, iv)
                ciphertext = unit_item['environment_data'].decode('base64')
                plaintext = aes.decrypt(ciphertext)
                unit_environment = json.loads(plaintext)
            else:
                unit_environment = unit_item['environment']
            unit_file = unit_item['unit_file']
            unit = FlotillaUnit(unit_item['name'], unit_file, unit_environment)
            unit_hash = unit.unit_hash
            if unit_hash != unit_item['unit_hash']:
                logger.warn('Unit hash %s expected %s', unit_hash,
                            unit_item['unit_hash'])
                unit_hash = unit_item['unit_hash']

            for revision in unit_revisions[unit_hash]:
                rev_unit = FlotillaUnit(unit_item['name'], unit_file,
                                        unit_environment, rev_hash)
                units.append(rev_unit)
                revisions[revision].units.append(rev_unit)

        # Verify each revision matches expected hash:
        for expected_hash, revision in revisions.items():
            revision_hash = revision.revision_hash
            if revision_hash != expected_hash:
                # FIXME: enforce?
                logger.warn('Revision hash %s expected %s', revision_hash,
                            expected_hash)
        return units
