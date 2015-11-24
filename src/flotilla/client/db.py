import logging
from boto.dynamodb2.exceptions import ItemNotFound
from collections import defaultdict
from flotilla.model import FlotillaServiceRevision, FlotillaUnit

logger = logging.getLogger('flotilla')


class FlotillaClientDynamo(object):
    """Database interaction for worker/agent component.

    Required table permissions:
    services:
        - GetItem
        - PutItem
        - UpdateItem
    revisions:
        - GetItem
        - PutItem
        - DeleteItem
    units:
        - GetItem
        - BatchWriteItem
    """

    def __init__(self, units_table, revisions_table, services_table):
        self._units = units_table
        self._revisions = revisions_table
        self._services = services_table

    def add_revision(self, service, revision):
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
                if unit.environment:
                    # TODO: KMS encrypt
                    unit_item['environment'] = unit.environment
                batch.put_item(data=unit_item)

        # Link units to revision + label:
        rev_hash = revision.revision_hash
        if not self._revisions.has_item(rev_hash=rev_hash):
            rev_item = self._revisions.new_item(rev_hash)
            rev_item['label'] = revision.label
            rev_item['units'] = [unit.unit_hash for unit in revision.units]
            rev_item.save()

        # Link revision to service + weight:
        try:
            rev_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            rev_item = self._services.new_item(service)
        rev_item[rev_hash] = revision.weight
        rev_item.partial_save()

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
            for unit_rev in unit_revs:
                flotilla_revisions[unit_rev].units.append(flotilla_unit)
        return flotilla_revisions.values()
