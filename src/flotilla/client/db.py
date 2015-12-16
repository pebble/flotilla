import logging
from boto.dynamodb2.exceptions import ItemNotFound
from collections import defaultdict
from flotilla.model import FlotillaServiceRevision, FlotillaUnit, \
    GLOBAL_ASSIGNMENT

logger = logging.getLogger('flotilla')


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

    def __init__(self, assignments, regions, revisions, services, units):
        self._assignments = assignments
        self._regions = regions
        self._revisions = revisions
        self._services = services
        self._units = units

    def add_revision(self, service, revision):
        rev_hash = self._store_revision(revision)
        try:
            rev_item = self._services.get_item(service_name=service)
        except ItemNotFound:
            rev_item = self._services.new_item(service)
        rev_item[rev_hash] = revision.weight
        rev_item.partial_save()

    def _store_revision(self, revision):
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

        return rev_hash

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

    def configure_regions(self, regions, updates):
        if isinstance(regions, str):
            regions = [regions]

        # Load current items:
        region_items = {}
        keys = [{'region_name': region} for region in regions]
        for item in self._regions.batch_get(keys):
            region = item['region_name']
            region_items[region] = item

        # Create/update items:
        for region in regions:
            region_item = region_items.get(region)
            if not region_item:
                region_item = self._regions.new_item(region)
                region_items[region] = region_item
            for key, value in updates.items():
                region_item[key] = value

        # Store updated items:
        with self._regions.batch_write() as batch:
            for region_item in region_items.values():
                batch.put_item(region_item)

    def configure_service(self, service, updates):
        service_item = self._services.get_item(service_name=service)
        if not service_item:
            service_item = self._services.new_item(service_name=service)
        for key, value in updates.items():
            service_item[key] = value
        service_item.save()

    def set_global(self, revision):
        self._store_revision(revision)
        rev_hash = self._store_revision(revision)
        self._assignments.put_item({
            'instance_id': GLOBAL_ASSIGNMENT,
            'assignment': rev_hash
        })
