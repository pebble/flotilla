import logging
import time

logger = logging.getLogger('flotilla')


class ServiceDoctor(object):
    def __init__(self, db, elb):
        self._db = db
        self._elb = elb

    def failed_revision(self, service, rev, instance):
        """
        Callback when an instance reports it failed to deploy a revision.
        :param service: Service name.
        :param rev: Failing revision.
        :param instance:  Failing instance.
        :return: None.
        """
        service_item = self._get_service_with_rev(service, rev)
        if not service_item or service_item[rev] < 0:
            # Invalid service/rev
            return

        healthy = self._healthy_instances_with_rev(service_item, rev, instance)
        if not healthy:
            logger.info('Diagnosis: %s is broken.', rev)
            service_item[rev] *= -1
            self._db.set_services([service_item])
        else:
            logger.info('Diagnosis: %s is broken.', instance)

    def is_healthy_revision(self, service, rev):
        """
        Determine if a service revision is healthy.
        :param service: Service name.
        :param rev:  Revision hash.
        :return: True if service has a healthy instance running this rev.
        """
        service_item = self._get_service_with_rev(service, rev)
        if not service_item:
            return False

        if service_item[rev] < 0:
            raise ValueError('Service has been marked as failed!')

        return self._healthy_instances_with_rev(service_item, rev, None)

    def _get_service_with_rev(self, service, rev):
        service_item = self._db.get_service(service)
        if not service_item:
            logger.warn('Service %s not found.', service)
            return None
        if rev not in service_item:
            logger.warn('Service %s does not have revision %s.', service, rev)
            return None

        return service_item

    def _healthy_instances_with_rev(self, service_item, rev, instance):
        # Are there any instances that _did_ load the service:
        running = self._running_instances(service_item['service_name'], rev,
                                          instance)
        logger.info('Found %s running instances.', len(running))
        if not running:
            return False
        logger.debug('Found %d running instances, verifying ELB health...',
                     len(running))
        # TODO: not so ELB-centric:
        healthy = self._healthy_instances(service_item, running)
        logger.info('Found %s healthy instances.', len(healthy))
        if not healthy:
            return False
        return True

    def _running_instances(self, service, rev, instance):
        """
        Return other instances stably running a target revision.
        :param service: Service name.
        :param rev: Revision
        :param instance: Instance id.
        :return: Running instances.
        """
        running_instances = set()
        service_statuses = self._db.get_service_status(service, rev, instance)
        for instance, services_status in service_statuses:
            for status in services_status.values():
                sub_state = status['sub_state']
                if sub_state == 'running':
                    running_instances.add(instance)

        return running_instances

    def _healthy_instances(self, service_item, running_instances):
        service_name = service_item['service_name']
        healthy_instances = set()

        service_outputs = service_item.get('cf_outputs')
        if not service_outputs:
            logger.warn('Service %s has no CF outputs', service_name)
            return healthy_instances

        service_elb = service_outputs.get('Elb')
        if not service_elb:
            logger.warn('Service %s has no ELB', service_name)
            return healthy_instances

        elb_health = self._elb.describe_instance_health(
                LoadBalancerName=service_elb,
                Instances=[{'InstanceId': instance_id}
                           for instance_id in running_instances]
        )
        for instance_state in elb_health.get('InstanceStates'):
            if instance_state['State'] == 'InService':
                healthy_instances.add(instance_state['InstanceId'])
        return healthy_instances
