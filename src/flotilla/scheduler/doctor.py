import logging
import time

logger = logging.getLogger('flotilla')

SERVICE_EXPIRY = 10


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
        service_item = self._db.get_service(service)
        if not service_item:
            logger.warn('Service %s not found.', service)
            return
        if rev not in service_item:
            logger.warn('Service %s does not have revision %s.', service, rev)
            return

        logger.info('Diagnosing error of %s in %s on %s...', rev, service,
                    instance)

        # Are there any instances that _did_ load the service:
        running = self._running_instances(service, instance, rev)
        logger.info('Found %s running instances.', len(running))
        if running:
            logger.debug('Found %d running instances, verifying ELB health...',
                         len(running))

            # TODO: not so ELB-centric:
            healthy = self._healthy_instances(service_item, running)
            logger.info('Found %s healthy instances.', len(healthy))

            if healthy:
                logger.info('Diagnosis: %s is broken.', instance)
                return

        logger.info('Diagnosis: %s is broken.', rev)
        service_item[rev] *= -1
        self._db.set_services([service_item])

    def _running_instances(self, service, rev, instance):
        """
        Return other instances stably running a target revision.
        :param service: Service name.
        :param rev: Revision
        :param instance: Instance id.
        :return: Running instances.
        """
        running_instances = set()

        active_cutoff = time.time() - SERVICE_EXPIRY
        service_statuses = self._db.get_service_status(service, rev, instance)
        for instance, services_status in service_statuses:
            for status in services_status.values():
                sub_state = status['sub_state']
                active_time = status['active_enter_time']
                if sub_state == 'running' and active_time <= active_cutoff:
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
