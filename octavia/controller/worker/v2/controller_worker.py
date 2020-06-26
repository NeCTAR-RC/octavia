# Copyright 2015 Hewlett-Packard Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#


from oslo_config import cfg
from oslo_log import log as logging
from oslo_utils import excutils
from sqlalchemy.orm import exc as db_exceptions
from taskflow.listeners import logging as tf_logging
import tenacity

from octavia.amphorae.driver_exceptions import exceptions
from octavia.api.drivers import utils as provider_utils
from octavia.common import base_taskflow
from octavia.common import constants
from octavia.controller.worker.v2.flows import amphora_flows
from octavia.controller.worker.v2.flows import health_monitor_flows
from octavia.controller.worker.v2.flows import l7policy_flows
from octavia.controller.worker.v2.flows import l7rule_flows
from octavia.controller.worker.v2.flows import listener_flows
from octavia.controller.worker.v2.flows import load_balancer_flows
from octavia.controller.worker.v2.flows import member_flows
from octavia.controller.worker.v2.flows import pool_flows
from octavia.db import api as db_apis
from octavia.db import repositories as repo

CONF = cfg.CONF
LOG = logging.getLogger(__name__)

RETRY_ATTEMPTS = 15
RETRY_INITIAL_DELAY = 1
RETRY_BACKOFF = 1
RETRY_MAX = 5


# We do not need to log retry exception information. Warning "Could not connect
#  to instance" will be logged as usual.
def retryMaskFilter(record):
    if record.exc_info is not None and isinstance(
            record.exc_info[1], exceptions.AmpConnectionRetry):
        return False
    return True

LOG.logger.addFilter(retryMaskFilter)


def _is_provisioning_status_pending_update(lb_obj):
    return not lb_obj.provisioning_status == constants.PENDING_UPDATE


class ControllerWorker(base_taskflow.BaseTaskFlowEngine):

    def __init__(self):

        self._amphora_flows = amphora_flows.AmphoraFlows()
        self._health_monitor_flows = health_monitor_flows.HealthMonitorFlows()
        self._lb_flows = load_balancer_flows.LoadBalancerFlows()
        self._listener_flows = listener_flows.ListenerFlows()
        self._member_flows = member_flows.MemberFlows()
        self._pool_flows = pool_flows.PoolFlows()
        self._l7policy_flows = l7policy_flows.L7PolicyFlows()
        self._l7rule_flows = l7rule_flows.L7RuleFlows()

        self._amphora_repo = repo.AmphoraRepository()
        self._amphora_health_repo = repo.AmphoraHealthRepository()
        self._health_mon_repo = repo.HealthMonitorRepository()
        self._lb_repo = repo.LoadBalancerRepository()
        self._listener_repo = repo.ListenerRepository()
        self._member_repo = repo.MemberRepository()
        self._pool_repo = repo.PoolRepository()
        self._l7policy_repo = repo.L7PolicyRepository()
        self._l7rule_repo = repo.L7RuleRepository()
        self._flavor_repo = repo.FlavorRepository()
        self._az_repo = repo.AvailabilityZoneRepository()

        super(ControllerWorker, self).__init__()

    @tenacity.retry(
        retry=(
            tenacity.retry_if_result(_is_provisioning_status_pending_update) |
            tenacity.retry_if_exception_type()),
        wait=tenacity.wait_incrementing(
            RETRY_INITIAL_DELAY, RETRY_BACKOFF, RETRY_MAX),
        stop=tenacity.stop_after_attempt(RETRY_ATTEMPTS))
    def _get_db_obj_until_pending_update(self, repo, id):

        return repo.get(db_apis.get_session(), id=id)

    def create_amphora(self, availability_zone=None):
        """Creates an Amphora.

        This is used to create spare amphora.

        :returns: amphora_id
        """
        try:
            store = {constants.BUILD_TYPE_PRIORITY:
                     constants.LB_CREATE_SPARES_POOL_PRIORITY,
                     constants.FLAVOR: None,
                     constants.AVAILABILITY_ZONE: None}
            if availability_zone:
                store[constants.AVAILABILITY_ZONE] = (
                    self._az_repo.get_availability_zone_metadata_dict(
                        db_apis.get_session(), availability_zone))
            create_amp_tf = self._taskflow_load(
                self._amphora_flows.get_create_amphora_flow(),
                store=store)
            with tf_logging.DynamicLoggingListener(create_amp_tf, log=LOG):
                create_amp_tf.run()

            return create_amp_tf.storage.fetch('amphora')
        except Exception as e:
            LOG.error('Failed to create an amphora due to: {}'.format(str(e)))

    def delete_amphora(self, amphora_id):
        """Deletes an existing Amphora.

        :param amphora_id: ID of the amphora to delete
        :returns: None
        :raises AmphoraNotFound: The referenced Amphora was not found
        """
        amphora = self._amphora_repo.get(db_apis.get_session(),
                                         id=amphora_id)
        delete_amp_tf = self._taskflow_load(
            self._amphora_flows.get_delete_amphora_flow(),
            store={constants.AMPHORA: amphora.to_dict()})
        with tf_logging.DynamicLoggingListener(delete_amp_tf,
                                               log=LOG):
            delete_amp_tf.run()

    def create_health_monitor(self, health_monitor):
        """Creates a health monitor.

        :param health_monitor: Provider health monitor dict
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        db_health_monitor = self._health_mon_repo.get(
            db_apis.get_session(),
            id=health_monitor[constants.HEALTHMONITOR_ID])

        pool = db_health_monitor.pool
        pool.health_monitor = db_health_monitor
        load_balancer = pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                pool.listeners))

        create_hm_tf = self._taskflow_load(
            self._health_monitor_flows.get_create_health_monitor_flow(),
            store={constants.HEALTH_MON: health_monitor,
                   constants.POOL_ID: pool.id,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.LOADBALANCER: provider_lb})
        with tf_logging.DynamicLoggingListener(create_hm_tf,
                                               log=LOG):
            create_hm_tf.run()

    def delete_health_monitor(self, health_monitor):
        """Deletes a health monitor.

        :param health_monitor: Provider health monitor dict
        :returns: None
        :raises HMNotFound: The referenced health monitor was not found
        """
        db_health_monitor = self._health_mon_repo.get(
            db_apis.get_session(),
            id=health_monitor[constants.HEALTHMONITOR_ID])

        pool = db_health_monitor.pool
        load_balancer = pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                pool.listeners))

        delete_hm_tf = self._taskflow_load(
            self._health_monitor_flows.get_delete_health_monitor_flow(),
            store={constants.HEALTH_MON: health_monitor,
                   constants.POOL_ID: pool.id,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.LOADBALANCER: provider_lb,
                   constants.PROJECT_ID: load_balancer.project_id})
        with tf_logging.DynamicLoggingListener(delete_hm_tf,
                                               log=LOG):
            delete_hm_tf.run()

    def update_health_monitor(self, original_health_monitor,
                              health_monitor_updates):
        """Updates a health monitor.

        :param original_health_monitor: Provider health monitor dict
        :param health_monitor_updates: Dict containing updated health monitor
        :returns: None
        :raises HMNotFound: The referenced health monitor was not found
        """
        try:
            db_health_monitor = self._get_db_obj_until_pending_update(
                self._health_mon_repo,
                original_health_monitor[constants.HEALTHMONITOR_ID])
        except tenacity.RetryError as e:
            LOG.warning('Health monitor did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            db_health_monitor = e.last_attempt.result()

        pool = db_health_monitor.pool

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                pool.listeners))

        load_balancer = pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        update_hm_tf = self._taskflow_load(
            self._health_monitor_flows.get_update_health_monitor_flow(),
            store={constants.HEALTH_MON: original_health_monitor,
                   constants.POOL_ID: pool.id,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.LOADBALANCER: provider_lb,
                   constants.UPDATE_DICT: health_monitor_updates})
        with tf_logging.DynamicLoggingListener(update_hm_tf,
                                               log=LOG):
            update_hm_tf.run()

    def create_listener(self, listener):
        """Creates a listener.

        :param listener: A listener provider dictionary.
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        db_listener = self._listener_repo.get(
            db_apis.get_session(), id=listener[constants.LISTENER_ID])
        if not db_listener:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'listener',
                        listener[constants.LISTENER_ID])
            raise db_exceptions.NoResultFound

        load_balancer = db_listener.load_balancer
        listeners = load_balancer.listeners
        dict_listeners = []
        for li in listeners:
            dict_listeners.append(
                provider_utils.db_listener_to_provider_listener(li).to_dict())
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        create_listener_tf = self._taskflow_load(
            self._listener_flows.get_create_listener_flow(),
            store={constants.LISTENERS: dict_listeners,
                   constants.LOADBALANCER: provider_lb,
                   constants.LOADBALANCER_ID: load_balancer.id})
        with tf_logging.DynamicLoggingListener(create_listener_tf,
                                               log=LOG):
            create_listener_tf.run()

    def delete_listener(self, listener):
        """Deletes a listener.

        :param listener: A listener provider dictionary to delete
        :returns: None
        :raises ListenerNotFound: The referenced listener was not found
        """
        # TODO(johnsom) Remove once the provider data model includes
        #               the project ID
        lb = self._lb_repo.get(db_apis.get_session(),
                               id=listener[constants.LOADBALANCER_ID])
        delete_listener_tf = self._taskflow_load(
            self._listener_flows.get_delete_listener_flow(),
            store={constants.LISTENER: listener,
                   constants.LOADBALANCER_ID:
                       listener[constants.LOADBALANCER_ID],
                   constants.PROJECT_ID: lb.project_id})
        with tf_logging.DynamicLoggingListener(delete_listener_tf,
                                               log=LOG):
            delete_listener_tf.run()

    def update_listener(self, listener, listener_updates):
        """Updates a listener.

        :param listener: A listener provider dictionary to update
        :param listener_updates: Dict containing updated listener attributes
        :returns: None
        :raises ListenerNotFound: The referenced listener was not found
        """
        db_lb = self._lb_repo.get(db_apis.get_session(),
                                  id=listener[constants.LOADBALANCER_ID])
        update_listener_tf = self._taskflow_load(
            self._listener_flows.get_update_listener_flow(),
            store={constants.LISTENER: listener,
                   constants.UPDATE_DICT: listener_updates,
                   constants.LOADBALANCER_ID: db_lb.id,
                   constants.LISTENERS: [listener]})
        with tf_logging.DynamicLoggingListener(update_listener_tf, log=LOG):
            update_listener_tf.run()

    @tenacity.retry(
        retry=tenacity.retry_if_exception_type(db_exceptions.NoResultFound),
        wait=tenacity.wait_incrementing(
            RETRY_INITIAL_DELAY, RETRY_BACKOFF, RETRY_MAX),
        stop=tenacity.stop_after_attempt(RETRY_ATTEMPTS))
    def create_load_balancer(self, loadbalancer, flavor=None,
                             availability_zone=None):
        """Creates a load balancer by allocating Amphorae.

        First tries to allocate an existing Amphora in READY state.
        If none are available it will attempt to build one specifically
        for this load balancer.

        :param loadbalancer: The dict of load balancer to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        lb = self._lb_repo.get(db_apis.get_session(),
                               id=loadbalancer[constants.LOADBALANCER_ID])
        if not lb:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'load_balancer',
                        loadbalancer[constants.LOADBALANCER_ID])
            raise db_exceptions.NoResultFound

        # TODO(johnsom) convert this to octavia_lib constant flavor
        # once octavia is transitioned to use octavia_lib
        store = {constants.LOADBALANCER_ID:
                 loadbalancer[constants.LOADBALANCER_ID],
                 constants.BUILD_TYPE_PRIORITY:
                 constants.LB_CREATE_NORMAL_PRIORITY,
                 constants.FLAVOR: flavor,
                 constants.AVAILABILITY_ZONE: availability_zone}

        topology = lb.topology
        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                lb.listeners)
        )

        store[constants.UPDATE_DICT] = {
            constants.TOPOLOGY: topology
        }

        create_lb_flow = self._lb_flows.get_create_load_balancer_flow(
            topology=topology, listeners=listeners_dicts)

        create_lb_tf = self._taskflow_load(create_lb_flow, store=store)
        with tf_logging.DynamicLoggingListener(create_lb_tf, log=LOG):
            create_lb_tf.run()

    def delete_load_balancer(self, load_balancer, cascade=False):
        """Deletes a load balancer by de-allocating Amphorae.

        :param load_balancer: Dict of the load balancer to delete
        :returns: None
        :raises LBNotFound: The referenced load balancer was not found
        """
        db_lb = self._lb_repo.get(db_apis.get_session(),
                                  id=load_balancer[constants.LOADBALANCER_ID])
        store = {}

        if cascade:
            flow = self._lb_flows.get_cascade_delete_load_balancer_flow(
                load_balancer)
            store.update(self._lb_flows.get_delete_pools_store(db_lb))
            store.update(self._lb_flows.get_delete_listeners_store(db_lb))
        else:
            flow = self._lb_flows.get_delete_load_balancer_flow(
                load_balancer)
        store.update({constants.LOADBALANCER: load_balancer,
                      constants.SERVER_GROUP_ID: db_lb.server_group_id,
                      constants.PROJECT_ID: db_lb.project_id})

        delete_lb_tf = self._taskflow_load(flow, store=store)

        with tf_logging.DynamicLoggingListener(delete_lb_tf,
                                               log=LOG):
            delete_lb_tf.run()

    def update_load_balancer(self, original_load_balancer,
                             load_balancer_updates):
        """Updates a load balancer.

        :param original_load_balancer: Dict of the load balancer to update
        :param load_balancer_updates: Dict containing updated load balancer
        :returns: None
        :raises LBNotFound: The referenced load balancer was not found
        """

        update_lb_tf = self._taskflow_load(
            self._lb_flows.get_update_load_balancer_flow(),
            store={constants.LOADBALANCER: original_load_balancer,
                   constants.LOADBALANCER_ID:
                       original_load_balancer[constants.LOADBALANCER_ID],
                   constants.UPDATE_DICT: load_balancer_updates})

        with tf_logging.DynamicLoggingListener(update_lb_tf,
                                               log=LOG):
            update_lb_tf.run()

    def create_member(self, member):
        """Creates a pool member.

        :param member: A member provider dictionary to create
        :returns: None
        :raises NoSuitablePool: Unable to find the node pool
        """
        pool = self._pool_repo.get(db_apis.get_session(),
                                   id=member[constants.POOL_ID])
        load_balancer = pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                pool.listeners))

        create_member_tf = self._taskflow_load(
            self._member_flows.get_create_member_flow(),
            store={constants.MEMBER: member,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.LOADBALANCER: provider_lb,
                   constants.POOL_ID: pool.id})
        with tf_logging.DynamicLoggingListener(create_member_tf,
                                               log=LOG):
            create_member_tf.run()

    def delete_member(self, member):
        """Deletes a pool member.

        :param member: A member provider dictionary to delete
        :returns: None
        :raises MemberNotFound: The referenced member was not found
        """
        pool = self._pool_repo.get(db_apis.get_session(),
                                   id=member[constants.POOL_ID])

        load_balancer = pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                pool.listeners))

        delete_member_tf = self._taskflow_load(
            self._member_flows.get_delete_member_flow(),
            store={constants.MEMBER: member,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER: provider_lb,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.POOL_ID: pool.id,
                   constants.PROJECT_ID: load_balancer.project_id
                   }

        )
        with tf_logging.DynamicLoggingListener(delete_member_tf,
                                               log=LOG):
            delete_member_tf.run()

    def batch_update_members(self, old_members, new_members,
                             updated_members):
        updated_members = [
            (provider_utils.db_member_to_provider_member(
                self._member_repo.get(db_apis.get_session(),
                                      id=m.get(constants.ID))).to_dict(),
             m)
            for m in updated_members]
        provider_old_members = [
            provider_utils.db_member_to_provider_member(
                self._member_repo.get(db_apis.get_session(),
                                      id=m.get(constants.ID))).to_dict()
            for m in old_members]
        if old_members:
            pool = self._pool_repo.get(db_apis.get_session(),
                                       id=old_members[0][constants.POOL_ID])
        elif new_members:
            pool = self._pool_repo.get(db_apis.get_session(),
                                       id=new_members[0][constants.POOL_ID])
        else:
            pool = self._pool_repo.get(
                db_apis.get_session(),
                id=updated_members[0][0][constants.POOL_ID])
        load_balancer = pool.load_balancer

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                pool.listeners))
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        batch_update_members_tf = self._taskflow_load(
            self._member_flows.get_batch_update_members_flow(
                provider_old_members, new_members, updated_members),
            store={constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER: provider_lb,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.POOL_ID: pool.id,
                   constants.PROJECT_ID: load_balancer.project_id})
        with tf_logging.DynamicLoggingListener(batch_update_members_tf,
                                               log=LOG):
            batch_update_members_tf.run()

    def update_member(self, member, member_updates):
        """Updates a pool member.

        :param member_id: A member provider dictionary  to update
        :param member_updates: Dict containing updated member attributes
        :returns: None
        :raises MemberNotFound: The referenced member was not found
        """
        # TODO(ataraday) when other flows will use dicts - revisit this
        pool = self._pool_repo.get(db_apis.get_session(),
                                   id=member[constants.POOL_ID])
        load_balancer = pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                pool.listeners))

        update_member_tf = self._taskflow_load(
            self._member_flows.get_update_member_flow(),
            store={constants.MEMBER: member,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER: provider_lb,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.POOL_ID: pool.id,
                   constants.UPDATE_DICT: member_updates})
        with tf_logging.DynamicLoggingListener(update_member_tf,
                                               log=LOG):
            update_member_tf.run()

    def create_pool(self, pool):
        """Creates a node pool.

        :param pool: Provider pool dict to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """

        # TODO(ataraday) It seems we need to get db pool here anyway to get
        # proper listeners
        db_pool = self._pool_repo.get(db_apis.get_session(),
                                      id=pool[constants.POOL_ID])
        if not db_pool:
            LOG.warning('Failed to fetch %s %s from DB. Retrying for up to '
                        '60 seconds.', 'pool', pool[constants.POOL_ID])
            raise db_exceptions.NoResultFound

        load_balancer = db_pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                db_pool.listeners))

        create_pool_tf = self._taskflow_load(
            self._pool_flows.get_create_pool_flow(),
            store={constants.POOL_ID: pool[constants.POOL_ID],
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.LOADBALANCER: provider_lb})
        with tf_logging.DynamicLoggingListener(create_pool_tf,
                                               log=LOG):
            create_pool_tf.run()

    def delete_pool(self, pool):
        """Deletes a node pool.

        :param pool: Provider pool dict to delete
        :returns: None
        :raises PoolNotFound: The referenced pool was not found
        """
        db_pool = self._pool_repo.get(db_apis.get_session(),
                                      id=pool[constants.POOL_ID])

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                db_pool.listeners))
        load_balancer = db_pool.load_balancer

        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        delete_pool_tf = self._taskflow_load(
            self._pool_flows.get_delete_pool_flow(),
            store={constants.POOL_ID: pool[constants.POOL_ID],
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER: provider_lb,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.PROJECT_ID: db_pool.project_id})
        with tf_logging.DynamicLoggingListener(delete_pool_tf,
                                               log=LOG):
            delete_pool_tf.run()

    def update_pool(self, origin_pool, pool_updates):
        """Updates a node pool.

        :param origin_pool: Provider pool dict to update
        :param pool_updates: Dict containing updated pool attributes
        :returns: None
        :raises PoolNotFound: The referenced pool was not found
        """
        try:
            db_pool = self._get_db_obj_until_pending_update(
                self._pool_repo, origin_pool[constants.POOL_ID])
        except tenacity.RetryError as e:
            LOG.warning('Pool did not go into %s in 60 seconds. '
                        'This either due to an in-progress Octavia upgrade '
                        'or an overloaded and failing database. Assuming '
                        'an upgrade is in progress and continuing.',
                        constants.PENDING_UPDATE)
            db_pool = e.last_attempt.result()

        load_balancer = db_pool.load_balancer
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            load_balancer).to_dict()

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                db_pool.listeners))

        update_pool_tf = self._taskflow_load(
            self._pool_flows.get_update_pool_flow(),
            store={constants.POOL_ID: db_pool.id,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER: provider_lb,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.UPDATE_DICT: pool_updates})
        with tf_logging.DynamicLoggingListener(update_pool_tf,
                                               log=LOG):
            update_pool_tf.run()

    def create_l7policy(self, l7policy):
        """Creates an L7 Policy.

        :param l7policy: Provider dict of the l7policy to create
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        db_listener = self._listener_repo.get(
            db_apis.get_session(), id=l7policy[constants.LISTENER_ID])

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                [db_listener]))

        create_l7policy_tf = self._taskflow_load(
            self._l7policy_flows.get_create_l7policy_flow(),
            store={constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: db_listener.load_balancer.id
                   })
        with tf_logging.DynamicLoggingListener(create_l7policy_tf,
                                               log=LOG):
            create_l7policy_tf.run()

    def delete_l7policy(self, l7policy):
        """Deletes an L7 policy.

        :param l7policy: Provider dict of the l7policy to delete
        :returns: None
        :raises L7PolicyNotFound: The referenced l7policy was not found
        """
        db_listener = self._listener_repo.get(
            db_apis.get_session(), id=l7policy[constants.LISTENER_ID])
        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                [db_listener]))

        delete_l7policy_tf = self._taskflow_load(
            self._l7policy_flows.get_delete_l7policy_flow(),
            store={constants.L7POLICY: l7policy,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: db_listener.load_balancer.id
                   })
        with tf_logging.DynamicLoggingListener(delete_l7policy_tf,
                                               log=LOG):
            delete_l7policy_tf.run()

    def update_l7policy(self, original_l7policy, l7policy_updates):
        """Updates an L7 policy.

        :param l7policy: Provider dict of the l7policy to update
        :param l7policy_updates: Dict containing updated l7policy attributes
        :returns: None
        :raises L7PolicyNotFound: The referenced l7policy was not found
        """
        db_listener = self._listener_repo.get(
            db_apis.get_session(), id=original_l7policy[constants.LISTENER_ID])

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                [db_listener]))

        update_l7policy_tf = self._taskflow_load(
            self._l7policy_flows.get_update_l7policy_flow(),
            store={constants.L7POLICY: original_l7policy,
                   constants.LISTENERS: listeners_dicts,
                   constants.LOADBALANCER_ID: db_listener.load_balancer.id,
                   constants.UPDATE_DICT: l7policy_updates})
        with tf_logging.DynamicLoggingListener(update_l7policy_tf,
                                               log=LOG):
            update_l7policy_tf.run()

    def create_l7rule(self, l7rule):
        """Creates an L7 Rule.

        :param l7rule: Provider dict l7rule
        :returns: None
        :raises NoResultFound: Unable to find the object
        """
        db_l7policy = self._l7policy_repo.get(db_apis.get_session(),
                                              id=l7rule[constants.L7POLICY_ID])

        load_balancer = db_l7policy.listener.load_balancer

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                [db_l7policy.listener]))
        l7policy_dict = provider_utils.db_l7policy_to_provider_l7policy(
            db_l7policy)

        create_l7rule_tf = self._taskflow_load(
            self._l7rule_flows.get_create_l7rule_flow(),
            store={constants.L7RULE: l7rule,
                   constants.L7POLICY: l7policy_dict.to_dict(),
                   constants.LISTENERS: listeners_dicts,
                   constants.L7POLICY_ID: db_l7policy.id,
                   constants.LOADBALANCER_ID: load_balancer.id
                   })
        with tf_logging.DynamicLoggingListener(create_l7rule_tf,
                                               log=LOG):
            create_l7rule_tf.run()

    def delete_l7rule(self, l7rule):
        """Deletes an L7 rule.

        :param l7rule: Provider dict of the l7rule to delete
        :returns: None
        :raises L7RuleNotFound: The referenced l7rule was not found
        """
        db_l7policy = self._l7policy_repo.get(db_apis.get_session(),
                                              id=l7rule[constants.L7POLICY_ID])
        l7policy = provider_utils.db_l7policy_to_provider_l7policy(db_l7policy)
        load_balancer = db_l7policy.listener.load_balancer

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                [db_l7policy.listener]))

        delete_l7rule_tf = self._taskflow_load(
            self._l7rule_flows.get_delete_l7rule_flow(),
            store={constants.L7RULE: l7rule,
                   constants.L7POLICY: l7policy.to_dict(),
                   constants.LISTENERS: listeners_dicts,
                   constants.L7POLICY_ID: db_l7policy.id,
                   constants.LOADBALANCER_ID: load_balancer.id
                   })
        with tf_logging.DynamicLoggingListener(delete_l7rule_tf,
                                               log=LOG):
            delete_l7rule_tf.run()

    def update_l7rule(self, original_l7rule, l7rule_updates):
        """Updates an L7 rule.

        :param l7rule: Origin dict of the l7rule to update
        :param l7rule_updates: Dict containing updated l7rule attributes
        :returns: None
        :raises L7RuleNotFound: The referenced l7rule was not found
        """
        db_l7policy = self._l7policy_repo.get(
            db_apis.get_session(), id=original_l7rule[constants.L7POLICY_ID])
        load_balancer = db_l7policy.listener.load_balancer

        listeners_dicts = (
            provider_utils.db_listeners_to_provider_dicts_list_of_dicts(
                [db_l7policy.listener]))
        l7policy_dict = provider_utils.db_l7policy_to_provider_l7policy(
            db_l7policy)

        update_l7rule_tf = self._taskflow_load(
            self._l7rule_flows.get_update_l7rule_flow(),
            store={constants.L7RULE: original_l7rule,
                   constants.L7POLICY: l7policy_dict.to_dict(),
                   constants.LISTENERS: listeners_dicts,
                   constants.L7POLICY_ID: db_l7policy.id,
                   constants.LOADBALANCER_ID: load_balancer.id,
                   constants.UPDATE_DICT: l7rule_updates})
        with tf_logging.DynamicLoggingListener(update_l7rule_tf,
                                               log=LOG):
            update_l7rule_tf.run()

    def _perform_amphora_failover(self, amp, priority):
        """Internal method to perform failover operations for an amphora.

        :param amp: The amphora to failover
        :param priority: The create priority
        :returns: None
        """
        stored_params = {constants.FAILED_AMPHORA: amp.to_dict(),
                         constants.LOADBALANCER_ID: amp.load_balancer_id,
                         constants.BUILD_TYPE_PRIORITY: priority, }

        if amp.role in (constants.ROLE_MASTER, constants.ROLE_BACKUP):
            amp_role = 'master_or_backup'
        elif amp.role == constants.ROLE_STANDALONE:
            amp_role = 'standalone'
        elif amp.role is None:
            amp_role = 'spare'
        else:
            amp_role = 'undefined'

        LOG.info("Perform failover for an amphora: %s",
                 {"id": amp.id,
                  "load_balancer_id": amp.load_balancer_id,
                  "lb_network_ip": amp.lb_network_ip,
                  "compute_id": amp.compute_id,
                  "role": amp_role})

        if amp.status == constants.DELETED:
            LOG.warning('Amphora %s is marked DELETED in the database but '
                        'was submitted for failover. Deleting it from the '
                        'amphora health table to exclude it from health '
                        'checks and skipping the failover.', amp.id)
            self._amphora_health_repo.delete(db_apis.get_session(),
                                             amphora_id=amp.id)
            return

        if (CONF.house_keeping.spare_amphora_pool_size == 0) and (
                CONF.nova.enable_anti_affinity is False):
            LOG.warning("Failing over amphora with no spares pool may "
                        "cause delays in failover times while a new "
                        "amphora instance boots.")

        # if we run with anti-affinity we need to set the server group
        # as well
        lb = self._amphora_repo.get_lb_for_amphora(
            db_apis.get_session(), amp.id)
        provider_lb = provider_utils.db_loadbalancer_to_provider_loadbalancer(
            lb).to_dict() if lb else lb
        if CONF.nova.enable_anti_affinity and lb:
            stored_params[constants.SERVER_GROUP_ID] = lb.server_group_id
        if lb and lb.flavor_id:
            stored_params[constants.FLAVOR] = (
                self._flavor_repo.get_flavor_metadata_dict(
                    db_apis.get_session(), lb.flavor_id))
        else:
            stored_params[constants.FLAVOR] = {}
        if lb and lb.availability_zone:
            stored_params[constants.AVAILABILITY_ZONE] = (
                self._az_repo.get_availability_zone_metadata_dict(
                    db_apis.get_session(), lb.availability_zone))
        else:
            stored_params[constants.AVAILABILITY_ZONE] = {}

        failover_amphora_tf = self._taskflow_load(
            self._amphora_flows.get_failover_flow(
                role=amp.role, load_balancer=provider_lb),
            store=stored_params)

        with tf_logging.DynamicLoggingListener(failover_amphora_tf, log=LOG):
            failover_amphora_tf.run()

        LOG.info("Successfully completed the failover for an amphora: %s",
                 {"id": amp.id,
                  "load_balancer_id": amp.load_balancer_id,
                  "lb_network_ip": amp.lb_network_ip,
                  "compute_id": amp.compute_id,
                  "role": amp_role})

    def failover_amphora(self, amphora_id):
        """Perform failover operations for an amphora.

        :param amphora_id: ID for amphora to failover
        :returns: None
        :raises AmphoraNotFound: The referenced amphora was not found
        """
        try:
            amp = self._amphora_repo.get(db_apis.get_session(),
                                         id=amphora_id)
            if not amp:
                LOG.warning("Could not fetch Amphora %s from DB, ignoring "
                            "failover request.", amphora_id)
                return
            self._perform_amphora_failover(
                amp, constants.LB_CREATE_FAILOVER_PRIORITY)
            if amp.load_balancer_id:
                LOG.info("Mark ACTIVE in DB for load balancer id: %s",
                         amp.load_balancer_id)
                self._lb_repo.update(
                    db_apis.get_session(), amp.load_balancer_id,
                    provisioning_status=constants.ACTIVE)
        except Exception as e:
            try:
                self._lb_repo.update(
                    db_apis.get_session(), amp.load_balancer_id,
                    provisioning_status=constants.ERROR)
            except Exception:
                LOG.error("Unable to revert LB status to ERROR.")
            with excutils.save_and_reraise_exception():
                LOG.error("Amphora %(id)s failover exception: %(exc)s",
                          {'id': amphora_id, 'exc': e})

    def failover_loadbalancer(self, load_balancer_id):
        """Perform failover operations for a load balancer.

        :param load_balancer_id: ID for load balancer to failover
        :returns: None
        :raises LBNotFound: The referenced load balancer was not found
        """

        # Note: This expects that the load balancer is already in
        #       provisioning_status=PENDING_UPDATE state
        try:
            lb = self._lb_repo.get(db_apis.get_session(),
                                   id=load_balancer_id)

            # Exclude amphora already deleted
            amps = [a for a in lb.amphorae if a.status != constants.DELETED]
            for amp in amps:
                # failover amphora in backup role
                # Note: this amp may not currently be the backup
                # TODO(johnsom) Change this to query the amp state
                #               once the amp API supports it.
                if amp.role == constants.ROLE_BACKUP:
                    self._perform_amphora_failover(
                        amp, constants.LB_CREATE_ADMIN_FAILOVER_PRIORITY)

            for amp in amps:
                # failover everyhting else
                if amp.role != constants.ROLE_BACKUP:
                    self._perform_amphora_failover(
                        amp, constants.LB_CREATE_ADMIN_FAILOVER_PRIORITY)

            self._lb_repo.update(
                db_apis.get_session(), load_balancer_id,
                provisioning_status=constants.ACTIVE)

        except Exception as e:
            with excutils.save_and_reraise_exception():
                LOG.error("LB %(lbid)s failover exception: %(exc)s",
                          {'lbid': load_balancer_id, 'exc': e})
                self._lb_repo.update(
                    db_apis.get_session(), load_balancer_id,
                    provisioning_status=constants.ERROR)

    def amphora_cert_rotation(self, amphora_id):
        """Perform cert rotation for an amphora.

        :param amphora_id: ID for amphora to rotate
        :returns: None
        :raises AmphoraNotFound: The referenced amphora was not found
        """

        amp = self._amphora_repo.get(db_apis.get_session(),
                                     id=amphora_id)
        LOG.info("Start amphora cert rotation, amphora's id is: %s", amp.id)

        certrotation_amphora_tf = self._taskflow_load(
            self._amphora_flows.cert_rotate_amphora_flow(),
            store={constants.AMPHORA: amp.to_dict(),
                   constants.AMPHORA_ID: amphora_id})

        with tf_logging.DynamicLoggingListener(certrotation_amphora_tf,
                                               log=LOG):
            certrotation_amphora_tf.run()

    def update_amphora_agent_config(self, amphora_id):
        """Update the amphora agent configuration.

        Note: This will update the amphora agent configuration file and
              update the running configuration for mutatable configuration
              items.

        :param amphora_id: ID of the amphora to update.
        :returns: None
        """
        LOG.info("Start amphora agent configuration update, amphora's id "
                 "is: %s", amphora_id)
        amp = self._amphora_repo.get(db_apis.get_session(), id=amphora_id)
        lb = self._amphora_repo.get_lb_for_amphora(db_apis.get_session(),
                                                   amphora_id)
        flavor = {}
        if lb.flavor_id:
            flavor = self._flavor_repo.get_flavor_metadata_dict(
                db_apis.get_session(), lb.flavor_id)

        update_amphora_tf = self._taskflow_load(
            self._amphora_flows.update_amphora_config_flow(),
            store={constants.AMPHORA: amp.to_dict(),
                   constants.FLAVOR: flavor})

        with tf_logging.DynamicLoggingListener(update_amphora_tf,
                                               log=LOG):
            update_amphora_tf.run()
