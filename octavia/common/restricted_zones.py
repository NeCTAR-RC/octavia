#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from keystoneclient import client as keystone_client
from oslo_log import log as logging

from octavia.common import cache_utils
from octavia.common import keystone


LOG = logging.getLogger(__name__)


AZ_CACHE_SECONDS = 60 * 60
MC = None


def _get_cache():
    global MC

    if MC is None:
        MC = cache_utils.get_client(expiration_time=AZ_CACHE_SECONDS)

    return MC


def reset_cache():
    """Reset the cache, mainly for testing purposes and update

    availability_zone for host aggregate
    """
    global MC
    MC = None


def get_restricted_zones(project_id):
    ALL_ZONES = 'ALL'
    cache = _get_cache()
    cache_key = 'restricted_zones-%s' % project_id
    zones = cache.get(cache_key)
    LOG.warn("Found cached restricted zones: %s", zones)

    if not zones:
        ksession = keystone.KeystoneSession()
        kclient = keystone_client.Client(session=ksession.get_session())
        project = kclient.projects.get(project_id)
        zones = getattr(project, 'compute_zones', ALL_ZONES)
        cache.set(cache_key, zones)

        LOG.warn("Cached restricted zones: %s", zones)
    if not zones or zones == ALL_ZONES:
        return None

    return zones.split(',')
