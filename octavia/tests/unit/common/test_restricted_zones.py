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

from unittest import mock

import octavia.common.restricted_zones as restricted_zones
import octavia.tests.unit.base as base


@mock.patch('octavia.common.restricted_zones.MC')
@mock.patch('octavia.common.restricted_zones.keystone_client')
class TestRestrictedZones(base.TestCase):

    def test_get_restricted_zones(self, mock_keystone, cache):
        cache.get.return_value = None
        fake_project = mock.MagicMock(spec=['name', 'id', 'compute_zones'])
        fake_project.compute_zones = 'foo,bar'
        project_id = 'fake-project-id'
        cache_key = 'restricted_zones-%s' % project_id
        client = mock_keystone.Client.return_value
        client.projects.get.return_value = fake_project

        result = restricted_zones.get_restricted_zones(project_id)

        cache.get.assert_called_once_with(cache_key)
        client.projects.get.called_once_with(project_id)
        cache.set.assert_called_once_with(cache_key,
                                          fake_project.compute_zones)
        self.assertEqual(['foo', 'bar'], result)

    def test_get_restricted_zones_cached(self, mock_keystone, cache):
        cache.get.return_value = 'foo,bar'
        project_id = 'fake-project-id'
        cache_key = 'restricted_zones-%s' % project_id
        client = mock_keystone.Client.return_value

        result = restricted_zones.get_restricted_zones(project_id)

        cache.get.assert_called_once_with(cache_key)
        client.projects.get.assert_not_called
        cache.set.assert_not_called()
        self.assertEqual(['foo', 'bar'], result)

    def test_get_restricted_zones_no_zones(self, mock_keystone, cache):
        cache.get.return_value = None
        fake_project = mock.MagicMock(spec=['name', 'id'])
        project_id = 'fake-project-id'
        cache_key = 'restricted_zones-%s' % project_id
        client = mock_keystone.Client.return_value
        client.projects.get.return_value = fake_project

        result = restricted_zones.get_restricted_zones(project_id)

        cache.get.assert_called_once_with(cache_key)
        client.projects.get.called_once_with(project_id)
        cache.set.assert_called_once_with(cache_key,
                                          restricted_zones.ALL_ZONES)
        self.assertIsNone(result)

    def test_get_restricted_zones_no_zones_cached(self, mock_keystone, cache):
        cache.get.return_value = restricted_zones.ALL_ZONES
        project_id = 'fake-project-id'
        cache_key = 'restricted_zones-%s' % project_id
        client = mock_keystone.Client.return_value

        result = restricted_zones.get_restricted_zones(project_id)

        cache.get.assert_called_once_with(cache_key)
        client.projects.get.assert_not_called
        cache.set.assert_not_called()
        self.assertIsNone(result)
