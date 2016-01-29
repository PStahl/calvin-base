# -*- coding: utf-8 -*-

# Copyright (c) 2015 Ericsson AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from calvin.runtime.north import calvin_node
from calvin.utilities import storage_node
from calvin.requests.request_handler import RequestHandler


def node_control(control_uri):
    class NodeControl(object):

        def __init__(self, control_uri):
            super(NodeControl, self).__init__()
            self._id = None
            self._uri = None
            self.control_uri = control_uri
            self.poster = RequestHandler()

        @property
        def id(self):
            if self._id is None:
                self._id = self.poster.get_node_id(self)
            return self._id

        @property
        def uri(self):
            if self._uri is None:
                self._uri = self.poster.get_node(self, self.id)["uri"]
            return self._uri

    return NodeControl(control_uri)


def dispatch_node(uri, control_uri, trace=False, attributes=None):
    p = calvin_node.start_node(uri, control_uri, trace, attributes)
    return node_control(control_uri), p


def start_node(uri, control_uri, trace=False, attributes=None):
    if trace:
        calvin_node.create_tracing_node(uri, control_uri, trace, attributes)
    else:
        calvin_node.create_node(uri, control_uri, attributes)


def dispatch_storage_node(uri, control_uri, trace=False, attributes=None):
    p = storage_node.start_node(uri, control_uri, trace, attributes)
    return node_control(control_uri), p


def start_storage_node(uri, control_uri, trace=False, attributes=None):
    if trace:
        storage_node.create_tracing_node(uri, control_uri, trace, attributes)
    else:
        storage_node.create_node(uri, control_uri, attributes)
