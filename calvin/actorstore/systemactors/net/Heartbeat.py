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

import socket

from calvin.actor.actor import Actor, ActionResult, manage, condition, guard

from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class Heartbeat(Actor):
    """
    Send all incoming tokens to given address/port over UDP

    Control port takes control commands of the form (uri only applicable for connect.)

    {
        "command" : "connect"/"disconnect",
        "uri": "udp://<address>:<port>"
    }

    Outputs:
      out : Nothing
    Inputs:
      in : Nothing
    """

    @manage(['address', 'port'])
    def init(self, node, address, port, delay=1):
        self.address = address
        self.port = port
        self.node = node
        self.delay = delay
        self.timer = None
        self.sender = None
        self.listener = None
        self.nodes = []
        self.setup()

    def connect(self):
        #self.sender = self['socket'].connect(self.address, self.port, connection_type="UDP")
        self.sender = socket.socket(socket.AF_INET, # Internet
                                    socket.SOCK_DGRAM) # UDP
        self.listener = self['server'].start(self.address, self.port, "udp")

    def will_migrate(self):
        if self.sender:
            self.sender.disconnect()

    def did_migrate(self):
        self.setup()
        if self.address is not None:
            self.connect()

    def setup(self):
        self.use('calvinsys.network.socketclienthandler', shorthand='socket')
        self.use('calvinsys.network.serverhandler', shorthand='server')
        self.use('calvinsys.native.python-re', shorthand='regexp')
        self.use('calvinsys.events.timer', shorthand='timer')
        self.timer = self['timer'].repeat(self.delay)
        self.connect()

    def register(self, node_id):
        self.nodes.append(node_id)

    def deregister(self, node_id):
        if node_id in self.nodes:
            self.nodes.remove(node_id)

    @condition(action_output=['out'])
    @guard(lambda self: self.sender and self.timer.triggered)
    def send(self):
        self.timer.ack()
        node_ids = []
        links = set(self.node.network.list_links())
        for node_id in self.nodes:
            uri = self.node.resource_manager.node_uris.get(node_id)
            if node_id not in links:
                node_ids.append(node_id)
            elif uri and not self.node.is_storage_node(node_id):
                uri = uri.replace("http://", "")
                uri = uri.replace("calvinip://", "")
                host = uri.split(":")[0]
                port = uri.split(":")[1]
                port = int(port) + 2
                _log.debug("Sending heartbeat to node {} at {}".format(node_id, (host, port)))
                self.sender.sendto(self.node.id, (host, port))
                node_ids.append(node_id)

        return ActionResult(production=(node_ids, ))

    @condition(action_input=['in'])
    def dummy(self, node_ids):
        self.node.increase_heartbeats(node_ids)
        return ActionResult(production=())

    @condition(action_output=['out'])
    @guard(lambda self: self.listener and self.listener.have_data())
    def receive(self):
        while self.listener.have_data():
            data = self.listener.data_get()
            _log.debug("Received heartbeat from node {}".format(data['data']))
            if data['data'] not in self.nodes:
                self.nodes.append(data['data'])
                self.node.peersetup([data['data']])

            self.node.clear_outgoing_heartbeat(data)
        return ActionResult(production=('',))

    # URI parsing - 0: protocol, 1: host, 2: port
    URI_REGEXP = r'([^:]+)://([^/:]*):([0-9]+)'

    def parse_uri(self, uri):
        status = False
        try:
            parsed_uri = self['regexp'].findall(self.URI_REGEXP, uri)[0]
            protocol = parsed_uri[0]
            if protocol != 'udp':
                _log.warn("Protocol '%s' not supported, assuming udp" % (protocol,))
            self.address = parsed_uri[1]
            self.port = int(parsed_uri[2])
            status = True
        except:
            _log.warn("malformed or erroneous control uri '%s'" % (uri,))
            self.address = None
            self.port = None
        return status

    action_priority = (receive, send, dummy)
    requires = ['calvinsys.network.socketclienthandler', 'calvinsys.native.python-re', 'calvinsys.native.python-json']
