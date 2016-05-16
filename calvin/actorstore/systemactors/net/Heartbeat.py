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
import json
import re
from datetime import datetime

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
        is_ip = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", address)
        if not is_ip:
            address = socket.gethostbyname(address)
        self.address = address
        self.port = port
        self.node = node
        self.delay = delay
        self.in_timer = None
        self.out_timer = None
        self.sender = None
        self.listener = None
        self.nodes = set()
        self.lost_nodes = set()
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
        self.in_timer = self['timer'].repeat(self.delay)
        self.out_timer = self['timer'].repeat(self.delay)
        self.connect()

    def register(self, node_id):
        if node_id not in self.lost_nodes:
            self.nodes.add(node_id)

    def deregister(self, node_id):
        self.lost_nodes.add(node_id)
        if node_id in self.nodes:
            self.nodes.remove(node_id)

    @condition(action_output=['out'])
    @guard(lambda self: self.sender and self.nodes and self.out_timer.triggered)
    def send(self):
        print "SEND     : {}".format(datetime.now())
        self.out_timer.ack()
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
                port = int(port) + 5001
                _log.debug("Sending heartbeat to node {} at {}".format(node_id, (host, port)))
                node_ids.append(node_id)
                data = {'node_id': self.node.id, 'uri': self.node.uri}
                print "SEND TO  : {} - {}".format(datetime.now(), node_id)
                try:
                    self.sender.sendto(json.dumps(data), (host, port))
                except Exception as e:
                    _log.error("Failed to send {} heartbeat to {}: {}".format(data, (host, port), e))

        print "SEND DONE: {}".format(datetime.now())
        return ActionResult(production=(node_ids, ))

    @condition(action_input=['in'])
    def dummy(self, token):
        return ActionResult(production=())

    @condition(action_output=['out'])
    @guard(lambda self: self.listener and self.in_timer.triggered)
    def receive(self):
        print "REC      : {}".format(datetime.now())
        self.in_timer.ack()
        while self.listener.have_data():
            data = self.listener.data_get()
            _log.debug("Received heartbeat from node {}".format(data['data']))
            data = json.loads(data['data'])
            node_id = data['node_id']
            print "REC FROM : {} - {}".format(datetime.now(), node_id)
            uri = data['uri']

            self.node.clear_outgoing_heartbeat(data)
            self.node.resource_manager.register(node_id, {}, uri)
            self.register(node_id)

        print "REC DONE : {}".format(datetime.now())
        return ActionResult(production=("",))

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

    action_priority = (send, receive, dummy)
    requires = ['calvinsys.network.socketclienthandler', 'calvinsys.native.python-re', 'calvinsys.native.python-json']
