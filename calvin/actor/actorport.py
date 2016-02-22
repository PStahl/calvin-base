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

from calvin.utilities import calvinuuid
from calvin.runtime.north import fifo
from calvin.runtime.south.endpoint import Endpoint, Peer
from calvin.utilities.calvinlogger import get_logger
import copy

_log = get_logger(__name__)


class FifoFullException(Exception):
    pass


class Port(object):
    """docstring for Port"""

    def __init__(self, name, owner, fifo_size=10):
        super(Port, self).__init__()
        # Human readable port name
        self.name = name
        # Actor instance to which the port belongs (may change over time)
        self.owner = owner
        # Unique id to universally identify port (immutable)
        self.id = calvinuuid.uuid("PORT")
        # The token queue. Not all scenarios use it,
        # but needed when e.g. changing from local to remote connection.
        self.fifo = fifo.FIFO(fifo_size)

    def __str__(self):
        return "%s id=%s" % (self.name, self.id)

    def _state(self):
        """Return port state for serialization."""
        return {'name': self.name, 'id': self.id, 'fifo': self.fifo._state()}

    def _set_state(self, state):
        """Set port state."""
        self.name = state.pop('name')
        self.id = state.pop('id')
        self.fifo._set_state(state.pop('fifo'))

    def attach_endpoint(self, ep):
        """
        Connect port to counterpart.
        """
        raise Exception("Can't attach endpoint to  port %s.%s with id: %s" % (
            self.owner.name, self.name, self.id))

    def _detach_endpoint(self, ep):
        """
        Disconnect port from counterpart.
        """
        raise Exception("Can't detach endpoint from  port %s.%s with id: %s" % (
            self.owner.name, self.name, self.id))

    def disconnect(self, actor_id):
        """Disconnect port from counterpart. Raises an exception if port is not connected."""
        # FIXME: Implement disconnect
        raise Exception("Can't disconnect port %s.%s with id: %s" % (
            self.owner.name, self.name, self.id))


class InPort(Port):

    def __init__(self, name, owner):
        super(InPort, self).__init__(name, owner)
        self.endpoints = []

    def __str__(self):
        s = super(InPort, self).__str__()
        return s + " " + str(", ".join([str(ep) for ep in self.endpoints]))

    def is_connected(self):
        return all([ep.is_connected() for ep in self.endpoints])

    def is_connected_to(self, peer_port_id):
        for ep in self.endpoints:
            if ep.get_peer().port_id == peer_port_id:
                return True
        return False

    def attach_endpoint(self, ep):
        peer_port_id = ep.peer_port_id
        match = [e for e in self.endpoints if e.peer_port_id == peer_port_id]
        if not match:
            old_endpoint = None
        else:
            old_endpoint = match[0]
            self._detach_endpoint(old_endpoint)

        self.fifo.add_reader(ep.fifo_key)
        self.endpoints.append(ep)
        self.owner.did_connect(self)
        return old_endpoint

    def _detach_endpoint(self, ep):
        if ep not in self.endpoints:
            _log.warning("Outport: No such endpoint")
            return
        self.endpoints.remove(ep)
        if not self.endpoints:
            self.owner.did_disconnect(self)
        return

    def disconnect(self, port_id):
        """Disconnects the port endpoints associated with the given actor_id"""
        endpoints = self.endpoints
        self.endpoints = []

        disconnected_endpoints = []
        for ep in endpoints:
            if not port_id or ep.peer_port_id == port_id:
                self.fifo.commit_reads(ep.fifo_key, False)
                disconnected_endpoints.append(ep)
                self._detach_endpoint(ep)
            else:
                self.endpoints.append(ep)

        if not self.endpoints:
            self.owner.did_disconnect(self)
        return disconnected_endpoints

    def read_token(self):
        """Used by actor (owner) to read a token from the ports.
        Returns the first available, or None if token queue is empty.
        """
        for ep in self.endpoints:
            token = ep.read_token()
            if token:
                return token

        return None

    def peek_token(self):
        """Used by actor (owner) to peek a token from the ports. Following peeks will get next token.
        Reset with peek_rewind.
        """
        tokens = []
        for ep in self.endpoints:
            tkns = ep.peek_token()
            if isinstance(tkns, list):
                tokens.extend(tkns)
            else:
                tokens.append(tkns)
        return tokens

    def peek_rewind(self):
        """Used by actor (owner) to rewind port peeking to front token."""
        r = []
        for ep in self.endpoints:
            r.append(ep.peek_rewind())
        return r

    def commit_peek_as_read(self):
        """Used by actor (owner) to rewind port peeking to front token."""
        r = []
        for ep in self.endpoints:
            r.append(ep.commit_peek_as_read())
        return r

    def available_tokens(self):
        """Used by actor (owner) to check number of tokens on the port."""
        available = 0
        for ep in self.endpoints:
            available += ep.available_tokens()
        return available

    def get_peers(self):
        peers = []
        for ep in self.endpoints:
            peers.append(ep.get_peer())

        return peers


class OutPort(Port):

    """An outport can have many endpoints."""

    def __init__(self, name, owner):
        super(OutPort, self).__init__(name, owner)
        self.fanout = 1
        self.endpoints = []

    def __str__(self):
        s = super(OutPort, self).__str__()
        s = s + "fan-out: %s\n" % self.fanout
        s = s + " [ "
        for ep in self.endpoints:
            s = s + str(ep) + " "
        s = s + "]"
        return s

    def _state(self):
        state = super(OutPort, self)._state()
        state['fanout'] = self.fanout
        return state

    def _set_state(self, state):
        self.fanout = state.pop('fanout')
        super(OutPort, self)._set_state(state)

    def is_connected(self):
        if len(self.endpoints) < self.fanout:
            return False
        for ep in self.endpoints:
            if not ep.is_connected():
                return False
        return True

    def is_connected_to(self, peer_port_id):
        for ep in self.endpoints:
            if ep.get_peer().port_id == peer_port_id:
                return True
        return False

    def attach_endpoint(self, ep):
        peer_port_id = ep.peer_port_id
        match = [e for e in self.endpoints if e.peer_port_id == peer_port_id]
        if not match:
            old_endpoint = None
        else:
            old_endpoint = match[0]
            self._detach_endpoint(old_endpoint)

        self.fifo.add_reader(ep.fifo_key)
        self.endpoints.append(ep)
        self.owner.did_connect(self)
        return old_endpoint

    def _detach_endpoint(self, ep):
        if ep not in self.endpoints:
            _log.warning("Outport: No such endpoint")
            return
        self.endpoints.remove(ep)
        if not self.endpoints:
            self.owner.did_disconnect(self)

    def disconnect(self, port_id):
        """Disconnects the port endpoints associated with the given actor_id"""
        endpoints = self.endpoints
        self.endpoints = []
        disconnected_endpoints = []

        # Rewind any tentative reads to acked reads
        # When local no effect since already equal

        # When tunneled transport tokens after last continuous acked token will be resent later, receiver will just ack them again if rereceived
        for ep in endpoints:
            if not port_id or ep.peer_port_id == port_id:
                self.fifo.commit_reads(ep.fifo_key, False)
                disconnected_endpoints.append(ep)
            else:
                self.endpoints.append(ep)

        if not self.endpoints:
            self.owner.did_disconnect(self)
        return disconnected_endpoints

    def write_token(self, data):
        """docstring for write_token"""
        errors = []
        for e in self.endpoints:
            if not self.fifo.write(data, e.fifo_key):
                errors.append("FIFO full when writing to port {}.{} from {}.{} with id {}".format(
                    self.owner.name, self.name, e.port.owner.name, e.port.name, e.fifo_key))
        if errors:
            raise FifoFullException("\n".join(errors))

    def available_tokens(self):
        """Used by actor (owner) to check number of token slots available on the port."""
        return sum([self.fifo.available_slots(ep.fifo_key) for ep in self.endpoints])

    def can_write(self):
        """Used by actor to test if writing a token is possible. Returns a boolean."""
        return self.fifo.can_write()

    def get_peers(self):
        peers = []
        for ep in self.endpoints:
            peers.append(ep.get_peer())
        if len(peers) < len(self.fifo.readers):
            all = copy.copy(self.fifo.readers)
            all -= set([p.port_id for p in peers])
            peers.extend([Peer(None, port_id) for port_id in all])
        return peers


if __name__ == '__main__':
    class Stub(object):

        def __init__(self, name):
            self.name = name

    a = Stub('a')

    i = InPort('in', None, a)
    o = OutPort('out', None, a)

    print(i.id)
    print(o.id)
