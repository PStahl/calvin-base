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

from multiprocessing import Process
# For trace
import sys
import trace
import logging

from calvin.calvinsys import Sys as CalvinSys

from calvin.runtime.north import actormanager
from calvin.runtime.north import appmanager
from calvin.runtime.north import scheduler
from calvin.runtime.north import storage
from calvin.runtime.north import calvincontrol
from calvin.runtime.north import metering
from calvin.runtime.north.calvin_network import CalvinNetwork
from calvin.runtime.north.calvin_proto import CalvinProto
from calvin.runtime.north.portmanager import PortManager
from calvin.runtime.north.replicator import Replicator
from calvin.runtime.south.monitor import Event_Monitor
from calvin.runtime.south.plugins.async import async
from calvin.utilities.attribute_resolver import AttributeResolver
from calvin.utilities.calvin_callback import CalvinCB
import calvin.utilities.calvinresponse as response
from calvin.utilities import calvinuuid
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinconfig
from calvin.runtime.north.resource_manager import ResourceManager

_log = get_logger(__name__)
_conf = calvinconfig.get()


def addr_from_uri(uri):
    _, host = uri.split("://")
    addr, _ = host.split(":")
    return addr


class Node(object):

    """A node of calvin
       the uri is a list of server connection points
       the control_uri is the local console
       attributes is a supplied list of external defined attributes that will be used as the key when storing index
       such as name of node
    """

    def __init__(self, uri, control_uri, attributes=None, self_start=True):
        super(Node, self).__init__()
        self.uri = uri
        self.control_uri = control_uri
        try:
            self.attributes = AttributeResolver(attributes)
        except:
            _log.exception("Attributes not correct, uses empty attribute!")
            self.attributes = AttributeResolver(None)
        self.id = calvinuuid.uuid("NODE")
        self.metering = metering.set_metering(metering.Metering(self))
        self.monitor = Event_Monitor()
        self.am = actormanager.ActorManager(self)
        self.control = calvincontrol.get_calvincontrol()
        _scheduler = scheduler.DebugScheduler if _log.getEffectiveLevel() <= logging.DEBUG else scheduler.Scheduler
        self.sched = _scheduler(self, self.am, self.monitor)
        self.async_msg_ids = {}
        self._calvinsys = CalvinSys(self)

        # Default will multicast and listen on all interfaces
        # TODO: be able to specify the interfaces
        # @TODO: Store capabilities
        self.storage = storage.Storage(self)

        self.network = CalvinNetwork(self)
        self.proto = CalvinProto(self, self.network)
        self.pm = PortManager(self, self.proto)
        self.app_manager = appmanager.AppManager(self)
        self.resource_manager = ResourceManager()

        # The initialization that requires the main loop operating is deferred to start function
        if self_start:
            async.DelayedCall(0, self.start)

    def insert_local_reply(self):
        msg_id = calvinuuid.uuid("LMSG")
        self.async_msg_ids[msg_id] = None
        return msg_id

    def set_local_reply(self, msg_id, reply):
        if msg_id in self.async_msg_ids:
            self.async_msg_ids[msg_id] = reply

    def connect(self, actor_id=None, port_name=None, port_dir=None, port_id=None,
                peer_node_id=None, peer_actor_id=None, peer_port_name=None,
                peer_port_dir=None, peer_port_id=None, cb=None):
        self.pm.connect(actor_id=actor_id,
                        port_name=port_name,
                        port_dir=port_dir,
                        port_id=port_id,
                        peer_node_id=peer_node_id,
                        peer_actor_id=peer_actor_id,
                        peer_port_name=peer_port_name,
                        peer_port_dir=peer_port_dir,
                        peer_port_id=peer_port_id,
                        callback=CalvinCB(self.logging_callback, preamble="connect cb")  if cb is None else cb)

    def disconnect(self, actor_id=None, port_name=None, port_dir=None, port_id=None, cb=None):
        _log.debug("disconnect(actor_id=%s, port_name=%s, port_dir=%s, port_id=%s)" %
                   (actor_id if actor_id else "", port_name if port_name else "",
                    port_dir if port_dir else "", port_id if port_id else ""))
        self.pm.disconnect(actor_id=actor_id, port_name=port_name,
                           port_dir=port_dir, port_id=port_id,
                           callback=CalvinCB(self.logging_callback, preamble="disconnect cb") if cb is None else cb)

    def peersetup(self, peers, cb=None):
        """ Sets up a RT to RT communication channel, only needed if the peer can't be found in storage.
            peers: a list of peer uris, e.g. ["calvinip://127.0.0.1:5001"]
        """
        _log.debug("peersetup(%s)" % (peers))
        peers_copy = peers[:]
        peer_node_ids = {}
        if not cb:
            callback = CalvinCB(self.logging_callback, preamble="peersetup cb")
        else:
            callback = CalvinCB(self.peersetup_collect_cb, peers=peers_copy, peer_node_ids=peer_node_ids, org_cb=cb)

        self.network.join(peers, callback=callback)

    def peersetup_collect_cb(self, status, uri, peer_node_id, peer_node_ids, peers, org_cb):
        if uri in peers:
            peers.remove(uri)
            peer_node_ids[uri] = (peer_node_id, status)
        if not peers:
            # Get highest status, i.e. any error
            comb_status = max([s for _, s in peer_node_ids.values()])
            org_cb(peer_node_ids=peer_node_ids, status=comb_status)

    def logging_callback(self, preamble=None, *args, **kwargs):
        _log.debug("\n%s# NODE: %s \n# %s %s %s \n%s" %
                   ('#' * 40, self.id, preamble if preamble else "*", args, kwargs, '#' * 40))

    def new(self, actor_type, args, deploy_args=None, state=None, prev_connections=None, connection_list=None):
        # TODO requirements should be input to am.new
        actor_id = self.am.new(actor_type, args, state, prev_connections, connection_list,
                               app_id=deploy_args['app_id'] if deploy_args else None,
                               signature=deploy_args['signature'] if deploy_args and 'signature' in deploy_args else None)
        if deploy_args:
            app_id = deploy_args['app_id']
            if 'app_name' not in deploy_args:
                app_name = app_id
            else:
                app_name = deploy_args['app_name']
            self.app_manager.add(app_id, actor_id,
                                 deploy_info = deploy_args['deploy_info'] if 'deploy_info' in deploy_args else None)
        return actor_id

    def deployment_control(self, app_id, actor_id, deploy_args):
        """ Updates an actor's deployment """
        self.am.deployment_control(app_id, actor_id, deploy_args)

    def calvinsys(self):
        """Return a CalvinSys instance"""
        # FIXME: We still need to sort out actor requirements vs. node capabilities and user permissions.
        # @TODO: Write node capabilities to storage
        return self._calvinsys

    def report_resource_usage(self, usage):
        _log.debug("Reporting resource usage for node {}: {}".format(self.id, usage))
        self.resource_manager.register(self.id, usage)
        for peer_id in self.network.links:
            callback = CalvinCB(self._report_resource_usage_cb, peer_id)
            self.proto.report_usage(peer_id, self.id, usage, callback=callback)

    def _report_resource_usage_cb(self, peer_id, status):
        _log.debug("Report resource usage callback received status {} for {}".format(status, peer_id))
        if not status:
            self.lost_node(peer_id)

    def register_resource_usage(self, node_id, usage, callback):
        _log.debug("Registering resource usage for node {}: {}".format(node_id, usage))
        self.resource_manager.register(node_id, usage)
        callback(status=response.CalvinResponse(True))

    def lost_node(self, node_id):
        _log.info("Lost node {}".format(node_id))
        highest_prio_node = self._highest_prio_node(node_id)
        if highest_prio_node == self.id:
            _log.debug("We have highest id, replicate actors")
            self.replicate_node_actors(node_id, cb=CalvinCB(self._lost_node_cb))

    def _lost_node_cb(self, status):
        if not status:
            _log.error("Failed to handle lost node: {}".format(status))

    def _highest_prio_node(self, node_id):
        node_ids = self.network.links.keys()
        if node_id in node_ids:
            node_ids.remove(node_id)

        if self.id not in node_ids:
            node_ids.append(self.id)

        return sorted(node_ids)[0]

    def replicate_node_actors(self, node_id, cb):
        _log.info("Fetching actors for lost node: {}".format(node_id))
        self.storage.get_node_actors(node_id, cb=CalvinCB(self._replicate_node_actors, node_id=node_id, cb=cb))

    def _replicate_node_actors(self, key, value, node_id, cb):
        _log.info("Replicating lost actors {}".format(value))
        if value is None:
            _log.debug("Storage returned None when fetching node actors")
            cb(status=response.CalvinResponse(False))
            return
        elif value == []:
            _log.debug("No value returned from storage when fetching node actors")
            cb(status=response.CalvinResponse(True))
            return

        for actor_id in value:
            self.storage.get_actor(actor_id, cb=CalvinCB(self._replicate_node_actor, lost_actor_id=actor_id, cb=cb))

    def _replicate_node_actor(self, key, value, lost_actor_id, cb):
        """ Get app id and actor name from actor info """
        _log.info("Replicating actor {}: {}".format(key, value))
        if not value:
            _log.error("Failed get lost actor info from storage")
            cb(response.CalvinResponse(False))
            return

        cb = CalvinCB(func=self._handle_lost_application_actor, lost_actor_id=lost_actor_id,
                      lost_actor_info=value, cb=cb)
        self.storage.get_application_actors(value['app_id'], cb=cb)

    def _handle_lost_application_actor(self, key, value, lost_actor_id, lost_actor_info, cb):
        """ Get required reliability from app info """
        if not value:
            _log.error("Failed to get application actors")
            return

        replicator = Replicator(self, self.control, calvincontrol.uuid_re)
        replicator.replicate_lost_actor(value, lost_actor_id, lost_actor_info, cb=cb)

    #
    # Event loop
    #
    def run(self):
        """main loop on node"""
        _log.debug("Node %s is running" % self.id)
        self.sched.run()

    def start(self):
        """ Run once when main loop is started """
        interfaces = _conf.get(None, 'transports')
        self.network.register(interfaces, ['json'])
        self.network.start_listeners(self.uri)
        # Start storage after network, proto etc since storage proxy expects them
        self.storage.start()
        self.storage.add_node(self)

        # Start control api
        proxy_control_uri = _conf.get(None, 'control_proxy')
        _log.debug("Start control API on %s with uri: %s and proxy: %s" % (self.id, self.control_uri, proxy_control_uri))
        if proxy_control_uri is not None:
            self.control.start(node=self, uri=proxy_control_uri, tunnel=True)
        else:
            if self.control_uri is not None:
                self.control.start(node=self, uri=self.control_uri)

        self._start_resource_reporter()

    def _start_resource_reporter(self):
        actor_id = self.new("sys.NodeResourceReporter", {'node': self})
        actor = self.am.actors[actor_id]
        if not actor.inports or not actor.outports:
            _log.warning("Could not set up ResourceReporter: {}".format(actor))
            return

        in_port = actor.inports['in']
        out_port = actor.outports['out']
        self.connect(actor_id, port_name=in_port.name, port_dir='in', port_id=in_port.id,
                     peer_node_id=self.id, peer_actor_id=actor_id, peer_port_name=out_port.name,
                     peer_port_dir='out', peer_port_id=out_port.id)

    def stop(self, callback=None):
        def stopped(*args):
            _log.analyze(self.id, "+", {'args': args})
            _log.debug(args)
            self.sched.stop()
            _log.analyze(self.id, "+ SCHED STOPPED", {'args': args})
            self.control.stop()
            _log.analyze(self.id, "+ CONTROL STOPPED", {'args': args})

        def deleted_node(*args, **kwargs):
            _log.analyze(self.id, "+", {'args': args, 'kwargs': kwargs})
            self.storage.stop(stopped)

        _log.analyze(self.id, "+", {})
        self.storage.delete_node(self.id, self.attributes.get_indexed_public(), cb=deleted_node)


def create_node(uri, control_uri, attributes=None):
    n = Node(uri, control_uri, attributes)
    n.run()
    _log.info('Quitting node "%s"' % n.uri)


def create_tracing_node(uri, control_uri, attributes=None):
    """
    Same as create_node, but will trace every line of execution.
    Creates trace dump in output file '<host>_<port>.trace'
    """
    n = Node(uri, control_uri, attributes)
    _, host = uri.split('://')
    with open("%s.trace" % (host, ), "w") as f:
        tmp = sys.stdout
        # Modules to ignore
        ignore = [
            'fifo', 'calvin', 'actor', 'pickle', 'socket',
            'uuid', 'codecs', 'copy_reg', 'string_escape', '__init__',
            'colorlog', 'posixpath', 'glob', 'genericpath', 'base',
            'sre_parse', 'sre_compile', 'fdesc', 'posixbase', 'escape_codes',
            'fnmatch', 'urlparse', 're', 'stat', 'six'
        ]
        with f as sys.stdout:
            tracer = trace.Trace(trace=1, count=0, ignoremods=ignore)
            tracer.runfunc(n.run)
        sys.stdout = tmp
    _log.info('Quitting node "%s"' % n.uri)


def start_node(uri, control_uri, trace_exec=False, attributes=None):
    _create_node = create_tracing_node if trace_exec else create_node
    p = Process(target=_create_node, args=(uri, control_uri, attributes))
    p.daemon = True
    p.start()
    return p
