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
from collections import defaultdict
# For trace
import sys
import os
import trace
import logging
import socket
import time
from datetime import datetime, timedelta

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
from calvin.runtime.north.app_monitor import AppMonitor
from calvin.runtime.north.replicator import Replicator
from calvin.runtime.south.monitor import Event_Monitor
from calvin.runtime.south.plugins.async import async
from calvin.runtime.north.lost_node_handler import LostNodeHandler
from calvin.utilities.attribute_resolver import AttributeResolver
from calvin.utilities.calvin_callback import CalvinCB
import calvin.utilities.calvinresponse as response
from calvin.utilities import calvinuuid
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinconfig
from calvin.runtime.north.resource_manager import ResourceManager

_log = get_logger(__name__)
_conf = calvinconfig.get()

HEARTBEAT_TIMEOUT = 0.5
HEARTBEAT_DELAY = 0.20


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

        self.app_monitor = AppMonitor(self, self.app_manager, self.storage)
        self.lost_node_handler = LostNodeHandler(self, self.resource_manager, self.pm, self.am, self.storage)

        self.heartbeat_actor = None
        self.outgoing_heartbeats = defaultdict(list)

        # The initialization that requires the main loop operating is deferred to start function
        if self_start:
            async.DelayedCall(0, self.start)

    @property
    def storage_node(self):
        return False

    def is_storage_node(self, node_id):
        if node_id == self.id:
            return self.storage_node
        #if node_id not in self.network.links:
        #    return False

        return self.storage.proxy == self.network.links[node_id].transport.get_uri()

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
        _log.info("peersetup(%s)" % (peers))
        peers_copy = peers[:]
        peer_node_ids = {}
        if not cb:
            callback = CalvinCB(self.logging_callback, preamble="peersetup cb")
        else:
            callback = CalvinCB(self.peersetup_collect_cb, peers=peers_copy, peer_node_ids=peer_node_ids, org_cb=cb)

        peers = filter(None, peers)
        self.network.join(peers, callback=callback)

    def peersetup_collect_cb(self, status, uri, peer_node_id, peer_node_ids, peers, org_cb):
        _log.debug("Peersetup collect cb: {} - {} - {} - {} - {}".format(status, uri, peer_node_id, peer_node_ids, peers))
        self.resource_manager.register_uri(peer_node_id, uri)
        if status:
            self.resource_manager.register_uri(peer_node_id, uri)
            self._register_heartbeat_receiver(peer_node_id)

        if uri in peers:
            peers.remove(uri)
            peer_node_ids[uri] = (peer_node_id, status)
        if not peers:
            # Get highest status, i.e. any error
            comb_status = max([s for _, s in peer_node_ids.values()])
            org_cb(peer_node_ids=peer_node_ids, status=comb_status)

        if peer_node_id and status:
            self._send_rm_info(peer_node_id)

    def _send_rm_info(self, peer_node_id):
        # Send own info to new peers and retreive there info
        callback = CalvinCB(self._send_rm_info_cb)
        usages = self.resource_manager.sync_info()

        self.proto.send_rm_info(peer_node_id, usages, callback)

    def _send_rm_info_cb(self, status, *args, **kwargs):
        # Receives other peers rm info
        if status.data:
            self.resource_manager.sync_info(status.data[0])

    def sync_rm_info(self, usages, callback):
        # sync received info
        usages = self.resource_manager.sync_info(usages)
        callback(usages, status=response.CalvinResponse(True))

    def logging_callback(self, preamble=None, *args, **kwargs):
        _log.debug("\n%s# NODE: %s \n# %s %s %s \n%s" %
                   ('#' * 40, self.id, preamble if preamble else "*", args, kwargs, '#' * 40))

    def new(self, actor_type, args, deploy_args=None, state=None, prev_connections=None, connection_list=None, callback=None):
        # TODO requirements should be input to am.new
        callback = CalvinCB(self._new, args=args, deploy_args=deploy_args, state=state, prev_connections=prev_connections,
                            connection_list=connection_list, callback=callback)
        self.am.new(actor_type, args, state, prev_connections, connection_list,
                    app_id=deploy_args['app_id'] if deploy_args else None,
                    signature=deploy_args['signature'] if deploy_args and 'signature' in deploy_args else None, callback=callback)

    def _new(self, actor_id, status, args, deploy_args, state, prev_connections, connection_list, callback):
        if not status:
            if callback:
                callback(status=status, actor_id=actor_id)
            return
        if deploy_args:
            app_id = deploy_args['app_id']
            if 'app_name' not in deploy_args:
                app_name = app_id
            else:
                app_name = deploy_args['app_name']
            self.app_manager.add(app_id, actor_id,
                                 deploy_info = deploy_args['deploy_info'] if 'deploy_info' in deploy_args else None)

        if callback:
            callback(status=status, actor_id=actor_id)

    def deployment_control(self, app_id, actor_id, deploy_args):
        """ Updates an actor's deployment """
        self.am.deployment_control(app_id, actor_id, deploy_args)

    def calvinsys(self):
        """Return a CalvinSys instance"""
        # FIXME: We still need to sort out actor requirements vs. node capabilities and user permissions.
        # @TODO: Write node capabilities to storage
        return self._calvinsys

    @property
    def hostname(self):
        return socket.gethostname()

    @property
    def testing(self):
        return "CALVIN_TESTING" in os.environ and os.environ["CALVIN_TESTING"]

    def info(self, s):
        _log.info("[{}] {}".format(self.hostname, s))

    def print_stats(self, lost_node_id=None):
        if self.storage_node:
            return

        for app in self.app_manager.applications.values():
            cb = CalvinCB(self._print_stats, app_id=app.id, lost_node_id=lost_node_id, required=app.required_reliability)
            self.storage.get_replica_nodes(app.id, 'actions:src', cb)

    def _print_stats(self, key, value, app_id, lost_node_id, required):
        if value is None:
            return

        if lost_node_id in value:
            value.remove(lost_node_id)

        nodes = [(node_id, self.resource_manager.node_uris.get(node_id)) for node_id in value]
        _log.debug("CURRENT NODES: {} {}".format(len(nodes), nodes))

        callback = CalvinCB(self._print_stats_cb, nodes=nodes, required=required, current_nodes=value)
        self.storage.get_replication_times('std.CountTimer', callback)

    def _print_stats_cb(self, key, value, nodes, required, current_nodes):
        self._failure_times = {}
        uris = []

        for node_id in self.network.list_links():
            uri = self.resource_manager.node_uris.get(node_id)
            uri = uri.replace("calvinip://", "").replace("http://", "") if uri else uri
            if uri:
                uris.append(uri)
        uris = list(set(uris))

        for uri in uris:
            self._failure_times[uri] = None

        for uri in uris:
            callback = CalvinCB(self._collect_failure_times, nodes=nodes, required=required, 
                                current_nodes=current_nodes, replication_times=value)
            self.storage.get_failure_times(uri, callback)

    def _collect_failure_times(self, key, value, nodes, required, current_nodes, replication_times):
        if value:
            self._failure_times[key] = value
        elif key in self._failure_times:
            del self._failure_times[key]

        if None in self._failure_times.values():
            return

        rm = self.resource_manager
        rels = []
        for node_id in self.network.list_links():
            rel = rm.get_reliability(node_id, replication_times, self._failure_times)
            rels.append((rm.node_uris.get(node_id), rel, 1 - rel))
        _log.info("NODE RELIABILITIES: {}".format(rels))

        actual_rel = self.resource_manager.current_reliability(current_nodes, replication_times, self._failure_times)
        _log.debug("RELIABILITY: {}".format(actual_rel))
        rep_time = self.resource_manager.replication_time(replication_times)
        actors = []
        for actor in self.am.actors.values():
            if 'actions:src' in actor.name:
                actors.append(actor)
        rels = self._get_rels(replication_times, self._failure_times)

        cpu_avgs = self.resource_manager.get_avg_usages()

        self.info("APP_INFO: [{}] [{}] [{}] [{}] [{}] [{}] [{}] [{}]".format(
            len(nodes), nodes, rels, actual_rel, required, rep_time, self._failure_times, str(cpu_avgs)))

    def _get_rels(self, replication_times, failure_times):
        all_nodes = self.network.list_links()
        rm = self.resource_manager
        rels = []
        _log.debug("all nodes: {}".format(all_nodes))
        for node_id in all_nodes:
            rel = rm.get_reliability(node_id, replication_times, failure_times)
            uri = rm.node_uris.get(node_id)
            if uri in failure_times:
                mtbf = rm.reliability_calculator.get_mtbf(failure_times[uri])
                rels.append((uri, rel, 1 - rel, mtbf))
            else:
                mtbf = rm.reliability_calculator.get_mtbf([])
                rels.append((uri, rel, 1 - rel, mtbf))
        return rels

    def report_resource_usage(self, usage):
        _log.debug("Reporting resource usage for node {}: {}".format(self.id, usage))
        self.resource_manager.register(self.id, usage, self.uri)

        self.print_stats()
        usage['uri'] = self.uri
        for peer_id in self.network.list_links():
            callback = CalvinCB(self._report_resource_usage_cb, peer_id)
            self.proto.report_usage(peer_id, self.id, usage, callback=callback)

        self.app_monitor.check_reliabilities()

    def _report_resource_usage_cb(self, peer_id, status):
        if not status:
            _log.error("Failed to report resource usage to: {} - {} - {}".format(peer_id, self.resource_manager.node_uris.get(peer_id), status))
        else:
            _log.debug("Report resource usage callback received status {} for {}".format(status, peer_id))

    def register_resource_usage(self, node_id, usage, callback):
        if self.storage_node:
            callback(status=response.CalvinResponse(True))
            return        
        _log.debug("Registering resource usage for node {}: {}".format(node_id, usage))
        uri = usage.get('uri')

        self.resource_manager.register(node_id, usage, uri)
        self._register_heartbeat_receiver(node_id)
        callback(status=response.CalvinResponse(True))

    def report_replication_time(self, actor_type, replication_time, node_id):
        _log.info('New replication time: {} when handling lost node {}'.format(replication_time, node_id))
        self.storage.new_replication_time(actor_type, replication_time)

    def lost_node(self, node_id):
        _log.debug("Lost node: {}".format(node_id))
        _log.analyze(self.id, "+", "Lost node {}".format(node_id))
        if self.storage_node:
            _log.debug("{} Is storage node, ignoring lost node".format(self.id))
            return

        self.print_stats(lost_node_id=node_id)

        if node_id in self.network.links:
            link = self.network.links[node_id]
            self.network.peer_disconnected(link, node_id, "Heartbeat timeout")
        self.lost_node_handler.handle_lost_node(node_id)

    def lost_node_request(self, node_id, cb):
        _log.debug("Lost node: {}".format(node_id))
        _log.analyze(self.id, "+", "Lost node {}".format(node_id))
        if self.storage_node:
            _log.debug("{} Is storage node, ignoring lost node".format(self.id))
            if cb:
                cb(status=response.CalvinResponse(False))
            return

        self.lost_node_handler.handle_lost_node_request(node_id, cb)

    def lost_actor(self, lost_actor_id, lost_actor_info, required_reliability, cb):
        _log.analyze(self.id, "+", "Lost actor {}".format(lost_actor_id))
        self.am.delete_actor(lost_actor_id)
        replicator = Replicator(self, lost_actor_id, lost_actor_info, required_reliability)
        replicator.replicate_lost_actor(cb, time.time())

    def _heartbeat_timeout(self, timestamp, node_id):
        diff = datetime.now() - (timestamp + timedelta(seconds=HEARTBEAT_TIMEOUT))
        print "LOST NODE: [{}], [{}] [{}] [{}]".format(node_id, datetime.now(), timestamp + timedelta(seconds=HEARTBEAT_TIMEOUT), diff.total_seconds())
        _log.error("LOST NODE: [{}], [{}] [{}] [{}]".format(node_id, datetime.now(), timestamp + timedelta(seconds=HEARTBEAT_TIMEOUT), diff.total_seconds()))
        self._clear_heartbeat_timeouts(node_id)
        self.heartbeat_actor.deregister(node_id)
        #self.lost_node(node_id)

    def clear_outgoing_heartbeat(self, data):
        if "node_id" in data:
            node_id = data['node_id']
            self._clear_heartbeat_timeouts(node_id)
            timeout_call = async.DelayedCall(HEARTBEAT_TIMEOUT, CalvinCB(self._heartbeat_timeout, timestamp=datetime.now(), node_id=node_id))
            self.outgoing_heartbeats[node_id].append(timeout_call)
            self.resource_manager.register_uri(data['node_id'], data['uri'])

    def _clear_heartbeat_timeouts(self, node_id):
        for timeout_call in self.outgoing_heartbeats[node_id]:
            try:
                timeout_call.cancel()
            except Exception as e:
                pass
    #
    # Event loop
    #
    def run(self):
        """main loop on node"""
        _log.debug("Node %s is running" % self.id)
        self.sched.run()

    def start(self):
        """ Run once when main loop is started """
        if not self.storage_node:
            self._start_heartbeat_system()
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

        if False and not self.storage_node:
            self._start_resource_reporter()

    def _start_resource_reporter(self):
        actor_id = self.new("sys.NodeResourceReporter", {'node': self, 'delay': 0.25}, callback=self._start_rr)

    def _start_rr(self, status, actor_id):
        if not status:
            _log.error("Failed to start resource reporter")
            return
        _log.info("Successfully started resource reporter")
        actor = self.am.actors[actor_id]
        if not actor.inports or not actor.outports:
            _log.warning("Could not set up ResourceReporter: {}".format(actor))
            return

        in_port = actor.inports['in']
        out_port = actor.outports['out']
        self.connect(actor_id, port_name=in_port.name, port_dir='in', port_id=in_port.id,
                     peer_node_id=self.id, peer_actor_id=actor_id, peer_port_name=out_port.name,
                     peer_port_dir='out', peer_port_id=out_port.id)

    def _start_heartbeat_system(self):
        if self.testing:
            return
        uri = self.control_uri.replace("http://", "")
        if uri == "localhost":
            addr = socket.gethostbyname(uri.split(":")[0])
        else:
            addr = uri.split(":")[0]
        port = int(uri.split(":")[1]) + 5000

        self.new("net.Heartbeat", {'node': self, 'address': addr, 'port': port, 'delay': HEARTBEAT_DELAY}, callback=self._start_hb)

    def _start_hb(self, status, actor_id):
        if not status:
            _log.error("Failed to start heartbeat system: ".format(status))
            return
        _log.info("Successfully started heartbeat actor")
        actor = self.am.actors[actor_id]
        in_port = actor.inports['in']
        out_port = actor.outports['out']
        self.connect(actor_id, port_name=in_port.name, port_dir='in', port_id=in_port.id,
                     peer_node_id=self.id, peer_actor_id=actor_id, peer_port_name=out_port.name,
                     peer_port_dir='out', peer_port_id=out_port.id)
        self.heartbeat_actor = actor

    def _register_heartbeat_receiver(self, node_id):
        if self.storage_node or self.is_storage_node(node_id):
            return
        if not self.heartbeat_actor:
            self._start_heartbeat_system()
        if self.testing or node_id in self.heartbeat_actor.nodes:
            return

        _log.info("Registering receiver: {}".format(node_id))
        self.heartbeat_actor.register(node_id)

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
    print "QUIT NODE: [{}] [{}]".format(n.id, datetime.now())


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
