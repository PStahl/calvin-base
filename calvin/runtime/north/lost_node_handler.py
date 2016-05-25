
import time

from collections import defaultdict

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities.calvinlogger import get_logger
import calvin.utilities.calvinresponse as response
from calvin.runtime.north.replicator import Replicator

_log = get_logger(__name__)


class LostNodeHandler(object):

    def __init__(self, node, resource_manager, port_manager, actor_manager, storage):
        self.node = node
        self._lost_nodes = set()
        self._lost_nodes_times = dict()
        self._lost_node_requests = set()
        self._failed_requests = defaultdict(set)
        self._finished_requests = dict()
        self._callbacks = defaultdict(set)
        self.resource_manager = resource_manager
        self.pm = port_manager
        self.am = actor_manager
        self.storage = storage

    def handle_lost_node(self, node_id):
        _log.debug("Lost node {}".format(node_id))
        self._delete_replica_nodes(node_id)
        self._register_lost_node(node_id)

        if node_id in self._lost_nodes:
            _log.debug("We are already handling lost node {}".format(node_id))
            return

        self._lost_nodes.add(node_id)
        self._lost_node_requests.add(node_id) #necessary?

        lost_node_time = time.time()
        self._lost_nodes_times[node_id] = lost_node_time

        self.pm.close_disconnected_ports(self.am.actors.values(), node_id)

        highest_prio_node = self._highest_prio_node(node_id)
        if highest_prio_node == self.node.id:
            _log.info("We have highest id, replicate actors")
            self._handle_lost_node(node_id)
        elif highest_prio_node:
            cb = CalvinCB(self._lost_node_request_cb, node_id=node_id, prio_node=highest_prio_node)
            _log.info("Sending lost node msg of node {} to {} - {}".format(
                node_id, highest_prio_node, self.resource_manager.node_uris.get(highest_prio_node)))
            self.node.proto.lost_node(highest_prio_node, node_id, cb)

    def handle_lost_node_request(self, node_id, cb=None):
        _log.debug("Got lost node request {}".format(node_id))
        self._register_lost_node(node_id)
        if cb:
            _log.debug("Adding callback: {} for node {}".format(cb, node_id))
            self._callbacks[node_id].add(cb)

        if node_id in self._lost_node_requests or self._lost_nodes:
            _log.debug("Got multiple lost node signals, ignoring")
            if node_id in self._finished_requests:
                cb(status=self._finished_requests[node_id])
            return

        self._delete_replica_nodes(node_id)
        self.pm.close_disconnected_ports(self.am.actors.values(), node_id)

        self._lost_nodes.add(node_id)
        self._lost_node_requests.add(node_id)

        lost_node_time = time.time()
        self._lost_nodes_times[node_id] = lost_node_time

        self._handle_lost_node(node_id)

    def _handle_lost_node(self, node_id):
        _log.info("Handling lost node {}".format(node_id))
        if not self.storage.started:
            self.storage.start()

        self.pm.close_disconnected_ports(self.am.actors.values(), node_id)

        cb = CalvinCB(self._lost_node_cb, node_id=node_id)
        self.replicate_node_actors(node_id, cb=cb)

    def _register_lost_node(self, node_id):
        try:
            self.resource_manager.lost_node(node_id)
        except Exception as e:
            _log.error("{}".format(e))

    def _delete_replica_nodes(self, node_id):
        for actor in self.node.am.actors.values():
            if actor.app_id:
                self.storage.delete_replica_node(actor.app_id, node_id, actor.name)

    def _delete_node(self, key, value):
        _log.debug("Deleting node {} with value {}".format(key, value))
        if not value:
            return

        indexed_public = value['attributes'].get('indexed_public')
        self.storage.delete_node(key, indexed_public)

    def _lost_node_request_cb(self, status, node_id, prio_node):
        """ Callback when we sent a request. If the request fails, send to new node """
        _log.debug("Lost node CB for lost node {}: {}".format(node_id, status))
        if not status:
            self._failed_requests[node_id].add(prio_node)
            prio_node_uri = self.resource_manager.node_uris.get(prio_node)
            _log.warning("Node {} {} failed to handle lost node {}: {}".format(prio_node, prio_node_uri, node_id, status))
            if node_id in self._lost_nodes:
                self._lost_nodes.remove(node_id)
            self.handle_lost_node(node_id)
        else:
            _log.info("Node {} successfully handled lost node {} - {}".format(prio_node, node_id, status))

    def _lost_node_cb(self, status, node_id):
        """ Callback when we handled lost node """
        _log.debug("Lost node CB for lost node {}: {}".format(node_id, status))
        self._finished_requests[node_id] = status
        if not status:
            _log.warning("We failed to handle lost node {}: {}".format(node_id, status))
        else:
            _log.info("We successfully handled lost node {} - {} - {}".format(node_id, self.node.id, status))
            self.storage.get_node(node_id, self._delete_node)
            uri = self.resource_manager.node_uris.get(node_id)
            if uri:
                self.node.storage.add_failure_time(uri, node_id, self._lost_nodes_times[node_id])
            else:
                _log.warning("Could not store failure info of node {}, no uri".format(node_id))
                #Warning?

        for cb in self._callbacks[node_id]:
            _log.debug("Calling cb {} with status {}".format(cb, status))
            cb(status=status)

    def _highest_prio_node(self, node_id):
        node_ids = self.node.network.list_links()
        _log.debug("Getting highest_prio_node among {}".format(node_ids))
        if not node_ids:
            # We are not connected to anyone
            return None

        if node_id in node_ids:
            node_ids.remove(node_id)
        for n_id in self._failed_requests[node_id]:
            if n_id in node_ids:
                node_ids.remove(n_id)

        if self.node.id not in node_ids:
            node_ids.append(self.node.id)

        node_ids = [n_id for n_id in node_ids if not self.node.is_storage_node(n_id)]
        if not node_ids:
            return None

        highest = sorted(node_ids)[0]
        _log.debug("Highest prio node: {} - {}".format(highest, self.resource_manager.node_uris.get(highest)))
        return highest

    def replicate_node_actors(self, node_id, cb):
        _log.debug("Fetching actors for lost node: {}".format(node_id))
        start_time = self._lost_nodes_times[node_id]
        try:
            self.storage.get_node_actors(node_id, cb=CalvinCB(self._replicate_node_actors,
                                                              node_id=node_id,
                                                              start_time=start_time,
                                                              cb=cb))
        except AttributeError as e:
            _log.error("Failed to get node actors: {}".format(e))
            # We are the deleted node
            pass

    def _replicate_node_actors(self, key, value, node_id, start_time, cb):
        _log.debug("Replicating lost actors {} for node {}".format(value, node_id))
        if value is None:
            _log.warning("Storage returned None when fetching node actors for node: {} - {}".format(
                node_id, self.resource_manager.node_uris.get(node_id)))
            cb(status=response.CalvinResponse(False))
            return
        elif value == []:
            _log.debug("No value returned from storage when fetching node actors for node {}".format(node_id))
            cb(status=response.CalvinResponse(self.storage.started))
            return

        for actor_id in value:
            self.storage.get_actor(actor_id, cb=CalvinCB(self._replicate_node_actor, lost_node_id=node_id,
                                   start_time=start_time, cb=cb))

    def _replicate_node_actor(self, key, value, lost_node_id, start_time, cb):
        """ Get app id and actor name from actor info """
        _log.debug("Replicating node actor {}: {}".format(key, value))
        self.storage.delete_actor(key)
        self.storage.delete_actor_from_node(lost_node_id, key)

        if not value:
            _log.error("Failed get actor info from storage for actor {}".format(key))
            cb(response.CalvinResponse(False))
            return

        cb = CalvinCB(func=self._handle_lost_application_actor, lost_node_id=lost_node_id,
                      lost_actor_id=key, lost_actor_info=value, start_time=start_time, cb=cb)
        self.storage.get_application(value['app_id'], cb=cb)
        self.storage.delete_actor_from_app(value['app_id'], key)

    def _handle_lost_application_actor(self, key, value, lost_node_id, lost_actor_id, lost_actor_info, start_time, cb):
        """ Get required reliability from app info """
        _log.debug("Handling lost application actor {} of app {}".format(lost_actor_id, key))
        self.storage.delete_replica_node(key, lost_node_id, lost_actor_info['name'])
        if not value:
            _log.error("Failed to get application info from storage for applicaiton {}".format(key))
            cb(status=response.CalvinResponse(False))

        replicator = Replicator(self.node, lost_actor_id, lost_actor_info, value['required_reliability'],
                                lost_node=lost_node_id)
        replicator.replicate_lost_actor(cb, start_time)
