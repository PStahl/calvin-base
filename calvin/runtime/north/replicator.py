import re

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinresponse as response
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class Replicator(object):
    def __init__(self, node, control, actor_id_re):
        self.node = node
        self.control = control
        self.actor_id_re = actor_id_re
        self.current_nbr_of_replicas = 0
        self.replica_id = None
        self.replica_value = None

    def _is_match(self, first, second):
        is_match = re.sub(self.actor_id_re, "", first) == re.sub(self.actor_id_re, "", second)
        _log.debug("{} and {} is match: ".format(first, second, is_match))
        return is_match

    def replicate_lost_actor(self, actors, lost_actor_id, lost_actor_info, cb):
        _log.info("Replicating lost actor: {}".format(lost_actor_id))
        if lost_actor_id in actors:
            actors.remove(lost_actor_id)
        cb = CalvinCB(self._find_and_replicate, actors=actors, lost_actor_info=lost_actor_info, lost_actor_id=lost_actor_id, index=0, cb=cb)
        self._delete_lost_actor(lost_actor_id, lost_actor_info, cb=cb)

    def _find_and_replicate(self, actors, status, lost_actor_id, lost_actor_info, index, cb):
        if index < len(actors):
            cb = CalvinCB(self._check_for_original, lost_actor_id=lost_actor_id, lost_actor_info=lost_actor_info,
                          actors=actors, index=index, cb=cb)
            _log.debug("Searching for actor to replicate, trying {}".format(actors[index]))
            self.node.storage.get_actor(actors[index], cb=cb)
        else:
            self._replicate(self.replica_id, self.replica_value, lost_actor_id, lost_actor_info, cb)

    def _check_for_original(self, key, value, lost_actor_id, lost_actor_info, actors, index, cb):
        if value and self._is_match(value['name'], lost_actor_info['name']):
            _log.debug("Found an replica of lost actor:".format(key))
            self.current_nbr_of_replicas += 1
            self.replica_id = key
            self.replica_value = value
        self._find_and_replicate(actors, lost_actor_id, lost_actor_info, index + 1, cb)

    def _replicate(self, actor_id, actor_info, lost_actor_id, lost_actor_info, cb):
        if not self.replica_id is None:
            _log.info("Sending replication request of actor {} to node {}".format(actor_id, actor_info['node_id']))
            self.node.proto.actor_replication_request(actor_id, actor_info['node_id'], self.node.id, cb)
        else:
            cb(status=calvinresponse.NOT_FOUND)
            _log.warning("Could not find actor to replicate")

    def _delete_lost_actor(self, lost_actor_info, lost_actor_id, cb):
        self._close_actor_ports(lost_actor_id, lost_actor_info, cb)
        if status:
            cb = CalvinCB(self._delete_lost_actor_cb, lost_actor_id=lost_actor_id,
                          lost_actor_info=lost_actor_info, cb=cb)
            self.node.proto.actor_destroy(lost_actor_info['node_id'], callback=cb, actor_id=lost_actor_id)
        else:
            org_cb(status=status)

    def _close_actor_ports(self, lost_actor_id, lost_actor_info, cb):
        _log.info("Closing ports of actor {}".format(lost_actor_id))
        callback = CalvinCB(self._send_disconnect_request)
        for inport in lost_actor_info['inports']:
            self.node.storage.get_port(inport['id'], cb=callback)
        for outport in lost_actor_info['outports']:
            self.node.storage.get_port(outport['id'], cb=callback)
        cb(status=response.OK)

    def _send_disconnect_request(self, key, value):
        if not value:
            return

        _log.debug("Sending disconnect request for port {}".format(key))
        for (node_id, port_id) in value['peers']:
            if node_id == 'local' or node_id == self.node.id:
                _log.debug("Node {} is local. Asking port manager to close peer_port_id {}, port_id {}".format(
                    node_id, port_id, key))
                self.node.pm.disconnection_request({'peer_port_id': port_id, 'port_id': key})
            else:
                _log.debug("Node {} is remote. Sending port disconnect request for port_id {}, \
                           peer_node_id {} and peer_port_id {}".format(node_id, key, node_id, port_id))
                self.node.proto.port_disconnect(port_id=key, peer_node_id=node_id, peer_port_id=port_id)

    def _delete_lost_actor_cb(self, status, lost_actor_id, lost_actor_info, cb):
        _log.info("Deleting actor {} from local storage".format(lost_actor_id))
        self.node.storage.delete_actor_from_app(lost_actor_id, lost_actor_info['app_id'])
        self.node.storage.delete_actor(lost_actor_id)
        if status.status == response.SERVICE_UNAVAILABLE and not lost_actor_info['node_id'] == self.node.id:
            _log.info("Node is unavailable, delete it {}".format(lost_actor_info['node_id']))
            self.node.storage.get_node(lost_actor_info['node_id'], self._delete_node)

        if not status:
            org_cb(status=response.CalvinResponse(False))
        elif org_cb:
            org_cb(status=org_status)

    def _delete_node(self, key, value):
        _log.debug("Deleting node {} with value {}".format(key, value))
        if not value:
            return

        indexed_public = value['attributes'].get('indexed_public')
        self.node.storage.delete_node(key, indexed_public)
