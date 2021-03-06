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

from calvin.utilities import dynops
from calvin.runtime.south.plugins.async import async
from calvin.runtime.south import endpoint
from calvin.runtime.north.calvin_proto import CalvinTunnel
from calvin.utilities import calvinuuid
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities.calvin_callback import CalvinCB
import calvin.utilities.calvinresponse as response
from calvin.actor.actor_factory import ActorFactory
from calvin.actor.actor import ShadowActor
from calvin.actor.connection_handler import ConnectionHandler

_log = get_logger(__name__)


def log_callback(reply, **kwargs):
    if reply:
        _log.info("%s: %s" % (kwargs['prefix'], reply))


class ActorManager(object):

    """docstring for ActorManager"""

    def __init__(self, node, factory=None, connection_handler=None):
        super(ActorManager, self).__init__()
        self.actors = {}
        self.node = node
        self.factory = factory if factory else ActorFactory(node)
        self.connection_handler = connection_handler if connection_handler else ConnectionHandler(node)

    def new(self, actor_type, args, state=None, prev_connections=None, connection_list=None, callback=None,
            signature=None, app_id=None, master_nodes=[]):
        """
        Instantiate an actor of type 'actor_type'. Parameters are passed in 'args',
        'name' is an optional parameter in 'args', specifying a human readable name.
        Returns actor id on success and raises an exception if anything goes wrong.
        Optionally applies a serialized state to the actor, the supplied args are ignored and args from state
        is used instead.
        Optionally reconnecting the ports, using either
          1) an unmodified connections structure obtained by the connections command supplied as
             prev_connections or,
          2) a mangled list of tuples with (in_node_id, in_port_id, out_node_id, out_port_id) supplied as
             connection_list
        """
        _log.debug("class: %s args: %s state: %s, signature: %s" % (actor_type, args, state, signature))
        callback = CalvinCB(self._after_new, prev_connections=prev_connections, connection_list=connection_list, callback=callback)
        return self._new(actor_type, args, state, signature, app_id, master_nodes, callback=callback)

    def _new(self, actor_type, args, state=None, signature=None, app_id=None, master_nodes=[], callback=None):
        """
        Instantiate an actor of type 'actor_type'. Parameters are passed in 'args',
        'name' is an optional parameter in 'args', specifying a human readable name.
        Returns actor id on success and raises an exception if anything goes wrong.
        """
        _log.analyze(self.node.id, "+", {'actor_type': actor_type, 'state': state})

        return self.factory.create_actor(actor_type=actor_type, state=state, args=args, signature=signature,
                                         app_id=app_id, callback=callback)

    def _after_new(self, actor, status, prev_connections=None, connection_list=None, callback=None):
        if not status:
            if callback:
                callback(status=response.CalvinResponse(False), actor_id=None)
            return

        self.node.control.log_actor_new(actor.id, actor.name, actor._type, isinstance(actor, ShadowActor))
        self.actors[actor.id] = actor

        if callback:
            callback = CalvinCB(callback, actor_id=actor.id)
        self.connection_handler.setup_connections(actor, prev_connections=prev_connections, connection_list=connection_list,
                                                  callback=callback)

    def new_replica(self, actor_type, args, state, prev_connections, app_id, callback):
        """Creates a new replica"""
        _log.debug("Creating new replica of type {}, with args {}, prev_connections {}".format(
            actor_type, args, prev_connections))

        state['id'] = args.pop('id')
        state['name'] = args.pop('name')
        state['set_ports'] = False
        _log.debug("New replica with prev connections: {}".format(prev_connections))

        replica_name = calvinuuid.remove_uuid(state['name'])
        for a in self.actors.values():
            if replica_name == calvinuuid.remove_uuid(a.name) and app_id == a.app_id:
                _log.warning("We have replica {} already, aborting replica request".format(replica_name))
                if callback:
                    callback(status=response.CalvinResponse(False, data={'actor_id': None}))
                return

        callback = CalvinCB(self._after_new_replica, state=state, prev_connections=prev_connections, callback=callback)
        self._new(actor_type, args, state, app_id=app_id, master_nodes=state['master_nodes'], callback=callback)

    def _after_new_replica(self, actor, status, state, prev_connections, callback=None):
        if not status:
            _log.warning("Failed to create new replica: {}".format(status))
            if callback:
                callback(status=response.CalvinResponse(False), actor_id=None)
            return

        self.node.control.log_actor_new(actor.id, actor.name, actor._type, isinstance(actor, ShadowActor))
        self.actors[actor.id] = actor

        if callback:
            callback = CalvinCB(callback, actor_id=actor.id)

        callback = CalvinCB(self._new_replica, actor_id=actor.id, callback=callback)
        self.connection_handler.setup_replica_connections(actor, state, prev_connections, callback)

    def _new_replica(self, status, actor_id, callback):
        actor = self.actors[actor_id]
        ports = actor.inports.values() + actor.outports.values()
        for port in ports:
            if not port.endpoints:
                _log.warning("Replication failed - port has no endpoints")
                status = response.CalvinResponse(False, data=status.data)
                break
            for ep in port.endpoints:
                if isinstance(ep, endpoint.TunnelOutEndpoint) or isinstance(ep, endpoint.TunnelInEndpoint):
                    if ep.tunnel.status != CalvinTunnel.STATUS.WORKING:
                        _log.warning("Replication failed - endpoint is not connected")
                        status = response.CalvinResponse(False, data=status.data)
                        break
        if not status:
            self.delete_actor(actor_id)

        if not isinstance(status, int):
            status = status.status
        if callback:
            callback(status=response.CalvinResponse(status, data={'actor_id': actor_id}))

    def delete_actor(self, actor_id, delete_from_app=False):
        actor = self.actors[actor_id]
        callback = CalvinCB(self._delete_actor_disconnected, actor=actor, delete_from_app=delete_from_app)
        self.node.pm.disconnect(callback=callback, actor_id=actor_id)

    def _delete_actor_disconnected(self, actor, delete_from_app, actor_id, status, *args, **kwargs):
        if not status:
            del self.actors[actor.id]
            _log.warning("Disconnecting of actor {} failed: {}".format(actor_id, status))
            return
        # @TOOD - check order here
        self.node.metering.remove_actor_info(actor.id)
        a = self.actors[actor.id]
        a.will_end()

        # @TOOD - insert callback here
        self.node.storage.delete_actor(actor.id)
        del self.actors[actor.id]

        self.node.control.log_actor_destroy(a.id)
        if delete_from_app and a.app_id:
            self.node.storage.delete_actor_from_app(a.app_id, actor.id)
            self.node.storage.delete_replica_node(a.app_id, self.node.id, actor.name)

        self.node.storage.delete_actor_from_node(self.node.id, actor.id)

        app = self.node.app_manager.applications.get(a.app_id)
        if app:
            app.remove_actor(actor.id)

        self.node.pm.remove_ports_of_actor(a)

    # DEPRECATED: Enabling of an actor is dependent on wether it's connected or not
    def enable(self, actor_id):
        if actor_id in self.actors:
            self.actors[actor_id].enable()

    # DEPRECATED: Disabling of an actor is dependent on wether it's connected or not
    def disable(self, actor_id):
        if actor_id in self.actors:
            self.actors[actor_id].disable()
        else:
            _log.info("!!!FAILED to disable %s", actor_id)

    def update_requirements(self, actor_id, requirements, extend=False, move=False, callback=None):
        """ Update requirements and trigger a potential migration """
        if actor_id not in self.actors:
            # Can only migrate actors from our node
            _log.analyze(self.node.id, "+ NO ACTOR", {'actor_id': actor_id})
            if callback:
                callback(status=response.CalvinResponse(False))
            return
        if not isinstance(requirements, (list, tuple)):
            # requirements need to be list
            _log.analyze(self.node.id, "+ NO REQ LIST", {'actor_id': actor_id})
            if callback:
                callback(status=response.CalvinResponse(response.BAD_REQUEST))
            return
        actor = self.actors[actor_id]
        actor._collect_placement_counter = 0
        actor._collect_placement_last_value = 0
        actor._collect_placement_cb = None
        actor.requirements_add(requirements, extend)
        node_iter = self.node.app_manager.actor_requirements(None, actor_id)
        possible_placements = set([])
        done = [False]
        node_iter.set_cb(self._update_requirements_placements, node_iter, actor_id, possible_placements,
                         move=move, cb=callback, done=done)
        _log.analyze(self.node.id, "+ CALL CB", {'actor_id': actor_id, 'node_iter': str(node_iter)})
        # Must call it since the triggers might already have released before cb set
        self._update_requirements_placements(node_iter, actor_id, possible_placements,
                                 move=move, cb=callback, done=done)
        _log.analyze(self.node.id, "+ END", {'actor_id': actor_id, 'node_iter': str(node_iter)})

    def _update_requirements_placements(self, node_iter, actor_id, possible_placements, done, move=False, cb=None):
        _log.analyze(self.node.id, "+ BEGIN", {}, tb=True)
        actor = self.actors[actor_id]
        if actor._collect_placement_cb:
            actor._collect_placement_cb.cancel()
            actor._collect_placement_cb = None
        if done[0]:
            return
        try:
            while True:
                _log.analyze(self.node.id, "+ ITER", {})
                node_id = node_iter.next()
                possible_placements.add(node_id)
        except dynops.PauseIteration:
            _log.analyze(self.node.id, "+ PAUSED",
                    {'counter': actor._collect_placement_counter,
                     'last_value': actor._collect_placement_last_value,
                     'diff': actor._collect_placement_counter - actor._collect_placement_last_value})
            # FIXME the dynops should be self triggering, but is not...
            # This is a temporary fix by keep trying
            delay = 0.0 if actor._collect_placement_counter > actor._collect_placement_last_value + 100 else 0.2
            actor._collect_placement_counter += 1
            actor._collect_placement_cb = async.DelayedCall(delay, self._update_requirements_placements,
                                                    node_iter, actor_id, possible_placements, done=done,
                                                     move=move, cb=cb)
            return
        except StopIteration:
            # all possible actor placements derived
            _log.analyze(self.node.id, "+ ALL", {})
            done[0] = True
            if move and len(possible_placements)>1:
                possible_placements.discard(self.node.id)
            if not possible_placements:
                if cb:
                    cb(status=response.CalvinResponse(False))
                return
            if self.node.id in possible_placements:
                # Actor could stay, then do that
                if cb:
                    cb(status=response.CalvinResponse(True))
                return
            # TODO do a better selection between possible nodes
            self.migrate(actor_id, possible_placements.pop(), callback=cb)
            _log.analyze(self.node.id, "+ END", {})
        except:
            _log.exception("actormanager:_update_requirements_placements")

    def migrate(self, actor_id, node_id, callback=None):
        """ Migrate an actor actor_id to peer node node_id """
        _log.info("Migrate actor {} to node {}".format(actor_id, node_id))
        if actor_id not in self.actors:
            _log.warning("Trying to migrate non-local actor {}, aborting".format(actor_id))
            # Can only migrate actors from our node
            if callback:
                callback(status=response.CalvinResponse(False))
            return
        if node_id == self.node.id:
            _log.warning("Trying to migrate actor {} to same node, aborting".format(actor_id))
            # No need to migrate to ourself
            if callback:
                callback(status=response.CalvinResponse(True))
            return
        actor = self.actors[actor_id]
        actor._migrating_to = node_id
        actor.will_migrate()
        actor_type = actor._type
        ports = actor.connections(self.node.id)
        # Disconnect ports and continue in _migrate_disconnect
        callback = CalvinCB(self._migrate_disconnected,
                            actor=actor,
                            actor_type=actor_type,
                            ports=ports,
                            node_id=node_id,
                            callback=callback)
        self.node.pm.disconnect(callback=callback, actor_id=actor_id)

    def _migrate_disconnected(self, actor, actor_type, ports, node_id, status, callback=None, **state):
        """ Actor disconnected, continue migration """
        if status:
            state = actor.state()
            self.delete_actor(actor.id)
            if actor.app_id:
                self.node.storage.delete_replica_node(actor.app_id, self.node.id, actor.name, cb=None)
            self.node.proto.actor_new(node_id, callback, actor_type, state, ports, app_id=actor.app_id)
        elif callback:  # FIXME handle errors!!!
            callback(status=status)

    def replicate(self, actor_id, node_id, callback=None):
        """Replicate an actor actor_id to peer node node_id """
        if actor_id not in self.actors:
            _log.warning("Failed to replicate {} to {}. Can only replicate actors from our node.".format(
                actor_id, node_id))
            # Can only replicate actors from our node
            if callback:
                _log.warning("Can only replicate actors from our node")
                callback(status=response.CalvinResponse(False))
            return

        actor = self.actors[actor_id]
        if actor.fsm.state() not in [actor.STATUS.PENDING, actor.STATUS.ENABLED]:
            if callback:
                _log.warning("Can only replicate pending or enabled actors, current state: {}".format(actor.fsm.state()))
                callback(status=response.CalvinResponse(False))
            return

        actor_type = actor._type
        prev_connections = actor.connections(self.node.id)
        prev_connections['port_names'] = actor.port_names()
        state = actor.state()

        args = actor.replication_args()
        app = self.node.app_manager.get_actor_app(actor_id)
        app_id = app.id if app else state['app_id']

        self.node.proto.actor_replication(node_id, callback, actor_type, state, prev_connections, args, app_id)

    def peernew_to_local_cb(self, reply, **kwargs):
        if kwargs['actor_id'] == reply:
            # Managed to setup since new returned same actor id
            self.node.set_local_reply(kwargs['lmsg_id'], "OK")
        else:
            # Just pass on new cmd reply if it failed
            self.node.set_local_reply(kwargs['lmsg_id'], reply)

    def connections(self, actor_id):
        if actor_id not in self.actors:
            return

        return self.connection_handler.connections(self.actors[actor_id])

    def dump(self, actor_id):
        actor = self.actors.get(actor_id, None)
        if not actor:
            raise Exception("Actor '%s' not found" % (actor_id,))
        _log.debug("-----------")
        _log.debug(actor)
        _log.debug("-----------")

    def set_port_property(self, actor_id, port_type, port_name, port_property, value):
        try:
            actor = self.actors[actor_id]
        except Exception as e:
            _log.exception("Actor '%s' not found" % (actor_id,))
            raise e
        success = actor.set_port_property(port_type, port_name, port_property, value)
        return 'OK' if success else 'FAILURE'

    def get_port_state(self, actor_id, port_id):
        try:
            actor = self.actors[actor_id]
        except Exception as e:
            _log.exception("Actor '%s' not found" % (actor_id,))
            raise e

        for port in actor.inports.values():
            if port.id == port_id:
                return port.fifo._state()
        for port in actor.outports.values():
            if port.id == port_id:
                return port.fifo._state()
        raise Exception("No port with id: %s" % port_id)

    def actor_type(self, actor_id):
        actor = self.actors.get(actor_id, None)
        return actor._type if actor else 'BAD ACTOR'

    def report(self, actor_id):
        actor = self.actors.get(actor_id)
        if not actor:
            _log.warning("Did not find actor with id {}".format(actor_id))
            return []

        return actor.report()

    def enabled_actors(self):
        return [actor for actor in self.actors.values() if actor.enabled()]

    def list_actors(self):
        return self.actors.keys()
