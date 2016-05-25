import time

from calvin.runtime.north.replicator import Replicator
from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinuuid
from calvin.utilities import calvinconfig

_log = get_logger(__name__)
_conf = calvinconfig.get()


class AppMonitor(object):
    def __init__(self, node, app_manager, storage):
        self.node = node
        self._monitor_count = 0
        self.app_manager = app_manager
        self.storage = storage
        freq = _conf.get('global', 'app_monitor_frequency') or 0
        self._frequency = int(freq)

    def check_reliabilities(self):
        self._monitor_count += 1

        if self._monitor_count == self._frequency:
            self._monitor_count = 0
            start_time = time.time()
            for app in self.app_manager.applications:
                self.check_app_reliability(app, start_time)

    def check_app_reliability(self, app_id, start_time):
        self.storage.get_application(app_id, cb=CalvinCB(self._check_app_reliability, start_time=start_time))

    def _check_app_reliability(self, key, value, start_time):
        if not value:
            _log.error("Failed to get app from storage: {}".format(key))

        self.storage.get_application_actors(key, cb=CalvinCB(self._check_reliability, app_info=value, start_time=start_time))

    def _check_reliability(self, key, value, app_info, start_time):
        _log.debug("Check reliability for app: {}".format(key))
        if not value:
            _log.warning("Failed to get application actors from storage: {}".format(key))
            return
        _log.debug("Checking reliability for actors: {}".format(value))
        self._check_actors_reliability(actors=value, app_info=app_info, names=[], index=0, start_time=start_time)

    def _check_actors_reliability(self, actors, app_info, names, index, start_time, status=None):
        if index == len(actors):
            return

        actor = actors[index]
        self.storage.get_actor(actor, CalvinCB(self._check_actor_reliability, actors=actors, app_info=app_info,
                                               names=names, index=index, start_time=start_time))

    def _check_actor_reliability(self, key, value, actors, app_info, names, index, start_time):
        _log.debug("Check reliability for actor: {}".format(key))
        if not value:
            _log.warning("Failed to get actor info from storage: {}".format(key))
            return self._check_actors_reliability(actors=actors, app_info=app_info, names=names, index=index + 1, start_time=start_time)

        name = calvinuuid.remove_uuid(value['name'])
        if value["replicate"] and name not in names:
            try:
                replicator = Replicator(self.node, key, value, app_info['required_reliability'], do_delete=False)
            except Exception as e:
                _log.warning("Failed to create replicator: {}".format(e))
                return
            names.append(name)
            cb = CalvinCB(self._check_actors_reliability, actors=actors, app_info=app_info, names=names, index=index + 1, start_time=start_time)
            replicator.replicate_lost_actor(cb=cb, start_time=start_time)
        else:
            if name in names:
                _log.debug("Already checked reliability of actor: {}".format(name))
            self._check_actors_reliability(actors=actors, app_info=app_info, names=names, index=index + 1, start_time=start_time)
