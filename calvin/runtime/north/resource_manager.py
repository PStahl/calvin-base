import sys
import operator
import socket
import re

from collections import defaultdict, deque
from calvin.runtime.north.reliability_calculator_factory import get_reliability_calculator
from calvin.runtime.north.task_scheduler_factory import get_task_scheduler

from calvin.utilities import calvinconfig
from calvin.utilities.calvinlogger import get_logger

_conf = calvinconfig.get()
_log = get_logger(__name__)

DEFAULT_HISTORY_SIZE = 20
DEFAULT_REPLICATION_HISTORY_SIZE = 5
DEFAULT_REPLICATION_TIME = 2.0
DEFAULT_NODE_RELIABILITY = 0.8
LOST_NODE_TIME = 0.5
MAX_PREFERRED_USAGE = 80


class ResourceManager(object):
    def __init__(self, history_size=DEFAULT_HISTORY_SIZE):
        self.history_size = history_size
        self.usages = defaultdict(lambda: deque(maxlen=self.history_size))
        rc = _conf.get('global', 'reliability_calculator')
        self.reliability_calculator = get_reliability_calculator(rc)
        ts = _conf.get('global', 'task_scheduler')
        self.task_scheduler = get_task_scheduler(ts, self)
        _log.info("Starting resource manager using reliability calculator {} and task scheduler {}".format(rc, ts))
        self.node_uris = {}
        self.node_ids = {}
        self.test_sync = 2
        self._lost_nodes = set()
        self._rep_times = {}

    def register_uri(self, node_id, uri):
        _log.debug("Registering uri: {} - {}".format(node_id, uri))
        if isinstance(uri, list):
            uri = uri[0]

        if uri:
            uri = uri.replace("calvinip://", "").replace("http://", "")
            addr = uri.split(":")[0]
            port = int(uri.split(":")[1])

            is_ip = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", addr)
            if not is_ip:
                addr = socket.gethostbyname(addr)

            uri = "{}:{}".format(addr, port)
            self.node_uris[node_id] = uri
            self.node_ids[uri] = node_id

    def get_id(self, uri):
        uri = uri.replace("calvinip://", "").replace("http://", "")
        addr = uri.split(":")[0]
        port = int(uri.split(":")[1])

        is_ip = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", addr)
        if not is_ip:
            addr = socket.gethostbyname(addr)

        return self.node_ids.get("{}:{}".format(addr, port))

    def register(self, node_id, usage, uri):
        _log.debug("Registering resource usage for node {}: {} with uri {}".format(node_id, usage, uri))
        self.register_uri(node_id, uri)

        if usage:
            self.usages[node_id].append(usage['cpu_percent'])

    def lost_node(self, node_id):
        if node_id in self._lost_nodes:
            return
        self._lost_nodes.add(node_id)
        del self.usages[node_id]
        _log.debug("Registering lost node: {}".format(node_id))

    def _average_usage(self, node_id):
        return sum(self.usages[node_id]) / max(len(self.usages[node_id]), 1)

    #Only used for evaluation
    def get_avg_usages(self):
        usages = {}
        for node_id in self.usages.keys():
            uri = self.node_uris.get(node_id)
            if uri:
                usages[uri] = self._average_usage(node_id)
        return usages

    def least_busy(self):
        """Returns the id of the node with the lowest average CPU usage"""
        min_usage, least_busy = sys.maxint, None
        for node_id in self.usages.keys():
            average = self._average_usage(node_id)
            if average < min_usage:
                min_usage = average
                least_busy = node_id

        return least_busy

    def most_busy(self):
        """Returns the id of the node with the highest average CPU usage"""
        min_usage, most_busy = - sys.maxint, None
        for node_id in self.usages.keys():
            average = self._average_usage(node_id)
            if average > min_usage:
                min_usage = average
                most_busy = node_id

        return most_busy

    def get_reliability(self, node_id, replication_times, failure_info):
        _log.info("Getting reliability for {}".format(node_id))
        uri = self.node_uris.get(node_id)
        if uri:
            replication_time = self.replication_time(replication_times)
            if uri in failure_info:
                failure_info = failure_info[uri]
            else:
                failure_info = []
            rel = self.reliability_calculator.calculate_reliability(failure_info, replication_time)
            _log.info("Returning reliability for {}: {}".format(node_id, rel))
            return rel
        else:
            _log.info("Returning default reliability: {}".format(DEFAULT_NODE_RELIABILITY))
            return DEFAULT_NODE_RELIABILITY

    def replication_time(self, replication_times):
        if not replication_times:
            return DEFAULT_REPLICATION_TIME

        key = "".join([str(t) for t in replication_times])
        if key in self._rep_times:
            return self._rep_times[key]

        value = self.reliability_calculator.replication_time(replication_times) + LOST_NODE_TIME
        self._rep_times[key] = value
        return value

    def get_preferred_nodes(self, node_ids):
        preferred = []
        for node_id in node_ids:
            if self._average_usage(node_id) < MAX_PREFERRED_USAGE:
                preferred.append(node_id)
        return preferred

    def _update_deque(self, new_values, old_values):
        for tup in new_values:
            if tup[0] > old_values[-1][0]:
                old_values.append(tup)

    def sort_nodes_reliability(self, node_ids, replication_times, failure_info):
        return self.task_scheduler.sort(node_ids, replication_times, failure_info)

    def current_reliability(self, current_nodes, replication_times, failure_info):
        current_nodes = list(set(current_nodes))
        _log.debug("Calculating reliability for nodes: {}".format(current_nodes))
        failure = []
        for node_id in current_nodes:
            f = 1 - self.get_reliability(node_id, replication_times, failure_info)
            _log.debug("Failure for {}: {}".format(node_id, f))
            failure.append(f)

        p = 1 - reduce(operator.mul, failure, 1)
        _log.info("Reliability for nodes {} is {}".format(current_nodes, p))
        return p

    def sync_info(self, usages=None):
        if usages:
            self._sync_usages(usages)

        usages = {}
        for (node_id, usage_list) in self.usages.iteritems():
            usages[node_id] = [usage for usage in usage_list]

        return usages

    def _sync_usages(self, usages):
        """
        Sync the usages for each node_id stored on another node.
        usages is a dict of lists but stored as a dict with deques
        """
        _log.debug("\n\nSyncing usages {} with usages {}".format(self.usages, usages))
        for (node_id, usage_list) in usages.iteritems():
            if (not node_id in self.usages.keys() or len(usage_list) > self.usages[node_id]) and not node_id in self._lost_nodes:
                usage_deq = deque(maxlen=self.history_size)
                for u in usage_list:
                    usage_deq.append(u)
                self.usages[node_id] = usage_deq
