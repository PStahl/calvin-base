import sys
import time
import operator
import socket
import re

from collections import defaultdict, deque
from calvin.runtime.north.reliability_calculator import ReliabilityCalculator

from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

DEFAULT_HISTORY_SIZE = 20
DEFAULT_REPLICATION_HISTORY_SIZE = 5
DEFAULT_REPLICATION_TIME = 2.0
DEFAULT_NODE_REALIABILITY = 0.8
LOST_NODE_TIME = 0.5
MAX_PREFERRED_USAGE = 80


class ResourceManager(object):
    def __init__(self, node, history_size=DEFAULT_HISTORY_SIZE):
        self.node = node
        self.history_size = history_size
        self.usages = defaultdict(lambda: deque(maxlen=self.history_size))
        self.reliability_calculator = ReliabilityCalculator()
        self.node_uris = {}
        self.test_sync = 2
        self._lost_nodes = set()
        self._rep_times = {}

    @property
    def lost_node_time(self):
        return self.node.heartbeat_timeout

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

            self.node_uris[node_id] = "{}:{}".format(addr, port)

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
        uri = self.node_uris.get(node_id)
        if uri:
            replication_time = self.replication_time(replication_times)
            if uri in failure_info:
                failure_info = failure_info[uri]
            else:
                failure_info = []
            return self.reliability_calculator.calculate_reliability(failure_info, replication_time)
        else:
            return DEFAULT_NODE_REALIABILITY

    def replication_time(self, replication_times):
        if not replication_times:
            return DEFAULT_REPLICATION_TIME

        key = "".join([str(t) for t in replication_times])
        if key in self._rep_times:
            return self._rep_times[key]

        value = self.reliability_calculator.replication_time(replication_times) + self.lost_node_time
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
        """Sorts after reliability"""
        node_ids = [(node_id, self.get_reliability(node_id, replication_times, failure_info)) for node_id in node_ids]
        node_ids.sort(key=lambda x: (x[1], x[0]), reverse=True)
        _log.debug("Sorting nodes {} after reliability {}".format([x[0] for x in node_ids], [x[1] for x in node_ids]))
        return [x[0] for x in node_ids]

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
