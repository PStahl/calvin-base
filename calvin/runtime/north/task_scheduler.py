from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinconfig

_log = get_logger(__name__)
_conf = calvinconfig.get()


class ReliabilityScheduler(object):

    def __init__(self, resource_manager):
        self.resource_manager = resource_manager  # in case this is needed

    def sort(self, node_ids, replication_times, failure_info):
        """Returns the nodes sorted by preference with the most reliable first"""
        node_ids = [(node_id, self.resource_manager.get_reliability(node_id, replication_times, failure_info)) for node_id in node_ids]
        _log.info("Sorting nodes {} after reliability {}".format([x[0] for x in node_ids], [x[1] for x in node_ids]))
        node_ids.sort(key=lambda x: (x[1], x[0]), reverse=True)
        return [x[0] for x in node_ids]
