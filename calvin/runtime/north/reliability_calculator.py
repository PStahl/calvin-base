import math
import scipy
import scipy.stats

from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinconfig

_log = get_logger(__name__)
_conf = calvinconfig.get()


class ReliabilityCalculator(object):

    def __init__(self):
        mtbf = _conf.get('global', 'default_mtbf') or 10  # seconds
        self.default_mtbf = float(mtbf)
        rep_time = _conf.get('global', 'default_replication_time') or 2.0  # seconds
        self.default_replication_time = float(rep_time)
        _log.info("Reliability calculator using default mtbf {} and default replication time {}".format(
            self.default_mtbf, self.default_replication_time))

    def calculate_reliability(self, failure_times, replication_time):
        """
        Calculates and returns the probability that a node (which has experinced len(failure_times) failures)
        does not experince any more failure during time replication_time
        """
        _log.debug("Calculating reliability. Failure info {}, replication_time {}".format(
            failure_times, replication_time))

        # Poisson process
        _lambda = self.failure_rate(failure_times, replication_time)
        _log.debug("Lambda: {}".format(_lambda))
        return math.exp(-_lambda)

    def get_mtbf(self, failure_times):
        MTBF = self.default_mtbf
        _log.debug("Get mtbf from {}".format(failure_times))
        times = sorted(failure_times) if failure_times else []

        if len(times) > 2:
            times = times[-3:]  # use only three latest
            time_between_failures = [(j - i) for i, j in zip(times, times[1:])]
            MTBF = float(sum(time_between_failures)) / len(time_between_failures)

        _log.debug("Calculating mtbf. Failure info {}. mtbf {}".format(
            failure_times, MTBF))

        return MTBF

    def failure_rate(self, failure_times, replication_time):
        _log.debug("Failure times: {}".format(failure_times))
        MTBF = self.get_mtbf(failure_times)
        fr = float(replication_time) / MTBF
        _log.debug("Failure rate: {}".format(fr))
        return fr

    def replication_time(self, replication_times, confidence=0.95):
        """Returns the value for the 'confidence'-percentile.

        Assumes a logistic distribution for the replication times.
        """
        if not replication_times:
            return self.default_replication_time

        _log.debug("Replication times: {}".format(replication_times))
        loc, scale = scipy.stats.logistic.fit(replication_times)
        value = scipy.stats.logistic.ppf(confidence, loc=loc, scale=scale)
        _log.debug("Returning value: {} for confidence level {} and replication times {}".format(value, confidence, replication_times))
        return value
