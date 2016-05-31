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

import re
import os
import unittest
import time
import pytest
import multiprocessing
import copy
import operator
from collections import Counter

from calvin.tests.helpers import expected_tokens, actual_tokens
from calvin.Tools import cscompiler as compiler
from calvin.Tools import deployer
from calvin.Tools.csruntime import csruntime
from calvin.utilities import calvinconfig
from calvin.utilities import utils
from calvin.utilities.nodecontrol import dispatch_node
from calvin.utilities.attribute_resolver import format_index_string
from calvin.utilities import calvinlogger
from calvin.utilities import calvinuuid


_log = calvinlogger.get_logger(__name__)
_conf = calvinconfig.get()

runtime = None
runtimes = []
peerlist = []
kill_peers = True
ip_addr = None



def setup_module(module):
    global runtime
    global runtimes
    global peerlist
    global kill_peers
    global ip_addr
    bt_master_controluri = None

    os.environ["CALVIN_TESTING"] = "1"

    try:
        ip_addr = os.environ["CALVIN_TEST_IP"]
        purpose = os.environ["CALVIN_TEST_UUID"]
    except KeyError:
        pass

    if ip_addr is None:
        # Bluetooth tests assumes one master runtime with two connected peers
        # CALVIN_TEST_BT_MASTERCONTROLURI is the control uri of the master runtime
        try:
            bt_master_controluri = os.environ["CALVIN_TEST_BT_MASTERCONTROLURI"]
            _log.debug("Running Bluetooth tests")
        except KeyError:
            pass

    if ip_addr:
        remote_node_count = 2
        kill_peers = False
        test_peers = None


        import socket
        ports=[]
        for a in range(2):
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(('', 0))
            addr = s.getsockname()
            ports.append(addr[1])
            s.close()

        runtime,_ = dispatch_node(["calvinip://%s:%s" % (ip_addr, ports[0])], "http://%s:%s" % (ip_addr, ports[1]))

        _log.debug("First runtime started, control http://%s:%s, calvinip://%s:%s" % (ip_addr, ports[1], ip_addr, ports[0]))

        interval = 0.5
        for retries in range(1,20):
            time.sleep(interval)
            _log.debug("Trying to get test nodes for 'purpose' %s" % purpose)
            test_peers = utils.get_index(runtime, format_index_string({'node_name':
                                                                         {'organization': 'com.ericsson',
                                                                          'purpose': purpose}
                                                                      }))
            if not test_peers is None and not test_peers["result"] is None and \
                    len(test_peers["result"]) == remote_node_count:
                test_peers = test_peers["result"]
                break

        if test_peers is None or len(test_peers) != remote_node_count:
            _log.debug("Failed to find all remote nodes within time, peers = %s" % test_peers)
            raise Exception("Not all nodes found dont run tests, peers = %s" % test_peers)

        test_peer2_id = test_peers[0]
        test_peer2 = utils.get_node(runtime, test_peer2_id)
        if test_peer2:
            runtime2 = utils.RT(test_peer2["control_uri"])
            runtime2.id = test_peer2_id
            runtime2.uri = test_peer2["uri"]
            runtimes.append(runtime2)
        test_peer3_id = test_peers[1]
        if test_peer3_id:
            test_peer3 = utils.get_node(runtime, test_peer3_id)
            if test_peer3:
                runtime3 = utils.RT(test_peer3["control_uri"])
                runtime3.id = test_peer3_id
                runtime3.uri = test_peer3["uri"]
                runtimes.append(runtime3)
    elif bt_master_controluri:
        runtime = utils.RT(bt_master_controluri)
        bt_master_id = utils.get_node_id(runtime)
        data = utils.get_node(runtime, bt_master_id)
        if data:
            runtime.id = bt_master_id
            runtime.uri = data["uri"]
            test_peers = utils.get_nodes(runtime)
            test_peer2_id = test_peers[0]
            test_peer2 = utils.get_node(runtime, test_peer2_id)
            if test_peer2:
                rt2 = utils.RT(test_peer2["control_uri"])
                rt2.id = test_peer2_id
                rt2.uri = test_peer2["uri"]
                runtimes.append(rt2)
            test_peer3_id = test_peers[1]
            if test_peer3_id:
                test_peer3 = utils.get_node(runtime, test_peer3_id)
                if test_peer3:
                    rt3 = utils.RT(test_peer3["control_uri"])
                    rt3.id = test_peer3_id
                    rt3.uri = test_peer3["uri"]
                    runtimes.append(rt3)
    else:
        try:
            ip_addr = os.environ["CALVIN_TEST_LOCALHOST"]
        except:
            import socket
            ip_addr = socket.gethostbyname(socket.gethostname())
        localhost = "calvinip://%s:5000" % (ip_addr,), "http://localhost:5001"
        remotehosts = [("calvinip://%s:%d" % (ip_addr, d), "http://localhost:%d" % (d+1)) for d in range(5002, 5005, 2)]
        # remotehosts = [("calvinip://127.0.0.1:5002", "http://localhost:5003")]

        for host in remotehosts:
            runtimes += [dispatch_node([host[0]], host[1])[0]]

        runtime, _ = dispatch_node([localhost[0]], localhost[1])

        time.sleep(1)

        # FIXME When storage up and running peersetup not needed, but still useful during testing
        utils.peer_setup(runtime, [i[0] for i in remotehosts])

        time.sleep(0.5)
        """

        # FIXME Does not yet support peerlist
        try:
            self.peerlist = peerlist(
                self.runtime, self.runtime.id, len(remotehosts))

            # Make sure all peers agree on network
            [peerlist(self.runtime, p, len(self.runtimes)) for p in self.peerlist]
        except:
            self.peerlist = []
        """

    peerlist = [rt.control_uri for rt in runtimes]
    print "SETUP DONE ***", peerlist


def teardown_module(module):
    global runtime
    global runtimes
    global kill_peers

    if kill_peers:
        for peer in runtimes:
            utils.quit(peer)
            time.sleep(0.2)
    utils.quit(runtime)
    time.sleep(0.2)
    for p in multiprocessing.active_children():
        p.terminate()
        time.sleep(0.2)


class CalvinTestBase(unittest.TestCase):

    def assert_list_prefix(self, expected, actual, allow_empty=False):
        assert actual
        if len(expected) > len(actual):
            self.assertListEqual(expected[:len(actual)], actual)
        elif len(expected) < len(actual):
            self.assertListEqual(expected, actual[:len(expected)])
        else:
            self.assertListEqual(expected, actual)

    def assert_list_postfix(self, expected, actual):
        assert expected
        assert actual
        if len(actual) > len(expected):
            return False
        if len(actual) == len(expected):
            return actual == expected

        index = expected.index(actual[0])
        return self.assertListEqual(expected[index:], actual)

    def setUp(self):
        self.runtime = runtime
        self.runtimes = runtimes
        self.peerlist = peerlist
        self.actors = {}
        self.deployer = None

    def tearDown(self):
        for (a_id, a_rt) in self.actors.iteritems():
            if a_id:
                try:
                    utils.delete_actor(a_rt, a_id)
                except Exception as e:
                    print e
        self.actors = {}
        if self.deployer:
            self.deployer.destroy()
        self.deployer = None


@pytest.mark.slow
@pytest.mark.essential
class TestNodeSetup(CalvinTestBase):

    """Testing starting a node"""

    def testStartNode(self):
        """Testing starting node"""

        print "### testStartNode ###", self.runtime
        rt, id_, peers = self.runtime, self.runtime.id, self.peerlist
        print "GOT RT"
        assert utils.get_node(rt, id_)['uri'] == rt.uri
        print "GOT URI", rt.uri


@pytest.mark.essential
@pytest.mark.slow
class TestActorDeletion(CalvinTestBase):

    """Testing deletion of actor"""

    def testDeleteLocalActor(self):
        rt = self.runtime

        script = """
            src : std.CountTimer()
            snk : io.StandardOut(store_tokens=1)
            src.integer > snk.token
        """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        app_id = d.deploy()
        self.deployer = d

        time.sleep(0.2)

        actors = utils.get_application_actors(rt, app_id)
        snk = d.actor_map['simple:snk']

        assert snk in actors
        utils.delete_actor(rt, snk)
        time.sleep(0.2)

        self.assertIsNone(utils.get_actor(rt, snk))
        assert snk not in utils.get_application_actors(rt, app_id)

    def testDeleteMigratedActor(self):
        rt = self.runtime
        peer = self.runtimes[0]

        script = """
            src : std.CountTimer()
            snk : io.StandardOut(store_tokens=1)
            src.integer > snk.token
        """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        app_id = d.deploy()
        self.deployer = d

        snk = d.actor_map['simple:snk']

        time.sleep(0.2)
        utils.migrate(rt, snk, peer.id)
        time.sleep(0.2)

        assert snk in utils.get_application_actors(rt, app_id)
        assert snk in utils.get_application_actors(peer, app_id)

        utils.delete_actor(peer, snk)
        time.sleep(3)

        snk = d.actor_map['simple:snk']

        self.assertIsNone(utils.get_actor(rt, snk))
        self.assertIsNone(utils.get_actor(peer, snk))

    def testDeleteRemoteActor(self):
        rt = self.runtime
        peer = self.runtimes[0]

        script = """
            src : std.CountTimer()
            snk : io.StandardOut(store_tokens=1)
            src.integer > snk.token
        """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        app_id = d.deploy()
        self.deployer = d

        snk = d.actor_map['simple:snk']

        time.sleep(0.2)
        utils.migrate(rt, snk, peer.id)
        time.sleep(0.2)

        assert snk in utils.get_application_actors(rt, app_id)
        assert snk in utils.get_application_actors(peer, app_id)

        utils.delete_actor(rt, snk)
        time.sleep(3)

        snk = d.actor_map['simple:snk']

        self.assertIsNone(utils.get_actor(rt, snk))
        self.assertIsNone(utils.get_actor(peer, snk))

    def testDeleteLocalActorRemotely(self):
        rt = self.runtime
        peer = self.runtimes[0]

        script = """
            src : std.CountTimer()
            snk : io.StandardOut(store_tokens=1)
            src.integer > snk.token
        """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        app_id = d.deploy()
        self.deployer = d

        snk = d.actor_map['simple:snk']

        time.sleep(0.2)

        assert snk in utils.get_application_actors(rt, app_id)
        assert snk in utils.get_application_actors(peer, app_id)

        utils.delete_actor(peer, snk)
        time.sleep(3)

        snk = d.actor_map['simple:snk']

        self.assertIsNone(utils.get_actor(rt, snk))
        self.assertIsNone(utils.get_actor(peer, snk))

@pytest.mark.essential
@pytest.mark.slow
class TestLocalConnectDisconnect(CalvinTestBase):

    """Testing local connect/disconnect/re-connect"""

    def testLocalSourceSink(self):
        """Testing local source and sink"""

        rt, id_, _ = self.runtime, self.runtime.id, self.peerlist

        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        utils.connect(rt, snk, 'token', id_, src, 'integer')
        self.actors.update({src:rt, snk:rt})

        time.sleep(0.4)

        # disable(rt, id_, src)
        utils.disconnect(rt, src)

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual = actual_tokens(rt, snk)

        self.assert_list_prefix(expected, actual)

    def testLocalConnectDisconnectSink(self):
        """Testing local connect/disconnect/re-connect on sink"""

        rt, id_ = self.runtime, self.runtime.id

        src = utils.new_actor(rt, "std.CountTimer", "src")
        snk = utils.new_actor_wargs(rt, "io.StandardOut", "snk", store_tokens=1)
        utils.connect(rt, snk, 'token', id_, src, 'integer')
        self.actors.update({src:rt, snk:rt})
        time.sleep(0.2)

        utils.disconnect(rt, snk)
        utils.connect(rt, snk, 'token', id_, src, 'integer')
        time.sleep(0.2)
        utils.disconnect(rt, snk)
        # disable(rt, id_, src)

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual = actual_tokens(rt, snk)
        self.assert_list_prefix(expected, actual)

    def testLocalConnectDisconnectSource(self):
        """Testing local connect/disconnect/re-connect on source"""

        rt, id_ = self.runtime, self.runtime.id

        src = utils.new_actor(rt, "std.CountTimer", "src")
        snk = utils.new_actor_wargs(rt, "io.StandardOut", "snk", store_tokens=1)
        self.actors.update({src:rt, snk:rt})

        utils.connect(rt, snk, "token", id_, src, "integer")
        time.sleep(0.2)
        utils.disconnect(rt, src)

        utils.connect(rt, snk, "token", id_, src, "integer")
        time.sleep(0.2)
        utils.disconnect(rt, src)
        #disable(rt, id_, src)

        expected = expected_tokens(rt, src, "std.CountTimer")
        actual = actual_tokens(rt, snk)
        self.assert_list_prefix(expected, actual)

    def testLocalConnectSourceWithMultipleSinks(self):
        """Testing local connect/disconnect/re-connect on source with multiple sinks"""

        rt, rt_id = self.runtime, self.runtime.id

        src = utils.new_actor(rt, "std.CountTimer", "src")
        snk = utils.new_actor_wargs(rt, "io.StandardOut", "snk", store_tokens=1)
        snk_2 = utils.new_actor_wargs(rt, "io.StandardOut", "snk_2", store_tokens=1)
        self.actors.update({src:rt, snk:rt, snk_2:rt})

        utils.connect(rt, snk, "token", rt_id, src, "integer")
        time.sleep(0.2)
        utils.connect(rt, snk_2, "token", rt_id, src, "integer")
        time.sleep(0.2)

        utils.disconnect(rt, src)

        utils.connect(rt, snk, "token", rt_id, src, "integer")
        time.sleep(0.2)
        utils.connect(rt, snk_2, "token", rt_id, src, "integer")

        expected = expected_tokens(rt, src, "std.CountTimer")
        actual_1 = actual_tokens(rt, snk)
        actual_2 = actual_tokens(rt, snk)
        self.assert_list_prefix(expected, actual_1)
        self.assert_list_prefix(expected, actual_2)

    def testLocalConnectSinkWithMultipleSources(self):
        """Testing local connect/disconnect/re-connect on source with multiple sinks"""

        rt, rt_id = self.runtime, self.runtime.id

        src = utils.new_actor(rt, "std.CountTimer", "src")
        src_2 = utils.new_actor(rt, "std.CountTimer", "src_2")
        snk = utils.new_actor_wargs(rt, "io.StandardOut", "snk", store_tokens=1)
        self.actors.update({src:rt, src_2:rt, snk:rt})

        utils.connect(rt, snk, "token", rt_id, src, "integer")
        utils.connect(rt, snk, "token", rt_id, src_2, "integer")
        time.sleep(0.2)

        utils.disconnect(rt, snk)

        utils.connect(rt, snk, "token", rt_id, src, "integer")
        time.sleep(0.2)
        utils.connect(rt, snk, "token", rt_id, src_2, "integer")
        time.sleep(.1)

        expected_1 = expected_tokens(rt, src, "std.CountTimer")
        expected_2 = expected_tokens(rt, src_2, "std.CountTimer")
        expected = sorted(expected_1 + expected_2)
        actual = sorted(actual_tokens(rt, snk))
        self.assert_list_prefix(expected, actual)

    def testLocalConnectDisconnectFilter(self):
        """Testing local connect/disconnect/re-connect on filter"""

        rt, id_ = self.runtime, self.runtime.id

        src = utils.new_actor(rt, "std.CountTimer", "src")
        sum_ = utils.new_actor(rt, "std.Sum", "sum")
        snk = utils.new_actor_wargs(rt, "io.StandardOut", "snk", store_tokens=1)
        self.actors.update({src:rt, sum_:rt, snk:rt})

        utils.connect(rt, snk, "token", id_, sum_, "integer")
        utils.connect(rt, sum_, "integer", id_, src, "integer")

        time.sleep(0.2)

        utils.disconnect(rt, sum_)

        utils.connect(rt, snk, "token", id_, sum_, "integer")
        utils.connect(rt, sum_, "integer", id_, src, "integer")

        time.sleep(0.2)

        utils.disconnect(rt, src)

        expected = expected_tokens(rt, src, "std.Sum")
        actual = actual_tokens(rt, snk)
        self.assert_list_prefix(expected, actual)

    def testTimerLocalSourceSink(self):
        """Testing timer based local source and sink"""

        rt, id_, = self.runtime, self.runtime.id

        src = utils.new_actor_wargs(
            rt, 'std.CountTimer', 'src', sleep=0.1, steps=10)
        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        utils.connect(rt, snk, 'token', id_, src, 'integer')
        self.actors.update({src:rt, snk:rt})

        time.sleep(1.2)

        # disable(rt, id_, src)
        utils.disconnect(rt, src)

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual = actual_tokens(rt, snk)

        self.assert_list_prefix(expected, actual)
        self.assertTrue(len(actual) > 0)


@pytest.mark.essential
@pytest.mark.slow
class TestRemoteConnection(CalvinTestBase):

    """Testing remote connections"""

    def testRemoteOneActor(self):
        """Testing remote port"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        sum_ = utils.new_actor(peer, 'std.Sum', 'sum')
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, sum_:peer, src:rt})

        utils.connect(rt, snk, 'token', peer_id, sum_, 'integer')
        utils.connect(peer, sum_, 'integer', id_, src, 'integer')
        time.sleep(0.5)

        utils.disable(rt, src)

        expected = expected_tokens(rt, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        self.assert_list_prefix(expected, actual)

    def testRemoteSlowPort(self):
        """Testing remote slow port and that token flow control works"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        snk1 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk1', store_tokens=1)
        alt = utils.new_actor(peer, 'std.Alternate', 'alt')
        src1 = utils.new_actor_wargs(rt, 'std.CountTimer', 'src1', sleep=0.1, steps=100)
        src2 = utils.new_actor_wargs(rt, 'std.CountTimer', 'src2', sleep=1.0, steps=10)
        self.actors.update({snk1:rt, alt:peer, src1:rt, src2:rt})

        utils.connect(rt, snk1, 'token', peer_id, alt, 'token')
        utils.connect(peer, alt, 'token_1', id_, src1, 'integer')
        utils.connect(peer, alt, 'token_2', id_, src2, 'integer')
        time.sleep(2)

        utils.disable(rt, src1)
        utils.disable(rt, src2)
        time.sleep(0.2)  # HACK

        def _d():
            for i in range(1,100):
                yield i
                yield i

        expected = list(_d())
        actual = actual_tokens(rt, snk1)
        assert(len(actual) > 1)
        self.assert_list_prefix(expected, actual)

    def testRemoteSlowFanoutPort(self):
        """Testing remote slow port with fan out and that token flow control works"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        snk1 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk1', store_tokens=1)
        snk2 = utils.new_actor_wargs(peer, 'io.StandardOut', 'snk2', store_tokens=1)
        alt = utils.new_actor(peer, 'std.Alternate', 'alt')
        src1 = utils.new_actor_wargs(rt, 'std.CountTimer', 'src1', sleep=0.1, steps=100)
        src2 = utils.new_actor_wargs(rt, 'std.CountTimer', 'src2', sleep=1.0, steps=10)
        self.actors.update({snk1:rt, snk2:peer, alt:peer, src1:rt, src2:rt})

        utils.connect(rt, snk1, 'token', peer_id, alt, 'token')
        utils.connect(peer, snk2, 'token', id_, src1, 'integer')
        utils.connect(peer, alt, 'token_1', id_, src1, 'integer')
        utils.connect(peer, alt, 'token_2', id_, src2, 'integer')
        time.sleep(2)

        utils.disable(rt, src1)
        utils.disable(rt, src2)
        time.sleep(0.2)  # HACK

        def _d():
            for i in range(1,100):
                yield i
                yield i

        expected = list(_d())
        actual = actual_tokens(rt, snk1)
        assert(len(actual) > 1)
        self.assert_list_prefix(expected, actual)

        expected = range(1, 100)
        actual = actual_tokens(peer, snk2)
        assert(len(actual) > 1)
        self.assert_list_prefix(expected, actual)

    def testSinkWithMultipleRemoteSources(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk = utils.new_actor_wargs(rt, "io.StandardOut", "snk", store_tokens=1)
        src_1 = utils.new_actor_wargs(peer, 'std.CountTimer', 'src_1')
        src_2 = utils.new_actor_wargs(peer, 'std.CountTimer', 'src_2')
        self.actors.update({snk:rt, src_1:peer, src_2:peer})

        utils.connect(rt, snk, 'token', peer.id, src_1, 'integer')
        time.sleep(.2)
        utils.connect(rt, snk, 'token', peer.id, src_2, 'integer')
        time.sleep(.2)

        expected_1 = expected_tokens(peer, src_1, "std.CountTimer")
        expected_2 = expected_tokens(peer, src_2, "std.CountTimer")
        expected = sorted(expected_1 + expected_2)

        actual = sorted(actual_tokens(rt, snk))
        assert len(actual) > 0
        self.assert_list_prefix(expected, actual)


@pytest.mark.essential
@pytest.mark.slow
class TestActorMigration(CalvinTestBase):

    def _start_sum_app(self, snk_rt, sum_rt, src_rt):
        snk = utils.new_actor_wargs(snk_rt, 'io.StandardOut', 'snk', store_tokens=1)
        sum_ = utils.new_actor(sum_rt, 'std.Sum', 'sum')
        src = utils.new_actor(src_rt, 'std.CountTimer', 'src')
        self.actors.update({snk:snk_rt, sum_:sum_rt, src:src_rt})

        utils.connect(snk_rt, snk, 'token', sum_rt.id, sum_, 'integer')
        utils.connect(sum_rt, sum_, 'integer', src_rt.id, src, 'integer')
        time.sleep(0.27)

        return (snk, sum_, src)

    def testOutPortRemoteToLocalMigration(self):
        """Testing outport remote to local migration"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        (snk, sum_, src) = self._start_sum_app(rt, peer, rt)

        actual_1 = actual_tokens(rt, snk)
        utils.migrate(rt, src, peer_id)
        time.sleep(0.2)

        expected = expected_tokens(peer, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testOutPortLocalToRemoteMigration(self):
        """Testing outport local to remote migration"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        (snk, sum_, src) = self._start_sum_app(rt, peer, peer)

        actual_1 = actual_tokens(rt, snk)
        utils.migrate(peer, src, id_)
        time.sleep(0.2)

        expected = expected_tokens(rt, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testOutPortLocalRemoteRepeatedMigration(self):
        """Testing outport local to remote migration and revers repeatedly"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        (snk, sum_, src) = self._start_sum_app(rt, peer, peer)

        actual_x = []
        actual_1 = actual_tokens(rt, snk)
        for i in range(5):
            if i % 2 == 0:
                utils.migrate(peer, src, id_)
            else:
                utils.migrate(rt, src, peer_id)
            time.sleep(0.2)
            actual_x_ = actual_tokens(rt, snk)
            assert(len(actual_x_) > len(actual_x))
            actual_x = actual_x_

        expected = expected_tokens(rt, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testInOutPortRemoteToLocalMigration(self):
        """Testing out- and inport remote to local migration"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        (snk, sum_, src) = self._start_sum_app(rt, peer, rt)

        actual_1 = actual_tokens(rt, snk)
        utils.migrate(peer, sum_, id_)
        time.sleep(0.4)

        expected = expected_tokens(rt, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testInOutPortLocalRemoteRepeatedMigration(self):
        """Testing outport local to remote migration and revers repeatedly"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        (snk, sum_, src) = self._start_sum_app(rt, rt, rt)

        actual_x = []
        actual_1 = actual_tokens(rt, snk)
        for i in range(5):
            if i % 2 == 0:
                utils.migrate(rt, sum_, peer_id)
            else:
                utils.migrate(peer, sum_, id_)
            time.sleep(0.2)
            actual_x_ = actual_tokens(rt, snk)
            assert(len(actual_x_) > len(actual_x))
            actual_x = actual_x_

        expected = expected_tokens(rt, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testInOutPortLocalToRemoteMigration(self):
        """Testing out- and inport local to remote migration"""

        rt = self.runtime
        id_ = rt.id
        peer = self.runtimes[0]
        peer_id = peer.id

        (snk, sum_, src) = self._start_sum_app(rt, rt, rt)

        actual_1 = actual_tokens(rt, snk)
        utils.migrate(rt, sum_, peer_id)
        time.sleep(0.2)

        expected = expected_tokens(rt, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testInOutPortRemoteToRemoteMigration(self):
        """Testing out- and inport remote to remote migration"""

        rt = self.runtime
        id_ = rt.id
        peer0 = self.runtimes[0]
        peer0_id = peer0.id
        peer1 = self.runtimes[1]
        peer1_id = peer1.id

        time.sleep(0.5)

        (snk, sum_, src) = self._start_sum_app(rt, peer0, rt)

        actual_1 = actual_tokens(rt, snk)
        utils.migrate(peer0, sum_, peer1_id)
        time.sleep(0.5)

        expected = expected_tokens(rt, src, 'std.Sum')
        actual = actual_tokens(rt, snk)
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testExplicitStateMigration(self):
        """Testing migration of explicit state handling"""

        rt = self.runtime
        id_ = rt.id
        peer0 = self.runtimes[0]
        peer0_id = peer0.id
        peer1 = self.runtimes[1]
        peer1_id = peer1.id

        snk = utils.new_actor_wargs(peer0, 'io.StandardOut', 'snk', store_tokens=1)
        wrapper = utils.new_actor(rt, 'misc.ExplicitStateExample', 'wrapper')
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk:peer0, wrapper:rt, src:rt})

        utils.connect(peer0, snk, 'token', id_, wrapper, 'token')
        utils.connect(rt, wrapper, 'token', id_, src, 'integer')
        time.sleep(0.3)

        actual_1 = actual_tokens(peer0, snk)
        utils.migrate(rt, wrapper, peer0_id)
        time.sleep(0.3)

        actual = actual_tokens(peer0, snk)
        expected = [u'((( 1 )))', u'((( 2 )))', u'((( 3 )))', u'((( 4 )))', u'((( 5 )))', u'((( 6 )))', u'((( 7 )))', u'((( 8 )))']
        assert(len(actual) > 1)
        assert(len(actual) > len(actual_1))
        self.assert_list_prefix(expected, actual)

    def testMigrateSourceWithMultipleLocalSinks(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk_1 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        snk_2 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk_1:rt, snk_2:rt, src:rt})

        utils.connect(rt, snk_1, 'token', rt.id, src, 'integer')
        utils.connect(rt, snk_2, 'token', rt.id, src, 'integer')
        time.sleep(0.27)

        before_migration_1 = actual_tokens(rt, snk_1)
        before_migration_2 = actual_tokens(rt, snk_2)

        utils.migrate(rt, src, peer.id)
        time.sleep(0.3)

        expected = expected_tokens(peer, src, 'std.CountTimer')
        actual_1 = actual_tokens(rt, snk_1)
        actual_2 = actual_tokens(rt, snk_2)

        assert(len(actual_1) > 1)
        assert(len(actual_2) > 1)
        assert(len(actual_1) > len(before_migration_1))
        assert(len(actual_2) > len(before_migration_2))
        self.assert_list_prefix(expected, actual_1)
        self.assert_list_prefix(expected, actual_2)
        self.assertListEqual(actual_1, actual_2)

    def testMigrateSourceWithMultipleRemoteAndLocalSinks(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk_1 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        snk_2 = utils.new_actor_wargs(peer, 'io.StandardOut', 'snk', store_tokens=1)
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk_1:rt, snk_2:peer, src:rt})

        utils.connect(rt, snk_1, 'token', rt.id, src, 'integer')
        utils.connect(peer, snk_2, 'token', rt.id, src, 'integer')
        time.sleep(0.27)

        before_migration_1 = actual_tokens(rt, snk_1)
        before_migration_2 = actual_tokens(peer, snk_2)

        utils.migrate(rt, src, peer.id)
        time.sleep(0.3)

        expected = expected_tokens(peer, src, 'std.CountTimer')
        actual_1 = actual_tokens(rt, snk_1)
        actual_2 = actual_tokens(peer, snk_2)

        assert(len(actual_1) > 1)
        assert(len(actual_2) > 1)
        assert(len(actual_1) > len(before_migration_1))
        assert(len(actual_2) > len(before_migration_2))
        self.assert_list_prefix(expected, actual_1)
        self.assert_list_prefix(expected, actual_2)
        self.assertListEqual(actual_1, actual_2)

    def testMigrateSourceWithMultipleRemoteSinks(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk_1 = utils.new_actor_wargs(peer, 'io.StandardOut', 'snk', store_tokens=1)
        snk_2 = utils.new_actor_wargs(peer, 'io.StandardOut', 'snk', store_tokens=1)
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk_1:peer, snk_2:peer, src:rt})

        utils.connect(peer, snk_1, 'token', rt.id, src, 'integer')
        utils.connect(peer, snk_2, 'token', rt.id, src, 'integer')
        time.sleep(0.27)

        before_migration_1 = actual_tokens(peer, snk_1)
        before_migration_2 = actual_tokens(peer, snk_2)

        utils.migrate(rt, src, peer.id)
        time.sleep(0.3)

        expected = expected_tokens(peer, src, 'std.CountTimer')
        actual_1 = actual_tokens(peer, snk_1)
        actual_2 = actual_tokens(peer, snk_2)

        assert(len(actual_1) > 1)
        assert(len(actual_2) > 1)
        assert(len(actual_1) > len(before_migration_1))
        assert(len(actual_2) > len(before_migration_2))
        self.assert_list_prefix(expected, actual_1)
        self.assert_list_prefix(expected, actual_2)
        self.assertListEqual(actual_1, actual_2)

    def testMigrateSinkWithMultipleLocalSources(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src_1 = utils.new_actor(rt, 'std.CountTimer', 'src')
        src_2 = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, src_1:rt, src_2:rt})

        utils.connect(rt, snk, 'token', rt.id, src_1, 'integer')
        utils.connect(rt, snk, 'token', rt.id, src_2, 'integer')
        time.sleep(0.2)

        utils.migrate(rt, snk, peer.id)
        time.sleep(0.3)

        expected_1 = expected_tokens(rt, src_1, 'std.CountTimer')
        expected_2 = expected_tokens(rt, src_2, 'std.CountTimer')
        expected = sorted(expected_1 + expected_2)
        actual = actual_tokens(peer, snk)

        assert(len(actual) > 1)
        self.assert_list_prefix(expected, actual)

    def testMigrateSinkWithMultipleRemoteAndLocalSources(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src_1 = utils.new_actor(rt, 'std.CountTimer', 'src')
        src_2 = utils.new_actor(peer, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, src_1:rt, src_2:peer})

        utils.connect(rt, snk, 'token', rt.id, src_1, 'integer')
        utils.connect(rt, snk, 'token', peer.id, src_2, 'integer')
        time.sleep(0.2)

        utils.migrate(rt, snk, peer.id)
        time.sleep(0.3)

        expected_1 = expected_tokens(rt, src_1, 'std.CountTimer')
        expected_2 = expected_tokens(peer, src_2, 'std.CountTimer')
        expected = sorted(expected_1 + expected_2)
        actual = actual_tokens(peer, snk)

        assert(len(actual) > 1)
        self.assert_list_prefix(expected, actual)

    def testMigrateSinkWithMultipleRemoteSources(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src_1 = utils.new_actor(peer, 'std.CountTimer', 'src')
        src_2 = utils.new_actor(peer, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, src_1:peer, src_2:peer})

        utils.connect(rt, snk, 'token', peer.id, src_1, 'integer')
        utils.connect(rt, snk, 'token', peer.id, src_2, 'integer')
        time.sleep(0.2)

        utils.migrate(rt, snk, peer.id)
        time.sleep(0.3)

        expected_1 = expected_tokens(peer, src_1, 'std.CountTimer')
        expected_2 = expected_tokens(peer, src_2, 'std.CountTimer')
        expected = sorted(expected_1 + expected_2)
        actual = actual_tokens(peer, snk)

        assert(len(actual) > 1)
        self.assert_list_prefix(expected, actual)

    def testMigrateSinkWithMultipleSinkAndSources(self):
        rt = self.runtime
        peer = self.runtimes[0]

        snk_1 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        snk_2 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src_1 = utils.new_actor(rt, 'std.CountTimer', 'src')
        src_2 = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk_1:rt, snk_2:rt, src_1:rt, src_2:rt})

        utils.connect(rt, snk_1, 'token', rt.id, src_1, 'integer')
        utils.connect(rt, snk_1, 'token', rt.id, src_2, 'integer')
        utils.connect(rt, snk_2, 'token', rt.id, src_1, 'integer')
        utils.connect(rt, snk_2, 'token', rt.id, src_2, 'integer')
        time.sleep(0.2)

        before_migration_1 = actual_tokens(rt, snk_1)
        before_migration_2 = actual_tokens(rt, snk_2)

        utils.migrate(rt, snk_1, peer.id)
        time.sleep(0.5)

        expected_1 = expected_tokens(rt, src_1, 'std.CountTimer')
        expected_2 = expected_tokens(rt, src_2, 'std.CountTimer')
        expected = sorted(expected_1 + expected_2)
        actual_snk_1 = actual_tokens(peer, snk_1)
        actual_snk_2 = actual_tokens(rt, snk_2)

        assert(len(actual_snk_1) > 1)
        assert(len(actual_snk_2) > 1)
        assert(len(actual_snk_1) > len(before_migration_1))
        assert(len(actual_snk_2) > len(before_migration_2))
        self.assert_list_prefix(expected, actual_snk_1)
        self.assert_list_prefix(expected, actual_snk_2)
        self.assert_list_prefix(actual_snk_1, actual_snk_2)


@pytest.mark.essential
@pytest.mark.slow
class TestActorReplication(CalvinTestBase):

    def _start_app(self):
        script = """
        src : std.CountTimer()
        snk : io.StandardOut(store_tokens=1)
        src.integer > snk.token
        """

        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(self.runtime, app_info, deploy_info={"required_reliability": 0.95})
        app_id = d.deploy()
        self.deployer = d
        time.sleep(0.2)

        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']
        return (d, app_id, src, snk)


    def testReplicationGetsNewName(self):
        """Testing outport remote to local migration"""
        rt = self.runtime
        peer = self.runtimes[0]

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=123, quiet=True)
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, src:rt})

        utils.connect(rt, snk, 'token', rt.id, src, 'integer')
        time.sleep(0.27)

        snk_replica = utils.replicate(rt, snk, peer.id)
        self.actors[snk_replica] = peer
        time.sleep(0.27)

        original = utils.get_actor(rt, snk)
        replica = utils.get_actor(peer, snk_replica)

        assert original['name'] != replica['name']

    def testLocalToRemoteReplication(self):
        rt = self.runtime
        peer = self.runtimes[0]

        (d, app_id, src, snk) = self._start_app()

        replica = utils.replicate(rt, snk, peer.id)
        time.sleep(0.2)

        actors = utils.get_application_actors(rt, app_id)
        assert replica in actors

        actors = utils.get_application_actors(peer, app_id)
        assert replica in actors

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual_orig = actual_tokens(rt, snk)
        actual_replica = actual_tokens(peer, replica)

        assert(len(actual_orig) > 1)
        self.assert_list_prefix(expected, actual_orig)
        self.assert_list_prefix(expected, actual_replica)

    @pytest.mark.xfail
    def testLocalToLocalReplication(self):
        rt = self.runtime

        (d, app_id, src, snk) = self._start_app()

        replica = utils.replicate(rt, snk, rt.id)
        time.sleep(0.2)

        actors = utils.get_application_actors(rt, app_id)
        assert replica in actors

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual_orig = actual_tokens(rt, snk)
        actual_replica = actual_tokens(rt, replica)

        assert(len(actual_orig) > 1)
        self.assert_list_prefix(expected, actual_orig)
        self.assert_list_prefix(expected, actual_replica)

    @pytest.mark.xfail
    def testRemoteToRemoteReplication(self):
        rt = self.runtime
        peer = self.runtimes[0]

        (d, app_id, src, snk) = self._start_app()

        utils.migrate(rt, snk, peer.id)
        time.sleep(0.2)
        replica = utils.replicate(peer, snk, peer.id)
        time.sleep(0.2)

        actors = utils.get_application_actors(rt, app_id)
        assert replica in actors

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual_orig = actual_tokens(peer, snk)
        actual_replica = actual_tokens(peer, replica)

        assert(len(actual_orig) > 1)
        self.assert_list_prefix(expected, actual_orig)
        self.assert_list_prefix(expected, actual_replica)

    def testDoubleReplication(self):
        rt = self.runtime
        peer = self.runtimes[0]
        peer2 = self.runtimes[1]

        script = """
            src : std.CountTimer()
            snk1 : io.StandardOut(store_tokens=1)
            snk2 : io.StandardOut(store_tokens=1)
            src.integer > snk1.token
            src.integer > snk2.token
        """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        app_id = d.deploy()
        self.deployer = d
        time.sleep(0.2)

        snk1 = d.actor_map['simple:snk1']
        snk2 = d.actor_map['simple:snk2']
        src = d.actor_map['simple:src']

        replica_1 = utils.replicate(rt, snk1, peer.id)
        time.sleep(0.2)
        replica_2 = utils.replicate(peer, replica_1, peer2.id)
        time.sleep(0.2)

        actors = utils.get_application_actors(rt, app_id)
        assert replica_1 in actors
        assert replica_2 in actors

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual_orig_1 = actual_tokens(rt, snk1)
        actual_orig_2 = actual_tokens(rt, snk2)
        actual_replica_1 = actual_tokens(peer, replica_1)
        actual_replica_2 = actual_tokens(peer2, replica_2)

        assert(len(actual_orig_1) > 1)
        assert(len(actual_orig_2) > 1)
        self.assert_list_prefix(expected, actual_orig_1)
        self.assert_list_prefix(expected, actual_replica_1)
        self.assert_list_prefix(expected, actual_orig_2)
        self.assert_list_prefix(expected, actual_replica_2)

    def testRemoteToLocalReplication(self):
        rt = self.runtime
        peer = self.runtimes[0]

        (d, app_id, src, snk) = self._start_app()

        utils.migrate(rt, snk, peer.id)
        time.sleep(0.2)
        replica = utils.replicate(peer, snk, rt.id)
        time.sleep(0.2)

        actors = utils.get_application_actors(rt, app_id)
        assert replica in actors
        actors = utils.get_application_actors(peer, app_id)
        assert replica in actors

        expected = expected_tokens(rt, src, 'std.CountTimer')
        actual_orig = actual_tokens(peer, snk)
        actual_replica = actual_tokens(rt, replica)

        assert(len(actual_orig) > 1)
        self.assert_list_prefix(expected, actual_orig)
        self.assert_list_prefix(expected, actual_replica)

    def testReplicateSourceWithMultipleSinksToRemote(self):
        """Testing outport remote to local migration"""
        rt = self.runtime
        peer = self.runtimes[0]

        snk_1 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        snk_2 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk_1:rt, snk_2:rt, src:rt})

        utils.connect(rt, snk_1, 'token', rt.id, src, 'integer')
        utils.connect(rt, snk_2, 'token', rt.id, src, 'integer')
        time.sleep(0.3)

        src_replica = utils.replicate(rt, src, peer.id)
        self.actors[src_replica] = rt
        time.sleep(0.2)

        expected_src = expected_tokens(rt, src, 'std.CountTimer')
        expected_replica = expected_tokens(peer, src_replica, 'std.CountTimer')
        expected = sorted(expected_src + expected_replica)
        actual_snk_1 = actual_tokens(rt, snk_1)
        actual_snk_2 = actual_tokens(rt, snk_2)

        counts = Counter(actual_snk_1)
        unique_elements = [val for val, cnt in counts.iteritems() if cnt == 1]
        assert len(unique_elements) > 0

        expected = filter(lambda x: x not in unique_elements, expected)
        actual_snk_1 = filter(lambda x: x not in unique_elements, actual_snk_1)
        actual_snk_2 = filter(lambda x: x not in unique_elements, actual_snk_2)

        assert(len(actual_snk_1) > 1)
        assert(len(actual_snk_2) > 1)
        self.assert_list_prefix(expected, sorted(actual_snk_1))
        self.assert_list_prefix(expected, sorted(actual_snk_2))

    @pytest.mark.xfail
    def testReplicateSourceWithMultipleSinksToLocal(self):
        """Testing outport remote to local migration"""
        rt = self.runtime

        snk_1 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        snk_2 = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk_1:rt, snk_2:rt, src:rt})

        utils.connect(rt, snk_1, 'token', rt.id, src, 'integer')
        utils.connect(rt, snk_2, 'token', rt.id, src, 'integer')
        time.sleep(0.2)

        src_replica = utils.replicate(rt, src, rt.id)
        self.actors[src_replica] = rt
        time.sleep(0.4)

        expected_src = expected_tokens(rt, src, 'std.CountTimer')
        expected_replica = expected_tokens(rt, src_replica, 'std.CountTimer')
        expected = sorted(expected_src + expected_replica)
        actual_snk_1 = actual_tokens(rt, snk_1)
        actual_snk_2 = actual_tokens(rt, snk_2)

        counts = Counter(actual_snk_1)
        unique_elements = [val for val, cnt in counts.iteritems() if cnt == 1]
        assert len(unique_elements) > 0

        expected = filter(lambda x: x not in unique_elements, expected)
        actual_snk_1 = filter(lambda x: x not in unique_elements, actual_snk_1)
        actual_snk_2 = filter(lambda x: x not in unique_elements, actual_snk_2)

        assert(len(actual_snk_1) > 1)
        assert(len(actual_snk_2) > 1)
        self.assert_list_prefix(expected, sorted(actual_snk_1))
        self.assert_list_prefix(expected, sorted(actual_snk_2))

    def testReplicateSinkWithMultipleSourcesToRemote(self):
        """Testing outport remote to local migration"""
        rt = self.runtime
        peer = self.runtimes[0]

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src_1 = utils.new_actor(rt, 'std.CountTimer', 'src')
        src_2 = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, src_1:rt, src_2:rt})

        utils.connect(rt, snk, 'token', rt.id, src_1, 'integer')
        utils.connect(rt, snk, 'token', rt.id, src_2, 'integer')
        time.sleep(0.2)

        snk_replica = utils.replicate(rt, snk, peer.id)
        self.actors[snk_replica] = peer
        time.sleep(0.2)

        expected_src_1 = expected_tokens(rt, src_1, 'std.CountTimer')
        expected_src_2 = expected_tokens(rt, src_2, 'std.CountTimer')
        expected = sorted(expected_src_1 + expected_src_2)
        actual_snk = actual_tokens(rt, snk)
        actual_replica = actual_tokens(peer, snk_replica)

        assert(len(actual_snk) > 1)
        assert(len(actual_replica) > 1)
        self.assert_list_prefix(expected, sorted(actual_snk))
        self.assert_list_prefix(expected, sorted(actual_replica))

    @pytest.mark.xfail
    def testReplicateSinkWithMultipleSourcesToLocal(self):
        """Testing outport remote to local migration"""
        rt = self.runtime

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src_1 = utils.new_actor(rt, 'std.CountTimer', 'src')
        src_2 = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, src_1:rt, src_2:rt})

        utils.connect(rt, snk, 'token', rt.id, src_1, 'integer')
        utils.connect(rt, snk, 'token', rt.id, src_2, 'integer')
        time.sleep(0.2)

        snk_replica = utils.replicate(rt, snk, rt.id)
        self.actors[snk_replica] = rt
        time.sleep(0.3)

        expected_src_1 = expected_tokens(rt, src_1, 'std.CountTimer')
        expected_src_2 = expected_tokens(rt, src_2, 'std.CountTimer')
        expected = sorted(expected_src_1 + expected_src_2)
        actual_snk = actual_tokens(rt, snk)
        actual_replica = actual_tokens(rt, snk_replica)

        assert(len(actual_snk) > 1)
        assert(len(actual_replica) > 1)
        self.assert_list_prefix(expected, sorted(actual_snk))
        self.assert_list_prefix(expected, sorted(actual_replica))

    def testReplicateActorWithBothInportAndOutports(self):
        """Testing outport remote to local migration"""
        rt = self.runtime
        peer = self.runtimes[0]

        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        ity = utils.new_actor_wargs(rt, 'std.Identity', 'ity', dump=True)
        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        self.actors.update({snk:rt, src:rt, ity:rt})

        utils.connect(rt, snk, 'token', rt.id, ity, 'token')
        utils.connect(rt, ity, 'token', rt.id, src, 'integer')

        time.sleep(0.2)
        actual = utils.report(rt, snk)

        replica = utils.replicate(rt, ity, peer.id)
        self.actors[replica] = peer
        time.sleep(0.3)

        expected_1 = expected_tokens(rt, src, 'std.CountTimer')
        expected_2 = expected_tokens(rt, src, 'std.CountTimer')
        expected = sorted(expected_1 + expected_2)
        actual = actual_tokens(rt, snk)

        assert(len(actual) > 1)
        assert(len(expected_2) > 1)
        assert actual.count(1) == 1
        assert actual.count(4) == 2

        counts = Counter(actual)
        unique_elements = [val for val, cnt in counts.iteritems() if cnt == 1]
        assert len(unique_elements) > 0

        expected = filter(lambda x: x not in unique_elements, expected)
        actual = filter(lambda x: x not in unique_elements, actual)
        self.assert_list_prefix(expected, sorted(actual))

    def testReplicateSinkAndSource(self):
        """Testing outport remote to local migration"""
        rt = self.runtime
        peer = self.runtimes[0]

        src = utils.new_actor(rt, 'std.CountTimer', 'src')
        ity = utils.new_actor_wargs(rt, 'std.Identity', 'ity', dump=True)
        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        self.actors.update({snk:rt, src:rt, ity:rt})

        utils.connect(rt, snk, 'token', rt.id, ity, 'token')
        utils.connect(rt, ity, 'token', rt.id, src, 'integer')
        time.sleep(0.2)

        snk_replica = utils.replicate(rt, snk, peer.id)
        src_replica = utils.replicate(rt, src, peer.id)
        self.actors.update({snk_replica:rt, src_replica:rt})
        time.sleep(0.2)

        expected_1 = expected_tokens(rt, src, 'std.CountTimer')
        expected_2 = expected_tokens(peer, src_replica, 'std.CountTimer')
        expected = sorted(expected_1 + expected_2)
        actual = actual_tokens(rt, snk)
        actual_replica = actual_tokens(peer, snk_replica)

        assert(len(actual) > 1)
        assert(len(expected_2) > 1)
        assert actual.count(1) == 1
        assert actual.count(4) == 2

        counts = Counter(actual)
        unique_elements = [val for val, cnt in counts.iteritems() if cnt == 1]
        assert len(unique_elements) > 0

        expected = filter(lambda x: x not in unique_elements, expected)
        actual = filter(lambda x: x not in unique_elements, actual)
        actual_replica = filter(lambda x: x not in unique_elements, actual_replica)

        self.assert_list_prefix(expected, sorted(actual))
        self.assert_list_prefix(expected, sorted(actual_replica))


@pytest.mark.essential
@pytest.mark.slow
class TestLosingActors(CalvinTestBase):

    def setUp(self):
        super(TestLosingActors, self).setUp()
        self.rt1 = runtime
        self.rt2 = runtimes[0]
        self.rt3 = runtimes[1]
        self.runtimes = [self.rt1]
        self.runtimes.extend(runtimes)

    def _start_app(self, replicate_src=0, replicate_snk=0):
        script = """
        src : std.ReplicatedCountTimer(replicate=""" + str(replicate_src) + """)
        snk : io.StandardOut(store_tokens=1, replicate=""" + str(replicate_snk) + """)
        src.integer > snk.token
        """
        self.src_type = "std.ReplicatedCountTimer"
        self.snk_type = "io.StandardOut"

        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(self.rt1, app_info, deploy_info={"required_reliability": 0.98})
        app_id = d.deploy()
        self.deployer = d
        time.sleep(0.2)

        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']
        return (d, app_id, src, snk)

    def _get_reliability(self, app_id, actor_name, actor_type):
        reliabilities = []
        for node_id in utils.get_replica_nodes(self.rt1, app_id, actor_name):
            reliabilities.append(1-utils.get_reliability(self.rt1, node_id, actor_type))
        return 1 - reduce(operator.mul, reliabilities, 1)

    def _check_reliability(self, app_id, name, actor_type):
        reliability = self._get_reliability(app_id, name, actor_type)
        app = utils.get_application(self.rt1, app_id)
        assert(reliability > app['required_reliability'])

    def _check_snk_replicas(self, app_id, expected):
        replicas = self._get_replicas(app_id, 'simple:snk')
        for a_id, a_rt in replicas.iteritems():
            actual = actual_tokens(a_rt, a_id)
            self.assert_list_prefix(expected, actual)

    def _check_src_replicas(self, app_id, snk, snk_rt, src, src_rt, actor_type, expected_before, snk_before, replica_before):
        print 'expected_before', expected_before
        print 'snk_before', snk_before
        print 'replica_before', replica_before
        actual_snk = actual_tokens(snk_rt, snk)
        print '\nactual_snk', actual_snk

        actual_replicas = []
        replicas = self._get_replicas(app_id, 'simple:src')
        for actor_id, rt in replicas.iteritems():
            actual_replicas.append(expected_tokens(rt, actor_id, actor_type))
        print 'actual_replicas', actual_replicas

        expected = expected_tokens(src_rt, src, actor_type)
        print '\nexpected', expected

        assert len(expected) > len(expected_before)
        assert len(actual_snk) > len(snk_before + replica_before)
        for actuals in actual_replicas:
            assert len(actuals) > 0
            assert len(actuals) > len(replica_before)

        counts = Counter(actual_snk)
        unique_elements = [val for val, cnt in counts.iteritems() if cnt < len(actual_replicas)]
        doubles = [val for val, cnt in counts.iteritems() if cnt > len(actual_replicas)]
        print "replicas: ", len(actual_replicas)
        print "unique: ", unique_elements
        print "doubles: ", doubles
        print "actual snk: ", actual_snk
        print "expected: ", expected

        assert len(unique_elements) > 0
        assert len(doubles) < 5

        for d in doubles:
            actual_snk.remove(d)

        filtered_expected = filter(lambda x: x not in unique_elements, expected)
        filtered_actual_snk = filter(lambda x: x not in unique_elements, actual_snk)
        filtered_expected_replicas = []
        for expected_replica in actual_replicas:
            filtered_expected_replicas.append(filter(lambda x: x not in unique_elements, expected_replica))

        filtered_expected_total = [x for x in filtered_expected]
        for i in range(len(actual_replicas) - 1):
            filtered_expected_total += filtered_expected
        self.assert_list_prefix(sorted(filtered_expected_total), sorted(filtered_actual_snk))
        for filtered_expected_replica in filtered_expected_replicas:
            self.assert_list_prefix(sorted(filtered_expected), sorted(filtered_expected_replica))

    def _get_replicas(self, app_id, actor_name):
        replicas = {}
        for rt in self.runtimes:
            actors = utils.get_actors(rt)
            for actor in actors:
                for i in range(5):
                    try:
                        a = utils.get_actor(self.rt1, actor)
                        a_name = calvinuuid.remove_uuid(a['name'])
                        if a_name == actor_name and a['node_id'] == rt.id:
                            replicas[actor] = rt
                        break
                    except:
                        time.sleep(0.2)
        return replicas

    ## Tests ##

    def testLoseLastActor(self):
        (d, app_id, src, snk) = self._start_app()

        actors_before = utils.get_application_actors(self.rt1, app_id)
        for a_id in actors_before:
            self.actors.update({a_id: self.rt1})

        utils.lost_actor(self.rt1, snk)
        time.sleep(0.6)

        actors_after = utils.get_application_actors(self.rt1, app_id)

        for a_id in actors_after:
            self.actors.update({a_id: self.rt1})

        new_actors = [actor for actor in actors_after if actor not in actors_before]
        assert(len(new_actors) == 0)

    def testReplicaNodeList(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)

        snk_replica = utils.replicate(self.rt1, snk, self.rt2.id)
        self.actors.update({snk_replica: self.rt2})
        nodes = utils.get_replica_nodes(self.rt1, app_id, 'simple:snk')
        assert(len(nodes) == 2)

        snk_replica = utils.replicate(self.rt1, snk, self.rt3.id)
        self.actors.update({snk_replica: self.rt3})
        nodes = utils.get_replica_nodes(self.rt1, app_id, 'simple:snk')
        assert(len(nodes) == 3)

        snk_replica = utils.delete_actor(self.rt1, snk)
        nodes = utils.get_replica_nodes(self.rt1, app_id, 'simple:snk')
        assert(len(nodes) == 2)

    def testLoseLocalSnkOneReplica(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)

        replica = utils.replicate(self.rt1, snk, self.rt2.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')

        utils.disable(self.rt2, replica)
        time.sleep(0.2)

        utils.lost_actor(self.rt2, replica)
        time.sleep(0.6)
        self.assertIsNone(utils.get_actor(self.rt2, replica))

        self._check_reliability(app_id, 'simple:snk', self.snk_type)

        expected = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')
        assert len(expected) > len(expected_before)

        self._check_snk_replicas(app_id, expected)

    def testLoseLocalSrcOneReplica(self):
        (d, app_id, src, snk) = self._start_app(replicate_src=1)

        replica = utils.replicate(self.rt1, src, self.rt2.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')
        actual_snk_before = actual_tokens(self.rt1, snk)
        expected_replica_before = expected_tokens(self.rt2, replica, 'std.ReplicatedCountTimer')

        utils.disable(self.rt2, replica)
        time.sleep(0.2)

        utils.lost_actor(self.rt2, replica)
        time.sleep(0.6)
        self.assertIsNone(utils.get_actor(self.rt2, replica))

        self._check_reliability(app_id, 'simple:src', self.src_type)

        self._check_src_replicas(app_id, snk, self.rt1, src, self.rt1, 'std.ReplicatedCountTimer', expected_before, actual_snk_before, expected_replica_before)

    def testLoseRemoteSrcOneReplica(self):
        (d, app_id, src, snk) = self._start_app(replicate_src=1)

        replica = utils.replicate(self.rt1, src, self.rt2.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')
        actual_snk_before = actual_tokens(self.rt1, snk)
        expected_replica_before = expected_tokens(self.rt2, replica, 'std.ReplicatedCountTimer')

        utils.disable(self.rt2, replica)
        time.sleep(0.2)

        utils.lost_actor(self.rt2, replica)
        time.sleep(0.6)
        self.assertIsNone(utils.get_actor(self.rt2, replica))

        self._check_reliability(app_id, 'simple:src', self.src_type)

        self._check_src_replicas(app_id, snk, self.rt1, src, self.rt1, 'std.ReplicatedCountTimer', expected_before, actual_snk_before, expected_replica_before)

    def testLoseRemoteSnkOneReplica(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)

        snk2 = utils.replicate(self.rt1, snk, self.rt2.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')

        utils.disable(self.rt2, snk2)
        time.sleep(0.2)

        utils.lost_actor(self.rt2, snk2)
        time.sleep(0.6)
        self.assertIsNone(utils.get_actor(self.rt1, snk2))

        self._check_reliability(app_id, 'simple:snk', self.snk_type)

        expected = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')
        assert len(expected) > len(expected_before)

        self._check_snk_replicas(app_id, expected)

    def testLoseLocalSnkTwoReplicas(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)

        snk2 = utils.replicate(self.rt1, snk, self.rt2.id)
        time.sleep(0.2)
        time.sleep(0.2)

        expected_before = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')

        utils.disable(self.rt2, snk2)
        time.sleep(0.2)

        utils.lost_actor(self.rt2, snk2)
        time.sleep(0.6)
        self.assertIsNone(utils.get_actor(self.rt2, snk2))

        self._check_reliability(app_id, 'simple:snk', self.snk_type)

        expected = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')
        print "EX HERE"
        print expected
        print expected_before
        assert len(expected) > len(expected_before)

        self._check_snk_replicas(app_id, expected)

    def testLoseRemoteSnkTwoReplicas(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)

        snk2 = utils.replicate(self.rt1, snk, self.rt2.id)
        time.sleep(0.2)
        snk3 = utils.replicate(self.rt2, snk2, self.rt3.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')

        utils.disable(self.rt2, snk2)
        time.sleep(0.2)

        utils.lost_actor(self.rt2, snk2)
        time.sleep(0.6)
        self.assertIsNone(utils.get_actor(self.rt1, snk2))

        self._check_reliability(app_id, 'simple:snk', self.snk_type)

        expected = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')
        assert len(expected) > len(expected_before)

        self._check_snk_replicas(app_id, expected)

    def testLoseTwoActorsTwoReplicas(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)

        snk2 = utils.replicate(self.rt1, snk, self.rt2.id)
        time.sleep(0.2)
        snk3 = utils.replicate(self.rt1, snk, self.rt3.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')

        utils.disable(self.rt2, snk2)
        time.sleep(0.2)

        utils.lost_actor(self.rt2, snk2)

        utils.disable(self.rt3, snk3)
        time.sleep(0.2)

        utils.lost_actor(self.rt3, snk3)
        time.sleep(0.6)
        self.assertIsNone(utils.get_actor(self.rt2, snk2))
        self.assertIsNone(utils.get_actor(self.rt3, snk3))

        self._check_reliability(app_id, 'simple:snk', self.snk_type)

        expected = expected_tokens(self.rt1, src, 'std.ReplicatedCountTimer')
        assert len(expected) > len(expected_before)

        self._check_snk_replicas(app_id, expected)


@pytest.mark.essential
@pytest.mark.slow
class TestDynamicReliability(CalvinTestBase):

    def setUp(self):
        super(TestDynamicReliability, self).setUp()
        self.rt1 = runtime
        self.rt2 = runtimes[0]
        self.rt3 = runtimes[1]
        self.runtimes = [self.rt1]
        self.runtimes.extend(runtimes)

    def _start_app(self, replicate_src=0, replicate_snk=0):
        script = """
        src : std.ReplicatedCountTimer(replicate=""" + str(replicate_src) + """)
        snk : io.StandardOut(store_tokens=1, replicate=""" + str(replicate_snk) + """)
        src.integer > snk.token
        """
        self.snk_type = "io.StandardOut"
        self.src_type = "std.ReplicatedCountTimer"

        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(self.rt1, app_info, deploy_info={"required_reliability": 0.98})
        app_id = d.deploy()
        self.deployer = d
        time.sleep(0.2)

        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']
        self.actors[src] = self.rt1
        self.actors[snk] = self.rt1
        return (d, app_id, src, snk)

    def _get_reliability(self, app_id, actor_name, actor_type):
        reliabilities = []
        for node_id in utils.get_replica_nodes(self.rt1, app_id, actor_name):
            reliabilities.append(1 - utils.get_reliability(self.rt1, node_id, actor_type))
        return 1 - reduce(operator.mul, reliabilities, 1)

    def _check_reliability(self, app_id, name, actor_type):
        reliability = self._get_reliability(app_id, name, actor_type)
        app = utils.get_application(self.rt1, app_id)
        assert(reliability > app['required_reliability'])

    def testCheckReliabilityOneActor(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)
        time.sleep(5)

        self._check_reliability(app_id, 'simple:snk', self.snk_type)

    def testCheckReliabilityTwoActors(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1, replicate_src=1)
        time.sleep(5)

        self._check_reliability(app_id, 'simple:snk', self.snk_type)
        self._check_reliability(app_id, 'simple:src', self.src_type)

    def testDropInReliability(self):
        (d, app_id, src, snk) = self._start_app(replicate_snk=1)
        time.sleep(5)

        self._check_reliability(app_id, 'simple:snk', self.snk_type)

        time.sleep(5)
        utils.simulate_node_failure(self.rt1, self.rt1.id, 1)
        time.sleep(5)

        self._check_reliability(app_id, 'simple:snk', self.snk_type)


@pytest.mark.essential
@pytest.mark.slow
class TestDyingRuntimes(CalvinTestBase):

    def setUp(self):
        global ip_addr
        os.environ["CALVIN_TESTING"] = ""
        self.ip_addr = ip_addr
        self.runtime = runtime
        self.runtime2 = runtimes[0]
        self.runtime3 = runtimes[1]

        csruntime(ip_addr, port=5028, controlport=5029, attr={}, storage=True)
        time.sleep(0.2)
        self.storage_runtime = utils.RT("http://%s:5029" % self.ip_addr)

        conf = copy.deepcopy(_conf)
        conf.set('global', 'storage_proxy', "calvinip://%s:5028" % self.ip_addr)
        conf.set('global', 'storage_start', "1")
        conf.save("/tmp/calvin5030.conf")

        csruntime(ip_addr, port=5032, controlport=5033, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.runtime = utils.RT("http://%s:5033" % self.ip_addr)

        csruntime(ip_addr, port=5034, controlport=5035, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.runtime2 = utils.RT("http://%s:5035" % self.ip_addr)

        csruntime(ip_addr, port=5036, controlport=5037, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.runtime3 = utils.RT("http://%s:5037" % self.ip_addr)

        csruntime(ip_addr, port=5030, controlport=5031, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.dying_rt = utils.RT("http://%s:5031" % self.ip_addr)

        self.runtimes = [self.runtime, self.runtime2, self.runtime3, self.dying_rt]

        ids = {}
        for rt in self.runtimes:
            for i in range(5):
                try:
                    rt_id = utils.get_node_id(rt)
                    ids[rt] = rt_id
                    break
                except:
                    time.sleep(0.2)
        for key in ids:
            if not ids[key]:
                self._kill_all()
                assert False
            key.id = ids[key]

        utils.peer_setup(self.runtime, ["calvinip://%s:5030" % ip_addr])
        utils.peer_setup(self.runtime, ["calvinip://%s:5034" % ip_addr])
        utils.peer_setup(self.runtime, ["calvinip://%s:5036" % ip_addr])
        utils.peer_setup(self.runtime2, ["calvinip://%s:5030" % ip_addr])
        utils.peer_setup(self.runtime2, ["calvinip://%s:5032" % ip_addr])
        utils.peer_setup(self.runtime2, ["calvinip://%s:5036" % ip_addr])
        utils.peer_setup(self.runtime3, ["calvinip://%s:5030" % ip_addr])
        utils.peer_setup(self.runtime3, ["calvinip://%s:5032" % ip_addr])
        utils.peer_setup(self.runtime3, ["calvinip://%s:5034" % ip_addr])
        utils.peer_setup(self.dying_rt, ["calvinip://%s:5032" % ip_addr])
        utils.peer_setup(self.dying_rt, ["calvinip://%s:5034" % ip_addr])
        utils.peer_setup(self.dying_rt, ["calvinip://%s:5036" % ip_addr])

        time.sleep(0.2)

    def tearDown(self):
        self._kill_all()
        os.environ["CALVIN_TESTING"] = "1"

    def _kill_all(self):
        os.system("pkill -9 -f 'csruntime -n %s -p 5028'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5030'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5032'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5034'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5036'" % (self.ip_addr,))

    def _check_reliability(self, rt, app_id, actors_before, actor_name, actor_type=None):
        new_actors, new_replicas = self._verify_new_actors(rt, app_id, actors_before)

        actor_runtimes = self._check_app_reliability(rt, new_actors, app_id, actor_name)

        actual_replicas = []
        for actor_id, rt in actor_runtimes.iteritems():
            if actor_id in new_replicas:
                if "src" in actor_name:
                    actual_replicas.append(expected_tokens(rt, actor_id, actor_type))
                else:
                    actual_replicas.append(actual_tokens(rt, actor_id))

        return actual_replicas

    def _verify_new_actors(self, rt, app_id, actors_before):
        new_actors = utils.get_application_actors(rt, app_id)
        new_replicas = [actor_id for actor_id in new_actors if actor_id not in actors_before]
        n = 0
        while n < 5:
            new_actors = utils.get_application_actors(rt, app_id)
            new_replicas = [actor_id for actor_id in new_actors if actor_id not in actors_before]
            try:
                assert len(new_replicas) > 0
                break
            except AssertionError:
                time.sleep(0.5)
            n += 1
        assert len(new_replicas) > 0

        return new_actors, new_replicas

    def _check_app_reliability(self, rt, new_actors, app_id, actor_name):
        reliabilities = []
        runtimes = self.runtimes
        runtimes.append(self.runtime)
        actor_runtimes = {}
        for actor in new_actors:
            a = utils.get_actor(rt, actor)
            if a:
                name = calvinuuid.remove_uuid(a['name'])
                if name == actor_name:
                    reliabilities.append(1 - utils.get_reliability(rt, a['node_id'], a['type']))
                for rt in runtimes:
                    if a['node_id'] == rt.id:
                        actor_runtimes[actor] = rt

        app = utils.get_application(rt, app_id)
        reliability = (1 - reduce(operator.mul, reliabilities, 1))
        assert reliability > app['required_reliability']

        return actor_runtimes

    def _check_dead_node(self, rt, dead_rt, dead_replica):
        self.assertIsNone(utils.get_actor(rt, dead_replica))
        self.assertIsNone(utils.get_node(rt, dead_rt.id))
        assert dead_rt.id not in utils.get_nodes(rt)

    def _check_values(self, rt1, rt2, snk, src, src_type, expected_before, snk_before, replica_before, actual_replicas):
        actual_snk = actual_tokens(rt1, snk)
        expected = expected_tokens(rt2, src, src_type)
        assert len(expected) > len(expected_before)
        assert len(actual_snk) > len(snk_before + replica_before)
        for actuals in actual_replicas:
            assert len(actuals) > 0
            assert len(actuals) > len(replica_before)

        counts = Counter(actual_snk)
        unique_elements = [val for val, cnt in counts.iteritems() if cnt <= len(actual_replicas)]
        doubles = [val for val, cnt in counts.iteritems() if cnt > len(actual_replicas) + 1]
        print "replicas: ", len(actual_replicas)
        print "unique: ", unique_elements
        print "doubles: ", doubles
        print "actual snk: ", actual_snk
        print "expected: ", expected

        assert len(unique_elements) > 0
        assert len(doubles) < 5

        for d in doubles:
            actual_snk.remove(d)

        filtered_expected = filter(lambda x: x not in unique_elements, expected)
        filtered_actual_snk = filter(lambda x: x not in unique_elements, actual_snk)
        filtered_expected_replicas = []
        for expected_replica in actual_replicas:
            filtered_expected_replicas.append(filter(lambda x: x not in unique_elements, expected_replica))

        filtered_expected_total = [x for x in filtered_expected]
        for i in range(len(actual_replicas)):
            filtered_expected_total += filtered_expected
        self.assert_list_prefix(sorted(filtered_expected_total), sorted(filtered_actual_snk))
        for filtered_expected_replica in filtered_expected_replicas:
            self.assert_list_prefix(sorted(filtered_expected), sorted(filtered_expected_replica))

    def _check_actuals(self, expected, actual_replicas, actual_replica_before):
        for actual_replica in actual_replicas:
            assert len(actual_replica) > len(actual_replica_before)
            self.assert_list_prefix(expected, actual_replica)

    def _kill_dying(self):
        os.system("pkill -9 -f 'csruntime -n %s -p 5030'" % (self.ip_addr,))

    def _start_app(self, replicate_snk=0, replicate_src=0):
        script = """
        src : std.ReplicatedCountTimer(replicate=""" + str(replicate_src) + """)
        snk : io.StandardOut(store_tokens=1, replicate=""" + str(replicate_snk) + """)
        src.integer > snk.token
        """

        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(self.runtime, app_info, deploy_info={"required_reliability": 0.98})
        app_id = d.deploy()
        self.deployer = d
        time.sleep(0.2)

        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']
        return (d, app_id, src, snk)

    def testLoseSnkActorWithLocalSource(self):
        d, app_id, src, snk = self._start_app(replicate_snk=1)

        replica = utils.replicate(self.runtime, snk, self.dying_rt.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.runtime, src, 'std.ReplicatedCountTimer')
        actual_snk_before = actual_tokens(self.runtime, snk)
        actual_replica_before = actual_tokens(self.dying_rt, replica)

        actors_before = utils.get_application_actors(self.runtime, app_id)
        self._kill_dying()
        time.sleep(1.3)

        actual_replicas = self._check_reliability(self.runtime, app_id, actors_before, 'simple:snk')

        self._check_dead_node(self.runtime, self.dying_rt, replica)

        expected = expected_tokens(self.runtime, src, 'std.ReplicatedCountTimer')
        actual = actual_tokens(self.runtime, snk)

        assert len(expected) > len(expected_before)
        assert len(actual) > len(actual_snk_before)
        assert len(actual) > len(actual_replica_before)

        self.assert_list_prefix(expected, actual)

        self._check_actuals(expected, actual_replicas, actual_replica_before)

    def testLoseSnkActorWithRemoteSource(self):
        d, app_id, src, snk = self._start_app(replicate_snk=1)

        replica = utils.replicate(self.runtime, snk, self.dying_rt.id)
        utils.migrate(self.runtime, src, self.runtime2.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.runtime2, src, 'std.ReplicatedCountTimer')
        actual_snk_before = actual_tokens(self.runtime, snk)
        actual_replica_before = actual_tokens(self.dying_rt, replica)

        actors_before = utils.get_application_actors(self.runtime, app_id)
        self._kill_dying()
        time.sleep(1.3)

        actual_replicas = self._check_reliability(self.runtime, app_id, actors_before, 'simple:snk')

        self._check_dead_node(self.runtime, self.dying_rt, replica)

        expected = expected_tokens(self.runtime2, src, 'std.ReplicatedCountTimer')
        actual = actual_tokens(self.runtime, snk)

        assert len(expected) > len(expected_before)
        assert len(actual) > len(actual_snk_before)
        assert len(actual) > len(actual_replica_before)

        self.assert_list_prefix(expected, actual)

        self._check_actuals(expected, actual_replicas, actual_replica_before)

    def testLoseSrcActorWithLocalSink(self):
        d, app_id, src, snk = self._start_app(replicate_src=1)

        replica = utils.replicate(self.runtime, src, self.dying_rt.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.runtime, src, 'std.ReplicatedCountTimer')
        snk_before = actual_tokens(self.runtime, snk)
        replica_before = expected_tokens(self.dying_rt, replica, 'std.ReplicatedCountTimer')

        actors_before = utils.get_application_actors(self.runtime, app_id)
        self._kill_dying()
        time.sleep(1.3)

        actual_replicas = self._check_reliability(self.runtime, app_id, actors_before, 'simple:src', 'std.ReplicatedCountTimer')

        self._check_dead_node(self.runtime, self.dying_rt, replica)

        self._check_values(self.runtime, self.runtime, snk, src, 'std.ReplicatedCountTimer',
                           expected_before, snk_before, replica_before, actual_replicas)

    def testLoseSrcActorWithRemoteSink(self):
        d, app_id, src, snk = self._start_app(replicate_src=1)

        replica = utils.replicate(self.runtime, src, self.dying_rt.id)
        utils.migrate(self.runtime, src, self.runtime2.id)
        time.sleep(0.2)

        expected_before = expected_tokens(self.runtime2, src, 'std.ReplicatedCountTimer')
        snk_before = actual_tokens(self.runtime, snk)
        replica_before = expected_tokens(self.dying_rt, replica, 'std.ReplicatedCountTimer')

        actors_before = utils.get_application_actors(self.runtime, app_id)
        self._kill_dying()
        time.sleep(1.3)

        actual_replicas = self._check_reliability(self.runtime, app_id, actors_before, 'simple:src', 'std.ReplicatedCountTimer')

        self._check_dead_node(self.runtime, self.dying_rt, replica)

        self._check_values(self.runtime, self.runtime2, snk, src, 'std.ReplicatedCountTimer',
                           expected_before, snk_before, replica_before, actual_replicas)

    def testLoseReplicaNoReplication(self):
        d, app_id, src, snk = self._start_app() #Sould not replicate lost actor

        replica = utils.replicate(self.runtime, src, self.dying_rt.id)
        utils.migrate(self.runtime, src, self.runtime2.id)
        time.sleep(1.2)

        actors_before = utils.get_application_actors(self.runtime, app_id)

        self._kill_dying()
        time.sleep(1.3)

        app_actors = utils.get_application_actors(self.runtime, app_id)
        new_actors = [actor_id for actor_id in app_actors if actor_id not in actors_before]
        assert(len(new_actors) == 0)

        self._check_dead_node(self.runtime, self.dying_rt, replica)

    def testLoseNodeWithoutActors(self):
        self._kill_dying()
        time.sleep(1.3)

        assert self.dying_rt.id not in utils.get_nodes(self.runtime)
        assert self.dying_rt.id not in utils.get_nodes(self.runtime2)
        assert self.dying_rt.id not in utils.get_nodes(self.runtime3)


@pytest.mark.essential
@pytest.mark.slow
class TestOptimization(CalvinTestBase):

    def setUp(self):
        global ip_addr
        os.environ["CALVIN_TESTING"] = ""
        self.ip_addr = ip_addr
        self.runtime = runtime
        self.runtime2 = runtimes[0]
        self.runtime3 = runtimes[1]

        csruntime(ip_addr, port=5028, controlport=5029, attr={}, storage=True)
        time.sleep(0.2)
        self.storage_runtime = utils.RT("http://%s:5029" % self.ip_addr)

        conf = copy.deepcopy(_conf)
        conf.set('global', 'storage_proxy', "calvinip://%s:5028" % self.ip_addr)
        conf.set('global', 'storage_start', "1")
        conf.save("/tmp/calvin5030.conf")

        csruntime(ip_addr, port=5032, controlport=5033, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.runtime = utils.RT("http://%s:5033" % self.ip_addr)

        csruntime(ip_addr, port=5034, controlport=5035, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.runtime2 = utils.RT("http://%s:5035" % self.ip_addr)

        csruntime(ip_addr, port=5036, controlport=5037, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.runtime3 = utils.RT("http://%s:5037" % self.ip_addr)

        self.runtimes = [self.runtime, self.runtime2, self.runtime3]

        ids = {}
        for rt in self.runtimes:
            for i in range(5):
                try:
                    rt_id = utils.get_node_id(rt)
                    ids[rt] = rt_id
                    break
                except:
                    time.sleep(0.2)
        for key in ids:
            if not ids[key]:
                self._kill_all()
                assert False
            key.id = ids[key]

        utils.peer_setup(self.runtime, ["calvinip://%s:5034" % ip_addr])
        utils.peer_setup(self.runtime, ["calvinip://%s:5036" % ip_addr])
        utils.peer_setup(self.runtime2, ["calvinip://%s:5032" % ip_addr])
        utils.peer_setup(self.runtime2, ["calvinip://%s:5036" % ip_addr])
        utils.peer_setup(self.runtime3, ["calvinip://%s:5032" % ip_addr])
        utils.peer_setup(self.runtime3, ["calvinip://%s:5034" % ip_addr])

        time.sleep(0.2)

    def tearDown(self):
        self._kill_all()
        os.environ["CALVIN_TESTING"] = "1"

    def _add_reliable(self):
        csruntime(self.ip_addr, port=5030, controlport=5031, attr={},
                  configfile="/tmp/calvin5030.conf")
        time.sleep(0.2)
        self.reliable_rt = utils.RT("http://%s:5031" % self.ip_addr)
        for i in range(5):
            try:
                rt_id = utils.get_node_id(self.reliable_rt)
                self.reliable_rt.id = rt_id
                break
            except:
                time.sleep(0.2)

        self.runtimes.append(self.reliable_rt)

        utils.peer_setup(self.reliable_rt, ["calvinip://%s:5032" % ip_addr])
        utils.peer_setup(self.reliable_rt, ["calvinip://%s:5034" % ip_addr])
        utils.peer_setup(self.reliable_rt, ["calvinip://%s:5036" % ip_addr])

    def _kill_all(self):
        os.system("pkill -9 -f 'csruntime -n %s -p 5028'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5030'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5032'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5034'" % (self.ip_addr,))
        os.system("pkill -9 -f 'csruntime -n %s -p 5036'" % (self.ip_addr,))

    def _check_reliability(self, rt, app_id, actors_before, actor_name, actor_type=None):
        new_actors = utils.get_application_actors(rt, app_id)
        new_replicas = [actor_id for actor_id in new_actors if actor_id not in actors_before]
        actor_runtimes = self._check_app_reliability(rt, new_actors, app_id, actor_name)

        actual_replicas = []
        for actor_id, rt in actor_runtimes.iteritems():
            if actor_id in new_replicas:
                if "src" in actor_name:
                    actual_replicas.append(expected_tokens(rt, actor_id, actor_type))
                else:
                    actual_replicas.append(actual_tokens(rt, actor_id))

        return actual_replicas

    def _check_app_reliability(self, rt, new_actors, app_id, actor_name):
        reliabilities = []
        runtimes = self.runtimes
        runtimes.append(self.runtime)
        actor_runtimes = {}
        for actor in new_actors:
            a = utils.get_actor(rt, actor)
            if a:
                name = calvinuuid.remove_uuid(a['name'])
                if name == actor_name:
                    reliabilities.append(1 - utils.get_reliability(rt, a['node_id'], a['type']))
                for rt in runtimes:
                    if a['node_id'] == rt.id:
                        actor_runtimes[actor] = rt

        app = utils.get_application(rt, app_id)
        reliability = (1 - reduce(operator.mul, reliabilities, 1))
        assert reliability > app['required_reliability']

        return actor_runtimes

    def _check_dead_node(self, rt, dead_rt, dead_replica):
        self.assertIsNone(utils.get_actor(rt, dead_replica))
        self.assertIsNone(utils.get_node(rt, dead_rt.id))
        assert dead_rt.id not in utils.get_nodes(rt)

    def _check_values(self, rt1, rt2, snk, src, src_type, expected_before, snk_before, replica_before, actual_replicas):
        actual_snk = actual_tokens(rt1, snk)
        expected = expected_tokens(rt2, src, src_type)
        assert len(expected) > len(expected_before)
        assert len(actual_snk) > len(snk_before + replica_before)
        for actuals in actual_replicas:
            assert len(actuals) > 0
            assert len(actuals) > len(replica_before)

        counts = Counter(actual_snk)
        unique_elements = [val for val, cnt in counts.iteritems() if cnt <= len(actual_replicas)]
        doubles = [val for val, cnt in counts.iteritems() if cnt > len(actual_replicas) + 1]
        print "replicas: ", len(actual_replicas)
        print "unique: ", unique_elements
        print "doubles: ", doubles
        print "actual snk: ", actual_snk
        print "expected: ", expected

        assert len(unique_elements) > 0
        assert len(doubles) < 5

        for d in doubles:
            actual_snk.remove(d)

        filtered_expected = filter(lambda x: x not in unique_elements, expected)
        filtered_actual_snk = filter(lambda x: x not in unique_elements, actual_snk)
        filtered_expected_replicas = []
        for expected_replica in actual_replicas:
            filtered_expected_replicas.append(filter(lambda x: x not in unique_elements, expected_replica))

        filtered_expected_total = [x for x in filtered_expected]
        for i in range(len(actual_replicas)):
            filtered_expected_total += filtered_expected
        self.assert_list_prefix(sorted(filtered_expected_total), sorted(filtered_actual_snk))
        for filtered_expected_replica in filtered_expected_replicas:
            self.assert_list_prefix(sorted(filtered_expected), sorted(filtered_expected_replica))

    def _check_actuals(self, expected, actual_replicas, actual_replica_before):
        for actual_replica in actual_replicas:
            assert len(actual_replica) > len(actual_replica_before)
            self.assert_list_prefix(expected, actual_replica)

    def _start_app(self, replicate_snk=0, replicate_src=0):
        script = """
        src : std.ReplicatedCountTimer(replicate=""" + str(replicate_src) + """)
        snk : io.StandardOut(store_tokens=1, replicate=""" + str(replicate_snk) + """)
        src.integer > snk.token
        """

        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(self.runtime, app_info, deploy_info={
            "required_reliability": 0.95,
            "master_nodes": ["calvinip://%s:5032" % self.ip_addr]
        })
        app_id = d.deploy()
        self.deployer = d
        time.sleep(0.2)

        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']
        return (d, app_id, src, snk)

    def testOptimization(self):
        d, app_id, src, snk = self._start_app(replicate_snk=1)

        utils.replicate(self.runtime, snk, self.runtime2.id)
        time.sleep(0.1)
        utils.replicate(self.runtime, snk, self.runtime3.id)
        time.sleep(0.1)

        actors_before = utils.get_application_actors(self.runtime, app_id)

        utils.simulate_node_failure(self.runtime, self.runtime2.id, 1)
        utils.simulate_node_failure(self.runtime, self.runtime3.id, 1)
        self._add_reliable()
        time.sleep(5)

        actors_after = utils.get_application_actors(self.runtime, app_id)
        assert len(actors_before) > len(actors_after)


@pytest.mark.essential
@pytest.mark.slow
class TestCalvinScript(CalvinTestBase):

    def testCompileSimple(self):
        rt = self.runtime

        script = """
        src : std.CountTimer()
        snk : io.StandardOut(store_tokens=1)
        src.integer > snk.token
        """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        d.deploy() # ignoring app_id here
        self.deployer = d
        time.sleep(0.5)
        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']

        utils.disconnect(rt, src)

        actual = actual_tokens(rt, snk)
        expected = expected_tokens(rt, src, 'std.CountTimer')

        self.assert_list_prefix(expected, actual)

    def testDestroyAppWithLocalActors(self):
        rt = self.runtime

        script = """
        src : std.CountTimer()
        snk : io.StandardOut(store_tokens=1)
        src.integer > snk.token
        """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        app_id = d.deploy()
        time.sleep(0.2)
        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']

        applications = utils.get_applications(rt)
        assert app_id in applications

        d.destroy()

        applications = utils.get_applications(rt)
        assert app_id not in applications

        actors = utils.get_actors(rt)
        assert src not in actors
        assert snk not in actors

    def testDestroyAppWithMigratedActors(self):
        rt = self.runtime
        rt1 = self.runtimes[0]
        rt2 = self.runtimes[1]

        script = """
      src : std.CountTimer()
      snk : io.StandardOut(store_tokens=1)
      src.integer > snk.token
    """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        app_id = d.deploy()
        time.sleep(1.0)
        src = d.actor_map['simple:src']
        snk = d.actor_map['simple:snk']

        # FIXME --> remove when operating on closed pending connections during migration is fixed
        utils.disable(rt, src)
        utils.disable(rt, snk)
        # <--
        utils.migrate(rt, snk, rt1.id)
        utils.migrate(rt, src, rt2.id)

        applications = utils.get_applications(rt)
        assert app_id in applications

        d.destroy()

        for retry in range(1, 5):
            applications = utils.get_applications(rt)
            if app_id in applications:
                print("Retrying in %s" % (retry * 0.2, ))
                time.sleep(0.2 * retry)
            else:
                break
        assert app_id not in applications

        for retry in range(1, 5):
            actors = []
            actors.extend(utils.get_actors(rt))
            actors.extend(utils.get_actors(rt1))
            actors.extend(utils.get_actors(rt2))
            intersection = [a for a in actors if a in d.actor_map.values()]
            if len(intersection) > 0:
                print("Retrying in %s" % (retry * 0.2, ))
                time.sleep(0.2 * retry)
            else:
                break

        for actor in d.actor_map.values():
            assert actor not in actors

    def testCompileWithMultipleInports(self):
        rt = self.runtime

        script = """
      src_1 : std.CountTimer()
      src_2 : std.CountTimer()
      snk : io.StandardOut(store_tokens=1)
      src_1.integer > snk.token
      src_2.integer > snk.token
    """
        app_info, errors, warnings = compiler.compile(script, "simple")
        d = deployer.Deployer(rt, app_info)
        d.deploy()
        self.deployer = d
        time.sleep(0.5)
        src_1 = d.actor_map['simple:src_1']
        src_2 = d.actor_map['simple:src_2']
        snk = d.actor_map['simple:snk']

        utils.disconnect(rt, src_1)

        actual = actual_tokens(rt, snk)
        expected_1 = expected_tokens(rt, src_1, 'std.CountTimer')
        expected_2 = expected_tokens(rt, src_2, 'std.CountTimer')

        self.assert_list_prefix(sorted(expected_1 + expected_2), actual)


@pytest.mark.essential
@pytest.mark.slow
class TestMultipleInports(CalvinTestBase):
    def TestMultipleInports(self):
        """Testing outport remote to local migration"""
        rt = self.runtime

        snk = utils.new_actor_wargs(rt, 'io.StandardOut', 'snk', store_tokens=1)
        src_1 = utils.new_actor(rt, 'std.CountTimer', 'src')
        src_2 = utils.new_actor(rt, 'std.CountTimer', 'src')
        self.actors.update({snk:rt, src_1:rt, src_2:rt})

        utils.connect(rt, snk, 'token', rt.id, src_1, 'integer')
        utils.connect(rt, snk, 'token', rt.id, src_2, 'integer')
        time.sleep(0.2)

        expected_src_1 = expected_tokens(rt, src_1, 'std.CountTimer')
        expected_src_2 = expected_tokens(rt, src_2, 'std.CountTimer')
        expected = sorted(expected_src_1 + expected_src_2)
        actual_snk = actual_tokens(rt, snk)

        assert(len(actual_snk) > 1)
        self.assert_list_prefix(expected, sorted(actual_snk))


@pytest.mark.essential
@pytest.mark.slow
class TestPeerSetup(unittest.TestCase):

    def testPeerSetupSinglePeer(self):
        global ip_addr
        rt1, _ = dispatch_node(["calvinip://%s:5020" % (ip_addr, )], "http://localhost:5021")
        rt2, _ = dispatch_node(["calvinip://%s:5022" % (ip_addr, )], "http://localhost:5023")

        peers = [rt2.uri[0]]
        utils.peer_setup(rt1, peers)

        time.sleep(0.5)

        rt1_nodes = utils.get_nodes(rt1)
        rt2_nodes = utils.get_nodes(rt2)

        for peer in [rt1, rt2]:
            utils.quit(peer)
            time.sleep(0.2)

        assert rt2.id in rt1_nodes
        assert rt1.id in rt2_nodes

    def testPeerSetupMultiplePeers(self):
        global ip_addr
        rt1, _ = dispatch_node(["calvinip://%s:5010" % (ip_addr, )], "http://localhost:5011")
        rt2, _ = dispatch_node(["calvinip://%s:5012" % (ip_addr, )], "http://localhost:5013")
        rt3, _ = dispatch_node(["calvinip://%s:5014" % (ip_addr, )], "http://localhost:5015")

        peers = [rt2.uri[0], rt3.uri[0]]
        utils.peer_setup(rt1, peers)

        time.sleep(0.5)

        rt1_nodes = utils.get_nodes(rt1)
        rt2_nodes = utils.get_nodes(rt2)
        rt3_nodes = utils.get_nodes(rt3)

        for peer in [rt1, rt2, rt3]:
            utils.quit(peer)
            time.sleep(0.2)

        assert rt2.id in rt1_nodes
        assert rt3.id in rt1_nodes

        assert rt1.id in rt2_nodes
        assert rt3.id in rt2_nodes

        assert rt1.id in rt3_nodes
        assert rt2.id in rt3_nodes
