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

import socket
import json
import copy

from calvin.actor.actor import Actor, ActionResult, manage, condition
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class VideoEncoder(Actor):

    """
    Read a file line by line, and send each line as a token on output port

    Inputs:
      in : data to encode and send
    """

    @manage([])
    def init(self, replicate=False):
        self.did_read = False
        self.frame_buffer = {}
        self.done_sending = False
        self.use("calvinsys.media.encoder", shorthand="encoder")
        self.encoder = self["encoder"]
        self._replicate = replicate
        self.s = None

    @property
    def replicate(self):
        return self._replicate

    @condition(['in'])
    def encode(self, data):
        #_log.info("\n\n\nENCODE")
        data = copy.deepcopy(data)
        url = data['url']
        host = url.split(":")[0]
        port = int(url.split(":")[1])

        frame = data.get('frame')

        if isinstance(self.encoder, dict):
            self.use("calvinsys.media.encoder", shorthand="encoder")
            self.encoder = self["encoder"]

        if frame:
            data['frame'] = self.encoder.encode(frame, ".jpg").decode("latin-1")

        data['id'] = self.id
        data['url'] = self.node._clean_uri()

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
        except Exception as e:
            print "failed to connect.", e
            return ActionResult(production=())
        try:
            s.sendall(json.dumps(data))
        except Exception as e:
            print "failed to send.", e
            pass

        #_log.info("ENCODE DONE")
        return ActionResult(production=())

    action_priority = (encode, )
    requires =  ['calvinsys.media.encoder']

    test_set = []
