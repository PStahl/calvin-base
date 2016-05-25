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

import json

from calvin.actor.actor import Actor, ActionResult, manage, condition, guard
from calvin.runtime.north.calvin_token import EOSToken, ExceptionToken
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class VideoReader(Actor):

    """
    Read a file line by line, and send each line as a token on output port

    Inputs:
      filename : File to read. If file doesn't exist, an ExceptionToken is produced
    Outputs:
      out : data with frame, frame count and url to send it to
    """

    @manage([])
    def init(self, sleep=0.005):
        self.did_read = False
        self.file_not_found = False
        self.video = None
        self.use(requirement='calvinsys.media.videohandler', shorthand='video')
        self.end_of_file = False
        self.filename = None
        self.url = None
        self.can_read = False
        self.images = []
        self.use("calvinsys.events.timer", shorthand="timer")
        self.timer = self['timer'].repeat(sleep)

    @condition(['filename'])
    @guard(lambda self, filename: not self.video and not self.filename)
    def open_file(self, data):
        self.frame_count = 0
        data = json.loads(data)
        self.url = data['url']
        self.filename = data['filename']
        self.can_read = True
        try:
            self.video = self['video'].open("videos/" + self.filename, 640, 480)
            self.end_of_file = False
        except Exception as e:
            self.video = None
            self.file_not_found = True

        #self.images = []
        #image = self.video.get_image()
        #while image:
        #    self.images.append(image)
        #    image = self.video.get_image()
        return ActionResult()

    @condition([])
    @guard(lambda self: self.file_not_found)
    def file_not_found(self):
        token = ExceptionToken(value="File not found")
        self.file_not_found = False  # Only report once
        return ActionResult(production=(token, ))

    @condition([], ['out'])
    @guard(lambda self: self.timer.triggered and self.video and self.url and not self.end_of_file)
    def read(self):
        _log.info("READ: {}".format(self.frame_count))
        self.timer.ack()
        data = {
            'frame_count': -1,
            'url': self.url
        }
        image = self.video.get_image()  # images[self.frame_count]  # self.video.get_image()
        if image:  # self.frame_count < len(self.images):
            #image = self.images[self.frame_count]  # self.video.get_image()
            data['frame'] = image
            data['frame_count'] = self.frame_count
            self.frame_count += 1
        else:
            self.end_of_file = True

        #self.can_read = False
        _log.info("READ DONE: {}".format(self.frame_count))
        return ActionResult(production=(data, ))

    @condition([])
    @guard(lambda self: self.video and self.end_of_file)
    def eof(self):
        self.video.close()
        self.video = None
        self.filename = None
        self.can_read = False
        return ActionResult()

    action_priority = (open_file, file_not_found, read, eof)
    requires =  ['calvinsys.media.videohandler']

    test_set = []
