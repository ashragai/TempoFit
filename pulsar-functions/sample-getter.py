#!/usr/bin/env python

from pulsar import Function
from collections import deque, defaultdict
import ast

#tracks users' playlists and prompts a new database query when the previous song 
#is close to ending. Also track whether each user has a query in progress to 
#avoid producing extra query requests
class getSongs(Function):
    def __init__(self):
        self.usr_info = defaultdict(dict)     
        self.window = 20
        self.numSongs = 10
        self.avg_topic = None

    def process(self, input, context):
        if self.avg_topic is None:
            self.avg_topic = "avgoutput-" + str(context.get_user_config_value("block"))
        name = str(context.get_current_message_topic_name())
        if name.endswith(self.avg_topic):
            idn, avgHR, t = input.split(',')
            avgHR, t = float(avgHR), int(t)
            if t == 0:
                self.usr_info[idn]['t-end'] = 0
                self.usr_info[idn]['q-stat'] = False
                return "{},{}".format(idn, avgHR)
            elif ((self.usr_info[idn]['t-end'] - t) < self.window and (self.usr_info[idn]['q-stat'] is False)):
                self.usr_info[idn]['q-stat'] =True
                return "{},{}".format(idn, avgHR)
            else:
                return
        else:
            idn, duration = input.split(',')
            duration = float(duration)
            self.usr_info[idn]['t-end'] += duration
            self.usr_info[idn]['q-stat'] = False
            return