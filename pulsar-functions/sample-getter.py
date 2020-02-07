#!/usr/bin/env python

from pulsar import Function
from collections import deque, defaultdict
import ast

class getSongs(Function):
    def __init__(self):
        self.usr_info = defaultdict(dict)           #{'song-list' : deque(), 't-end' : 0, 'q-stat' : False}
        self.window = 10
        self.numSongs = 5
        self.avg_topic = None

    def process(self, input, context):
        if self.avg_topic is None:
            self.avg_topic = "avg-output-" + str(context.get_user_config_value("block"))
        name = str(context.get_current_message_topic_name())
        if name.endswith(self.avg_topic):           
            (idn, avgHR, t) = ast.literal_eval(input)
            if t == 0:
                self.usr_info[idn]['song-list'] = []
                self.usr_info[idn]['t-end'] = 0
                self.usr_info[idn]['q-stat'] = False
                return str([idn, avgHR, list(self.usr_info[idn]['song-list'])])
            elif ((self.usr_info[idn]['t-end'] - t) < self.window and (self.usr_info[idn]['q-stat'] is False)):
                self.usr_info[idn]['q-stat'] =True
                return str([idn, avgHR, list(self.usr_info[idn]['song-list'])])
            else:
                return
        else:
            (idn, songid, duration) = ast.literal_eval(input)
            if len(self.usr_info[idn]['song-list']) < self.numSongs:
                self.usr_info[idn]['song-list'].append(songid)
            else:
                self.usr_info[idn]['song-list'].rotate(-1)
                self.usr_info[idn]['song-list'][-1] = songid
            self.usr_info[idn]['t-end'] += int(duration)
            self.usr_info[idn]['q-stat'] = False
            return 
        



        


