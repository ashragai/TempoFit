#!/usr/bin/env python

from pulsar import Function
import ast
from collections import defaultdict, deque
import random
import psycopg2 as psy
import os

#queries the million song database when prompted
#requests a song within a given tempo range reflecting a user's heart rate
class queryMSongs(Function):
  def __init__(self):
    self.con = None
    self.cur = None
    self.block_num = None
    self.dash_topic = None

    self.numSongs = 10
    self.usr_info = defaultdict(deque)

  def process(self, input, context):
    if self.con is None:
      DB= os.getenv('DB_NAME')
      HOST = os.getenv("DB_HOST")
      PORT = os.getenv('DB_PORT')
      USR = os.getenv('DB_USR')
      PWD = os.getenv('DB_PWD')
      self.con = psy.connect(dbname= DB, host = HOST, port = PORT, user= USR, password= PWD)
      self.cur = self.con.cursor()
    if self.dash_topic is None:
      self.dash_topic = context.get_user_config_value("dash")
      self.block_num =context.get_user_config_value("block")

    idn, avgHR = input.split(',')
    avgHR = float(avgHR)
    tempo_min, tempo_max = 0.90 * avgHR, 1.1*avgHR
    if len(self.usr_info[idn]) > 1:
        exclude = tuple(self.usr_info[idn])
    elif len(self.usr_info[idn]) == 1:
        exclude = "('" + str(self.usr_info[idn][0]) + "')"
    if len(self.usr_info[idn]) == 0:
        query = "SELECT songid, duration, artistname, title FROM test_idx WHERE tempo BETWEEN {} AND {} ORDER BY artistfamiliarity LIMIT 10;".format(tempo_min, tempo_max)
    else:
        query = "SELECT songid, duration, artistname, title FROM test_idx WHERE tempo BETWEEN {} AND {} AND songid NOT IN {} ORDER BY artistfamiliarity LIMIT 10;".format(tempo_min, tempo_max, exclude)
    self.cur.execute(query)
    res = self.cur.fetchall()
    if len(res) > 1:
        idx = random.randint(0, len(res) - 1)
        to_push = res[idx]
    elif len(res) == 1:
        to_push = res[0]
    else:
        to_push= self.usr_info[idn][0]
    if len(self.usr_info[idn]) < self.numSongs:
        self.usr_info[idn].append(to_push[0])
    else:
        self.usr_info[idn].rotate(-1)
        self.usr_info[idn][-1] = to_push[0]
    #res returned as songid, duration, artistname, title
    #push title and artist name to dash and songid and duration to function
    artName, songTitle = to_push[2].replace(',', ''), to_push[3].replace(',', '')
    msg = str([idn, avgHR, to_push[1], songTitle, artName])
    context.publish(self.dash_topic, msg)
    return "{},{}".format(idn, to_push[1])