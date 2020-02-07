#!/usr/bin/env python

from pulsar import Function
import ast
import psycopg2 as psy

class queryMSongs(Function):
  def __init__(self):
    self.con = None 
    self.cur = None
    self.dash_topic = None 

  def process(self, input, context):
    if self.con is None:
      DB= 'millionsongs'
      HOST = 
      PORT = 5439
      USR = 
      PWD = 
      self.con = psy.connect(dbname= DB, host = HOST, port = PORT, user= USR, password= PWD)
      self.cur = self.con.cursor()
    if self.dash_topic is None:
      self.dash_topic = context.get_user_config_value("dash")

    (idn, avgHR, songs) = ast.literal_eval(input)
    tempo_min, tempo_max = 0.95 * avgHR, 1.05*avgHR
    if len(songs) > 1:
        exclude = tuple(songs)
    elif len(songs) == 1:
        exclude = "('" + str(songs[0]) + "')"
    elif len(songs) == 0:
        query = "SELECT songid, duration, artistname, title FROM msongs WHERE tempo BETWEEN {} AND {};".format(tempo_min, tempo_max)
    else:
        query = "SELECT songid, duration, artistname, title FROM msongs WHERE tempo BETWEEN {} AND {} AND songid NOT IN {};".format(tempo_min, tempo_max, exclude)
    self.cur.execute(query)
    res = self.cur.fetchall()
    
    to_push = res[0]        
    #res returned as songid, duration, artistname, title
    #push title and artist name to dash and songid and duration to function
    
    context.publish(self.dash_topic, str([idn, to_push[3], to_push[2]]).encode('utf-8'))
    return str([idn, to_push[0], to_push[1]]).encode('utf-8')