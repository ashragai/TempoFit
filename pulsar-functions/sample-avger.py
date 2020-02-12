#!/usr/bin/env python

from pulsar import Function
import pickle
import ast
from collections import deque, defaultdict

#Tabulates a running average of users' heart rates over a defined interval
class runningAvg(Function):
  def __init__(self):
    self.usr_info = defaultdict(dict) 
    self.avg_interval = 120 


  def process(self, input, context):
    idn, ts, hr = input.split(',')
    idn, ts, hr = int(idn), int(ts), float(hr)
    if ts == 0:
      self.usr_info[idn] = {'hr' : deque(), 'delta_t' : deque(), 'interval' : 0, 'prev_t' : 0}
      return "{},{},{}".format(idn, hr, ts)
    usrInfo = self.usr_info[idn]
    dt = ts - usrInfo['prev_t']
    if (usrInfo['interval'] + dt) >= self.avg_interval and len(usrInfo['delta_t']) != 0:
      usrInfo['interval'] += (dt - usrInfo['delta_t'][0])
      usrInfo['hr'].rotate(-1)
      usrInfo['hr'][-1] = hr
      usrInfo['delta_t'].rotate(-1)
      usrInfo['delta_t'][-1] = dt
    else:
      usrInfo['interval'] += dt
      usrInfo['delta_t'].append(dt)
      usrInfo['hr'].append(hr)
    usrInfo['prev_t'] = ts
    num = sum(usrInfo['hr'][i] * usrInfo['delta_t'][i] for i in range(len(usrInfo['delta_t'])))

    return "{},{},{}".format(idn, int(num / usrInfo['interval']), ts)