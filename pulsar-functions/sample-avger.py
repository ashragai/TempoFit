#!/usr/bin/env python

from pulsar import Function
import ast
from collections import deque, defaultdict

class runningAvg(Function):
  def __init__(self):
    self.usr_info = defaultdict(dict)    #{'hr' : deque(), 'delta_t' : deque(), 'interval' : 0, 'prev_t' : 0}

  def process(self, input, context):
    (idn, ts, hr) = ast.literal_eval(input)
    if ts == 0:
      self.usr_info[idn] = {'hr' : deque(), 'delta_t' : deque(), 'interval' : 0, 'prev_t' : 0}
      return str([idn, hr, ts])
    usrInfo = self.usr_info[idn]
    dt = ts - usrInfo['prev_t']
    if (usrInfo['interval'] + dt) >= 60:
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

    return str([idn, int(num / usrInfo['interval']), ts])