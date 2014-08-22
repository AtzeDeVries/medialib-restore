#!/usr/bin/env python

from lib import *
import os

path = '/data/tar/'

filelist = [ f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f)) ]

for file in filelist:
  try:
    restore.indexTar(path+file)
  except Exception as e:
    log.logger.error('Could not index tar')
