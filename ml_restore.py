#!/usr/bin/env python

from lib import *

# good restore example:
# directory = /20130610
# file = 1292_HERBARIUMWAG_20130610235917.tar
# restore.getTar('20130718','0235_HERBARIUMWAG_20130718000130.tar')
#print config.get('db_host','tar_index')
#print config.get('db_host','ml_analyse')



#### DOWNLOADING AND INDEXING PART
# requests = restore.getTar(5)
#
# ftp_con = restore.openFTP()
# for req in requests:
#   try:
#     size = restore.checkRemoteFile(ftp_con,req['remote_dir'],req['tar'])
#   except Exception as e:
#     log.logger.debug(e)
#   if size:
#     log.logger.info('Size of ' + req['tar'] + ' : ' + str( round(float(size)/float(1024**2),3) ) + 'MB')
#     #if size > (1*1024*1024):
#       #print 'candidate'
#     try:
#       restore.downloadTar(ftp_con,req['remote_dir'],req['tar'])
#     except Exception as e:
#       log.logger.error(e)
#     try:
#       restore.indexTar(req['tar'])
#     except Exception as e:
#       log.logger.error('Could not index tar')
#
#   else:
#     print 'could not return size'
#
# restore.closeFTP(ftp_con)
################

##### Extract and convert #############

# try:
#   filename = restore.extractTar(15)
# except Exception as e:
#   log.logger.info(e)
# image.convertToJpeg(filename,'/tmp')

#######################################
for unmatched in match.getUnmatched(1):
  #can = match.getCandidates(unmatched['name'])
  can = match.getCandidates('000050120-2013-05-27 1872')
  if len(can) == 0:
    print 'no candidates, no restore'
    pass
  else:
    for c in can:
      print c


#### use this to update ml_wag database with good filename search
# match.updateFilename()
####



###### Image Matching ################
# print image.matchHistogram('/tmp/000704693-WAG.1512446.jpg','/tmp/000704682-WAG.1392547.jpg')
# print image.matchHistogram('/tmp/000704693-WAG.1512446.jpg','/tmp/000704693-WAG.1512446.jpg')
############################# #########
