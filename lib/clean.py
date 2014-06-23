import config
import log
from datetime import datetime
import db
import os
import image

def cleanTar():
  try:
    tar_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))
    log.logger.debug('Succesfully connected to database on: ' + config.get('db_host'))
  except Exception as e:
    log.logger.critical('Could not connect database to :' + config.get('db_host'))
    log.logger.debug(e)

  q = 'SELECT DISTINCT tar FROM tar_index WHERE purged = 0'

  try:
    tars = db.query(tar_db,q)
    log.logger.debug('Succesfully queried tha tar_index database')
  except Exception as e:
    log.logger.critical('Unable to query tar_index database with ' + q)
    log.logger.debug(e)

  for tar in tars:
    q = 'SELECT tar FROM tar_index WHERE processed = 0 AND processed_2013 = 0 AND tar = ' + tar['tar']
    try:
      tar_count = db.query(tar_db,q)
      log.logger.debug('Succesfully queried tha tar_index database')
    except Exception as e:
      log.logger.critical('Unable to query tar_index database with ' + q)
      log.logger.debug(e)

    if len(tar_count[0]) == 0:
      # all is processed, tar can be pruged.
      try:
        os.remove(tar['tar'])
        log.logger.info('Cleanup tar: ' + tar['tar'])
      except Exception as e:
        os.logger.error('Could not remove tar: ' + tar['tar'])
        os.logger.debug(e)

      q = 'UPDATE tar_index SET purged = 1 WHERE tar = ' + tar['tar']

      try:
        db.update(tar_db,q)
        log.logger.debug('Updated tar db')
      except Exception as e:
        log.logger.error('could not update tar_index with ' + q)
        log.logger.debug(e)
