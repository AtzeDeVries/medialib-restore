import config
import log
from datetime import datetime
import db
import os
import image
import restore
import tools


####
# create table match_index (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255), tar_index_id INT(255), proccess_datetime DATETIME , match_value DOUBLE , ml_wag_id INT(255) );

def getUnmatched():
  try:
    ml_db = db.connect(config.get('db_host','ml_analyse'),config.get('db_user','ml_analyse'),config.get('db_password','ml_analyse'),config.get('db_name','ml_analyse'))
    log.logger.debug('Succesfully connected to database ' + config.get('db_host','ml_analyse'))
  except Exception as e:
    log.logger.critical('Could not connect to database ' + config.get('db_host','ml_analyse'))
    log.logger.debug(e)
    exit(1)

  q = 'SELECT qr,path,selected,evaluated,Id,scan_date,match_box,match_file FROM ml WHERE uniq > 1 AND neglect = 0 AND restore_matched = 0  AND selected = 1 AND upload_date < "2013-08-31" ORDER BY RAND() limit 250'

  try:
    unmatched = db.query(ml_db,q)
    log.logger.debug('Succesfully queried tha ml_db database')
    return unmatched
  except Exception as e:
    log.logger.critical('Unable to query  ml_db database')
    log.logger.debug(e)


def getCandidates(db_object):
  try:
    tar_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))
    log.logger.debug('Succesfully connected to database on: ' + config.get('db_host'))
  except Exception as e:
    log.logger.critical('Could not connect database to :' + config.get('db_host'))
    log.logger.debug(e)
    exit(1)

  filename = db_object['path'].split('/')[-1][0:-(len(db_object['path'].split('.')[-1]) + 1)]

  #print filename

  q = 'SELECT id,tar,filename,tar_index,name FROM tar_index WHERE name = "' + filename + '"'

  try:
    candidates = db.query(tar_db,q)
    log.logger.debug('Succesfully queried tha tar_db database')
  except Exception as e:
    log.logger.critical('Unable to query tar_db database')
    log.logger.debug(e)

  # print candidates

  # if '-2013-' in name:
  #   qrs = []
  #   for c in candidates:
  #     if not c['qr'] in qrs:
  #       qrs.append(c['qr'])
  #   candidates = []
  #   qr_join = '"' + '","'.join(qrs) + '"'
  #   #for qr in qrs:
  #   q = 'SELECT qr,path,selected,evaluated,Id FROM ml WHERE qr in (' + qr_join + ') AND uniq > 1 AND neglect = 0'
  #
  #   try:
  #     candidates = db.query(ml_db,q)
  #     log.logger.debug('Succesfully queried tha ml_db database')
  #   except Exception as e:
  #     log.logger.critical('Unable to query ml_db database')
  #     log.logger.debug(e)

  return candidates


def fix2013(setSize):
  try:
    tar_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))
    log.logger.debug('Succesfully connected to database on: ' + config.get('db_host'))
  except Exception as e:
    log.logger.critical('Could not connect database to :' + config.get('db_host'))
    log.logger.debug(e)

  try:
    ml_db = db.connect(config.get('db_host','ml_analyse'),config.get('db_user','ml_analyse'),config.get('db_password','ml_analyse'),config.get('db_name','ml_analyse'))
    log.logger.debug('Succesfully connected to database ' + config.get('db_host','ml_analyse'))
  except Exception as e:
    log.logger.critical('Could not connect to database ' + config.get('db_host','ml_analyse'))
    log.logger.debug(e)
    exit(1)

  q = 'SELECT id,name,filename FROM tar_index WHERE proccessed = 1 AND processed_2013 = 0 LIMIT ' + str(setSize)

  try:
    unfixed = db.query(tar_db,q)
    log.logger.debug('Succesfully queried tha tar_index database')
  except Exception as e:
    log.logger.critical('Unable to query tar_index database')
    log.logger.debug(e)

  for unfix in unfixed:
    #filename = restore.extractTar(unfix['id'])
    q = 'SELECT Id,path,qr,uniq FROM ml WHERE selected = 1 AND uniq = 1 AND Filename = "' + unfix['name'] + '"'
    try:
      qr_can = db.query(ml_db,q)
      log.logger.debug('Succesfully queried tha ml database')
    except Exception as e:
      log.logger.critical('Unable to query ml database with ' + q)
      log.logger.debug(e)
    if len(qr_can) == 1:

      q = 'UPDATE ml SET restore_match_id = ' + str(unfix['id']) + ' WHERE Id = ' + str(qr_can[0]['Id'])
      try:
        db.update(ml_db,q)
        log.logger.debug('Updated ml_id')
      except Exception as e:
        log.logger.error('could not update ml_db with ' + q)
        log.logger.debug(e)

      q = 'INSERT INTO restore_index '
      q += 'subscription_timstamp=' + str(datetime.now()) + ', '
      q += 'tar_id=' + str(unfix['id']) + ', '
      q += 'ml_id=' + str(qr_can[0]['Id']) + ', '
      q += 'current_path=' + str(qr_can[0]['path']) + ', '
      q += 'new_path=' + config.get('restore_upload_dir') + '/' + str(qr_can[0]['qr']) + '.tif'

      try:
        db.update(tar_db,q)
        log.logger.debug('added record to restore_index')
      except Exception as e:
        log.logger.error('could not update tar_db with ' + q)
        log.logger.debug(e)

    else:
      #this could theorecticly end up with 3 matches. Image matching should be done
      session_dir = '/tmp/ ' + str(datetime.now().__format__('%Y.%m.%d.%H.%M.%S.%f'))
      os.mkdir(session_dir)
      image.convertToJpeg(unfix['filename'],session_dir)
      match_values = []
      for c in qr_can:
        match_values.append(image.matchHistogram(session_dir + '/ ' + unfix['name'] + '.jpg', config.get('medialib_root_mount') + c['path']))
      match = match_values.index(min(match_values))
      os.remove(session_dir + '/' + unfix['name'] + '.jpg')
      os.remove(session_dir)

      q = 'UPDATE ml SET restore_match_id = ' + str(unfix['id']) + ' WHERE Id = ' + str(qr_can[match]['Id'])
      try:
        db.update(ml_db,q)
        log.logger.debug('Updated ml_id')
      except Exception as e:
        log.logger.error('could not update ml_db with ' + q)
        log.logger.debug(e)

      q = 'INSERT INTO restore_index '
      q += 'subscription_timstamp=' + str(datetime.now()) + ', '
      q += 'tar_id=' + str(unfix['id']) + ', '
      q += 'ml_id=' + str(qr_can[match]['Id']) + ', '
      q += 'current_path=' + str(qr_can[match]['path']) + ', '
      q += 'new_path=' + config.get('restore_upload_dir') + '/' + str(qr_can[match]['qr']) + '.tif'

      try:
        db.update(tar_db,q)
        log.logger.debug('added record to restore_index')
      except Exception as e:
        log.logger.error('could not update tar_db with ' + q)
        log.logger.debug(e)


def select(select_object,candidates_object):
  if not os.path.isfile(select_object['path']):
    log.logger.critical('Cannot find master image: ' + select_object['path'])
    exit(1)

  match = []

  for c in candidates_object:
    r = restore.extractTar(c['id'])
    if not os.path.isfile(r):
      log.logger.critical('Cannot find restore image: ' + r)
      exit(1)
    image.convertToJpeg(c['filename'],'/tmp/')
    match.append(image.matchHistogram(select_object['path'],'/tmp/' + c['name'] + '.jpg'))
    os.remove('/tmp/' + c['name'] + '.jpg')

  m = match.index(min(match))

  #print 'THE MATCH VALUE IS!!!: ' + str(min(match))

  #print select_object
  #print candidates_object[m]

  if select_object['scan_date'] is None:
    put_dir = os.path.join('/data/selected/','unknown_scandir')
  else:
    put_dir = os.path.join('/data/selected/',str(select_object['scan_date']))

  jpg_put_dir = os.path.join(put_dir,'jpeg')
  csv_put_dir = os.path.join(put_dir,'csv')

  if not os.path.exists(put_dir):
    os.makedirs(jpg_put_dir)
  if not os.path.exists(jpg_put_dir):
    os.makedirs(jpg_put_dir)
  if not os.path.exists(csv_put_dir):
    os.makedirs(csv_put_dir)

  try:
    os.rename(candidates_object[m]['filename'],os.path.join(put_dir,select_object['qr'] + '.tif'))
  except Exception as e:
    log.logger.critical('Could not move: ' + candidates_object[m]['filename'] + ' to: ' + os.path.join(put_dir,select_object['qr'] + '.tif'))
    log.logger.debug(e)
    exit(1)

  try:
    image.convertToJpeg(os.path.join(put_dir,select_object['qr'] + '.tif'),jpg_put_dir,thumbnail=True)
  except Exception as e:
    log.logger.critical('Could not create thumnail from: ' + candidates_object[m]['filename'] + ' to: /tmp/' + candidates_object[m]['name'] + '.jpg')
    log.logger.debug(e)
    exit(1)

  # try:
  #   f = open(os.path.join(jpg_put_dir,select_object['qr'] + '.txt'),'w')
  #   f.write('\\\\nnms125\\Master-Images' + select_object['path'][13:].replace('/','\\'))
  #   f.close()
  # except Exception as e:
  #   log.logger.error(e)

  for c in candidates_object:
    if os.path.exists(c['filename']):
      try:
        os.remove(c['filename'])
        log.logger.info('Succesfully removed temponary file: ' + c['filename'])
      except Exception as e:
        log.logger.waring('Could not remove temponary file: ' + c['filename'] + ' Please cleanup manually')
        log.logger.debug(e)

  #['qr','check_it','tiff','jpg','master','scan_date','wag_jpg_link','box','analytics_database_id','best_match_value','second_best_match_value','match_factor','match_diff','correct','false_description']
  ma = tools.match_analytics(match)
  rowfill = [select_object['qr'],
             tools.checkit(1000,80),
             '\\\\10.61.2.125\\selected\\' + str(select_object['scan_date']) + '\\' + select_object['qr'] + '.tif',
             '\\\\10.61.2.125\\selected\\' + str(select_object['scan_date']) + '\\jpeg\\' + select_object['qr'] + '.jpg',
             '\\\\nnms125\\Master-Images' + select_object['path'][13:].replace('/','\\'),
             str(select_object['scan_date']),
             '\\\\nnm\\dino\\Digibarium\\FES herbarium digistraten\\' + select_object['match_file'].replace('/','\\'),
             select_object['match_box'],
             select_object['Id'],
             ma[0],
             ma[1],
             ma[2],
             ma[3]
             ]

  tools.write_csv(os.path.join(csv_put_dir,str(select_object['scan_date']) + '.csv'),
                  rowfill,
                  )


def selectMatch(subject):
  can = getCandidates(subject['name'])
  if len(can) == 0:
    try:
      tar_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))
      log.logger.debug('Succesfully connected to database on: ' + config.get('db_host'))
    except Exception as e:
      log.logger.critical('Could not connect database to :' + config.get('db_host'))
      log.logger.debug(e)

    try:
      ml_db = db.connect(config.get('db_host','ml_analyse'),config.get('db_user','ml_analyse'),config.get('db_password','ml_analyse'),config.get('db_name','ml_analyse'))
      log.logger.debug('Succesfully connected to database ' + config.get('db_host','ml_analyse'))
    except Exception as e:
      log.logger.critical('Could not connect to database ' + config.get('db_host','ml_analyse'))
      log.logger.debug(e)
      exit(1)

    q = 'UPDATE tar_index SET processed = 1 WHERE id = ' + str(subject['id'])
    try:
      db.update(tar_db,q)
      log.logger.debug('Updated tar db')
    except Exception as e:
      log.logger.error('could not update tar_index with ' + q)
      log.logger.debug(e)

  else:

    #filename = restore.extractTar(subject['id'])
    session_dir = '/tmp/ ' + str(datetime.now().__format__('%Y.%m.%d.%H.%M.%S.%f'))
    os.mkdir(session_dir)
    image.convertToJpeg(subject['filename'],session_dir)
    match_values = []
    for c in can:
      match_values.append(image.matchHistogram(session_dir + '/' + subject['name'] + '.jpg', config.get('medialib_root_mount') + c['path']))
    match = match_values.index(min(match_values))
    os.remove(session_dir + '/' + subject['name'] + '.jpg')
    os.remove(session_dir)

    if can[match]['selected'] == 0:
      # selected candidate should not be updated
      q = 'UPDATE tar_index SET processed = 1 WHERE id = ' + str(subject['id'])
      try:
        db.update(tar_db,q)
        log.logger.debug('Updated tar db')
      except Exception as e:
        log.logger.error('could not update tar_index with ' + q)
        log.logger.debug(e)
    elif match == 0:
      #since can array is sorted by upload date reversed, newest (current) is array entry zero, so current is ok if match = 0
      q = 'UPDATE tar_index SET processed = 1,match_value = ' + str(match_values[match]) + ',ml_id = ' + str(can[match]['Id']) + ' WHERE id = ' + str(subject['id'])
      try:
        db.update(tar_db,q)
        log.logger.debug('Updated tar db')
      except Exception as e:
        log.logger.error('could not update tar_index with ' + q)
        log.logger.debug(e)

      q = 'UPDATE ml SET restore_match_id = ' + str(subject['id']) + ' WHERE Id = ' + str(can[match]['Id'])
      try:
        db.update(ml_db,q)
        log.logger.debug('Updated ml_id')
      except Exception as e:
        log.logger.error('could not update ml_db with ' + q)
        log.logger.debug(e)

    else:
      q = 'SELECT id,match_value FROM tar_index WHERE ml_id = ' + str(can[match]['Id']) + ' ORDER BY match_value DESC'
      try:
        others = db.query(tar_db,q)
        log.logger.debug('queried tar db')
      except Exception as e:
        log.logger.error('could not query tar_id with ' + q)
        log.logger.debug(e)

      if others[0]['match_value'] >= match_values[match]:
        #case when new match is worse that older match
        q = 'UPDATE tar_index SET processed = 1 WHERE id = ' + str(subject['id'])
        try:
          db.update(tar_db,q)
          log.logger.debug('Updated tar db')
        except Exception as e:
          log.logger.error('could not update tar_index with ' + q)
          log.logger.debug(e)
      else:
        #case when new match is bettar
        q = 'UPDATE tar_index SET processed=1,restored=1,match_value = ' + str(match_values[match]) + ',ml_id = ' + str(can[match]['Id']) + ' WHERE id = ' + str(subject['id'])
        try:
          db.update(tar_db,q)
          log.logger.debug('Updated tar db')
        except Exception as e:
          log.logger.error('could not update tar_index with ' + q)
          log.logger.debug(e)

        q = 'UPDATE tar_index SET restored = NULL,match_value = NULL,ml_id = NULL  WHERE id = ' + str(others[0]['id'])
        try:
          db.update(tar_db,q)
          log.logger.debug('Updated tar db')
        except Exception as e:
          log.logger.error('could not update tar_index with ' + q)
          log.logger.debug(e)

        q = 'UPDATE ml SET restore_match_id = ' + str(subject['id']) + ' WHERE Id = ' + str(can[match]['Id'])
        try:
          db.update(ml_db,q)
          log.logger.debug('Updated ml_id')
        except Exception as e:
          log.logger.error('could not update ml_db with ' + q)
          log.logger.debug(e)

        q = 'INSERT INTO restore_index '
        q += 'subscription_timstamp=' + str(datetime.now()) + ', '
        q += 'tar_id=' + str(subject['id']) + ', '
        q += 'ml_id=' + str(can[match]['Id']) + ', '
        q += 'current_path=' + str(can[match]['path']) + ', '
        q += 'new_path=' + config.get('restore_upload_dir') + '/' + str(can[match]['qr']) + '.tif'

        try:
          db.update(tar_db,q)
          log.logger.debug('added record to restore_index')
        except Exception as e:
          log.logger.error('could not update tar_db with ' + q)
          log.logger.debug(e)


def updateFilename():
    try:
      ml_db = db.connect(config.get('db_host','ml_analyse'),config.get('db_user','ml_analyse'),config.get('db_password','ml_analyse'),config.get('db_name','ml_analyse'))
      log.logger.debug('Succesfully connected to database ' + config.get('db_host','ml_analyse'))
    except Exception as e:
      log.logger.critical('Could not connect to database ' + config.get('db_host','ml_analyse'))
      log.logger.debug(e)
      exit(1)

    q = 'SELECT Id,path FROM ml'

    try:
      records = db.query(ml_db,q)
      log.logger.debug('Succesfully queried tha ml_db database')
    except Exception as e:
      log.logger.critical('Unable to query ml_db database')
      log.logger.debug(e)

    for record in records:
      filename_full = record['path'].split('/')[-1]
      filename = filename_full[:-len(filename_full.split('.')[-1]) - 1]
      #print filename + '\t' + record['path']
      q = 'UPDATE ml SET filename = "' + str(filename) + '" WHERE Id = ' + str(record['Id'])
      try:
        db.update(ml_db,q)
      except Exception as e:
        log.logger.cirtical('Could not update id ' + str(record['Id']))
