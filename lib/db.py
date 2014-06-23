import config
import log
import MySQLdb
import MySQLdb.cursors


def connect(host,user,password,db):
        try:
                con = MySQLdb.connect(host,user,password,db,cursorclass=MySQLdb.cursors.DictCursor)
                log.logger.debug('Succesfully connected to MySQL database')
                return con
        except Exception as e:
                log.logger.critical('Error with connecting to MySQL database')
                log.logger.debug(e)
                raise Exception

def update(db,q):

  try:
    cur = db.cursor()
    log.logger.debug('Succesfully set cursor to: ' + str(db))
  except Exception as e:
    log.logger.critical('Could not setup cursor to ' + str(db))
    log.logger.debug(e)
    raise Exception

  try:
    cur.execute(q)
    log.logger.debug('Succesfully send query to: ' + str(db))
  except Exception as e:
    log.logger.error('Error setting query: ' + q)
    log.logger.debug(e)
    raise Exception

  try:
    db.commit()
    log.logger.debug('Succcesfully commited the query to: ' + str(db))
  except Exception as e:
    log.logger.error('Error commiting query to ' + str(db))
    log.logger.debug(e)
    raise Exception


def query(db,q):
  try:
    cur = db.cursor()
    log.logger.debug('Succesfully set cursor to: ' + str(db))
  except Exception as e:
    log.logger.critical('Could not setup cursor to ' + str(db))
    log.logger.debug(e)
    raise Exception

  try:
      cur.execute(q)
      log.logger.debug('Succesfully send and returnd query to: ' + str(db))
      return cur.fetchall()
  except Exception as e:
      log.logger.critical('Error setting query: ' + q)
      log.logger.debug(e)
      raise Exception
