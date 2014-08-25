import ftplib
import config
import log
import tarfile
from datetime import datetime
import db

def getRemoteTars():
	try:
		ml_db = db.connect(config.get('db_host','medialib_live'),config.get('db_user','medialib_live'),config.get('db_password','medialib_live'),config.get('db_name','medialib_live'))
		log.logger.debug('Succesfully connected to database ' + config.get('db_host','medialib_live') )
	except Exception as e:
		log.logger.critical('Could not connect to database '+ config.get('db_host','medialib_live'))
		log.logger.debug(e)
		exit(1)

	#q = 'SELECT name,remote_dir FROM tar_file WHERE name LIKE "%_HERBARIUMWAG_%" AND backup_created BETWEEN "' + date + ' 00:00:00" AND "' + date + ' 23:59:59"'
	q = 'SELECT name,remote_dir FROM tar_file WHERE name LIKE "%_HERBARIUMWAG_%" AND backup_created < "2013-08-01"'
	try:
		tars = db.query(ml_db,q)
		log.logger.debug('Succesfully queried to database ' + config.get('db_host','medialib_live') )
		return tars
	except Exception as e:
		log.logger.critical('Could not query to database '+ config.get('db_host','medialib_live'))
		log.logger.debug(e)
		exit(1)

def indexRemoteTars(tar_list):

	try:
		tar_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))
		log.logger.debug('Succesfully connected to database on: ' + config.get('db_host'))
	except Exception as e:
		log.logger.critical('Could not connect database to :' + config.get('db_host'))
		log.logger.debug(e)


	for tar in tar_list:
		try:
			if int(tar['name'].split('_')[2][:8]) < 20130924:
				remote_dir = tar['name'].split('_')[2][:8]
			elif tar['remote_dir'] is None:
				remote_dir = ''
			else:
				remote_dir = tar['remote_dir']

			q = 'INSERT INTO ftp_index (tar,remote_dir) VALUES ("'+tar['name']+'","'+str(remote_dir)+'")'
			db.update(tar_db,q)
			log.logger.debug('Succesfully added this record to the database: ' + tar['name'])
		except Exception as e:
			log.logger.critical('Could not add record ' + tar['name'] + ' this is a CRITICAL -> EXITING')
			exit(1)

def getTar(setsize):
	try:
		ftp_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))
		log.logger.debug('Succesfully connected to database on: ' + config.get('db_host'))
	except Exception as e:
		log.logger.critical('Could not connect database to :' + config.get('db_host'))
		log.logger.debug(e)

	q = 'SELECT * FROM ftp_index WHERE downloaded = 0 ORDER BY id DESC limit ' + str(setsize)
	try:
		tars = db.query(ftp_db,q)
		log.logger.debug('Succesfully queried to database ' + config.get('db_host') )
		return tars
	except Exception as e:
		log.logger.critical('Could not query to database '+ config.get('db_host'))
		log.logger.debug(e)
		exit(1)

	#for tar in tars:
	#	print tar


def openFTP():
	ftp = ftplib.FTP(config.get('ftp_host'))
	try:
		ftp.login(config.get('ftp_username'),config.get('ftp_password'))
		ftp.set_pasv(True)
		log.logger.debug('FTP connection to ' + config.get('ftp_host') + ' succesfull')
		return ftp
	except Exception as e:
		log.logger.critical('Fail during setup of ftp connection to ' + config.get('ftp_host') )
		log.logger.debug(e)
		raise Exception

def closeFTP(ftp):
	try:
		ftp.close()
		log.logger.debug('FTP connection close of ' + config.get('ftp_host') + ' succesfull')
	except Exception as e:
		log.logger.critical('Fail during closing of ftp connection to ' + config.get('ftp_host') )
		log.logger.debug(e)
		raise Exception

def checkRemoteFile(ftp,path,tarfile):
	try:
		ftp.cwd('/'+path)
		log.logger.debug('Succesfully changed dir to /' + path)
	except Exception as e:
		log.logger.critical('Fail during directory change of /' + path )
		log.logger.debug(e)
		raise Exception

	try:
		size = ftp.size(tarfile)
		log.logger.debug('Succesfully checked size of ' + tarfile )
	except Exception as e:
		log.logger.critical('Fail checked size of ' + tarfile )
		log.logger.debug(e)
		raise Exception

	try:
		if size is None:
			return False
		else:
			return size
	except Exception as e:
		log.logger.critical('Could not return size of ' + tarfile )
		log.logger.debug(e)
		raise Exception

def downloadTar(ftp,path,tarfile):
	try:
		ftp.cwd('/'+path)
		log.logger.debug('Succesfully changed dir to /' + path)
	except Exception as e:
		log.logger.critical('Fail during directory change of /' + path )
		log.logger.debug(e)
		raise Exception

	try:
		ftp.retrbinary('RETR ' + tarfile,open(tarfile, 'wb').write)
		log.logger.debug('Succesfully downloaded ' + tarfile)
	except Exception as e:
		log.logger.critical('Fail retriefing file ' + tarfile )
		log.logger.debug(e)
		raise Exception

	ftp_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))

	q = 'UPDATE ftp_index SET downloaded = 1, download_date = "' +str(datetime.now()) +  '" WHERE tar = "'+tarfile+'"'

	try:
		db.update(ftp_db,q)
		log.logger.debug('Succesfully updated ftp_index database with downloaded file')
	except Exception as e:
		log.logger.critical('Could not update ftp_index database')
		log.logger.debug(e)

def extractTar(id):
	tar_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))
	q = 'SELECT tar,filename FROM tar_index WHERE id = ' + str(id)
	tar_info = {}
	try:
		tar_info = db.query(tar_db,q)
		print tar_info['tar']
		#tar_info = db.query(tar_db,q)[0]
	except Exception as e:
		log.logger.critical('Could not query tar_index table')
		log.logger.debug(e)
	if len(tar_info) == 0:
		log.logger.error('Could not find this id in tar_index. ID:' + str(id))
		raise Exception
	else:
		try:
			tar = tarfile.open(tar_info['tar'])
			log.logger.debug('Succesfully opened ' + tar_info['tar'])
		except Exception as e:
			log.logger.critical('Fail during opening ' + tar_info['tar'] )
			log.logger.debug(e)
			raise Exception
		try:
			tar.extract(tar_info['filename'])
			#tar.extract(14)
			log.logger.debug('Succesfully extracted ' + tar_info['filename'])
			return tar_info['filename']
		except Exception as e:
			log.logger.critical('Could not extact' + tar_info['filename'] + ' from ' + tar_info['tar'])
			log.logger.debug(e)

def indexTar(tarfilename):

	__MIME__ = ['tif','tiff','jpg','jpeg']

	tar_db = db.connect(config.get('db_host'),config.get('db_user'),config.get('db_password'),config.get('db_name'))

	try:
		tar = tarfile.open(tarfilename)
		log.logger.debug('Succesfully opened ' + tarfilename)
	except Exception as e:
		log.logger.critical('Fail during opening ' + tarfilename )
		log.logger.debug(e)
		raise Exception

	try:
		members = tar.getnames()
		log.logger.debug('Succesfully indexed tar file ' + tarfilename)
	except Exception as e:
		log.logger.critical('Fail during opening ' + tarfilename )
		log.logger.debug(e)
		raise Exception
	totaller = 0
	for i in range(len(members)):
		member = members[i]
		if member.split('/')[-1].split('.')[-1] in __MIME__:
			totaller = totaller + 1
			fullpath = member
			tar_index = i
			filename = member.split('/')[-1]
			name = filename[0:-len(filename.split('.')[-1])-1]
			t = tarfilename.split('.')[0].split('_')[-1]
			tar_date = datetime(int(t[:4]),int(t[4:6]),int(t[6:8]),int(t[8:10]),int(t[10:12]),int(t[12:]))
			index_date = datetime.now()
			if '-2013-' in name:
				proccessed_2013 = 0
			else:
				proccessed_2013 = 1

			q = 'INSERT INTO tar_index (tar,filename,tar_index,name,index_date,tar_date,processed_2013) VALUES (\
			"' + tarfilename + '",\
			"' + fullpath + '",\
			"' + str(tar_index) + '",\
			"' + name + '",\
			"' + str(index_date) + '",\
			"' + str(tar_date) + '",\
			"' + str(proccessed_2013) + '"\
			)'
			try:
				db.update(tar_db,q)
			except Exception as e:
				log.logger.critical('Could not add record ' + fullpath + ' from ' + tarfilename + ' this is a CRITICAL -> EXITING')
				exit(1)
		log.logger.info('Indexed ' + str(totaller) + ' files from tar: ' + tarfilename)
