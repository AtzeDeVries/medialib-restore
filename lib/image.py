from PIL import Image,ImageChops
import operator,math, os
import log

def matchHistogram(image1,image2):
        try:
          im1 = Image.open(image1)
          log.logger.debug('Succesfully loaded image file ' + image1)
        except Exception as e:
          log.logger.critical('Fail open of image file ' + image1 )
          log.logger.debug(e)
          raise Exception

        try:
          im2 = Image.open(image2)
          log.logger.debug('Succesfully loaded image file ' + image2)
        except Exception as e:
          log.logger.critical('Fail open of image file ' + image2 )
          log.logger.debug(e)
          raise Exception

        try:
          im1 = im1.resize(im2.size, Image.NEAREST).histogram()
          log.logger.debug('Succesfully made histogram of ' + image1)
        except Exception as e:
          log.logger.critical('Fail with creating a histogram of ' + image2 )
          log.logger.debug(e)
          raise Exception

        try:
          im2 = im2.histogram()
          log.logger.debug('Succesfully made histogram of ' + image2)
        except Exception as e:
          log.logger.critical('Fail with creating a histogram of ' + image2 )
          log.logger.debug(e)

        try:
          result =  int(math.sqrt(reduce(operator.add,map(lambda a,b:(a-b)**2,im1,im2))/len(im1)))
          log.logger.debug('Succesfully returned histogram difference ' + image2 + ' with value: ' +  str(result))
          return result
        except Exception as e:
          log.logger.critical('Fail with creating a histogram of ' + image2 )
          log.logger.debug(e)
          raise Exception

def convertToJpeg(tiff,jpglocation,thumbnail=False):
  try:
    #cleanname = tiff.split('/')[-1][0:len(tiff.split('/')[-1].split('.')[-1])-1]
    cleanname = tiff.split('/')[-1][0:-len( tiff.split('/')[-1].split('.')[-1] ) -1]
    log.logger.debug('Succesfully parsed image file name')
  except Exception as e:
    log.logger.cirtical('Could not parse image file name of: ' + tiff)
    log.logger.debug(e)
    raise Exception

  if jpglocation[-1] != '/':
    jpglocation = jpglocation + '/'
  if not thumbnail:
    try:
      os.system('/usr/bin/convert "' + tiff + '" "' + jpglocation + cleanname + '.jpg"')
      log.logger.debug('Succesfully converted this image from original to jpg: ' + tiff + ' to ' + jpglocation + cleanname + '.jpg')
    except Exception as e:
      log.logger.critical('Could not convert this image to jpg: ' + tiff)
      log.logger.debug(e)
  else:
    try:
      os.system('/usr/bin/convert -quality 80 -resize 25% "' + tiff + '" "' + jpglocation + cleanname + '.jpg"')
      log.logger.debug('Succesfully converted this image from original to jpg: ' + tiff + ' to ' + jpglocation + cleanname + '.jpg')
    except Exception as e:
      log.logger.critical('Could not convert this image to jpg: ' + tiff)
      log.logger.debug(e)
