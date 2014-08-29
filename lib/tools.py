import random
import csv
import os


def checkit(size=10000,allowset=315):
  if random.randint(0,size) < allowset:
    return 1
  else:
    return 0


def match_analytics(ma):
  if len(ma) > 1:
    ma = sorted(ma)
    a = []
    a.append(ma[0])
    a.append(ma[1])
    a.append(round(float(ma[1]) / float(ma[0]),5))
    a.append(ma[1] - ma[0])
    return a
  elif len(ma) == 1:
    return [-1,-1,-1,-1]


def write_csv(f,row):
  header = ['qr','check_it','tiff','jpg','master','scan_date','street','box','analytics_database_id','best_match_value','second_best_match_value','match_factor','match_diff','correct','false_description']
  if not os.path.isfile(f):
    mf = open(f,'wb')
    wr = csv.write(mf,quoting=csv.QUOTE_ALL, dialect='excel')
    wr.writerow(header)
    wr.writerow(row)
    mf.close()
  else:
    mf = open(f,'ab')
    wr = csv.write(mf,quoting=csv.QUOTE_ALL, dialect='excel')
    wr.writerow(row)
    mf.close()
