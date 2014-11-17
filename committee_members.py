import json
import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

#gets all of the committees in CA
url = urlopen('http://openstates.org/api/v1/committees/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce&state=ca').read()
result = json.loads(url)

def find_committee(temp):
	for i in range(len(result)):
		if temp in result[i]['committee']:
			return result[i]['id']
	return "invalid"

db = mysql.connector.connect(user = 'root', db = 'DDDB', password = '')
dd = db.cursor(buffered = True)

try:
	select_stmt = ("SELECT * FROM Committee")
	dd.execute(select_stmt)
	for x in xrange(0,30):
		temp = dd.fetchone()
		print temp[2]
		id = find_committee(temp[2])
		print "Committee {0}".format(temp[2])
		if id is not "invalid":
			str = 'http://openstates.org/api/v1/committees/' + id + '/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce'
			url2 = urlopen(str).read()
			committee = json.loads(url2)
			for m in range(len(committee['members'])):
				print committee['members'][m]['name']

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()

