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
	for i in range(len(url)):
		if temp in url[i]['committee']:
			return url[i]['id']
	return "invalid"

db = mysql.connector.connect(user = 'root', db = 'DDDB', password = '')
dd = db.cursor(buffered = True)

try:
	select_stmt = ("SELECT * FROM Committee")
	dd.execute(select_stmt)
	for x in xrange(0,30):
		temp = dd.fetchone()
		print temp
		id = find_committee(temp['committee'])
		if id is not "invalid":
			str = 'http://openstates.org/api/v1/committees/' + id + '/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce'
			url2 = urlopen(str)
			committee = json.loads(url2)
			for u in range(len(committee)):
				for m in range(len(committee[u]["members"])):
					print committee[u]["members"]["name"]

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()

