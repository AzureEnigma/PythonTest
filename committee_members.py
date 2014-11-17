import json
import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

def find_committee():
	print 'committee'

#gets all of the committees in CA
url = urlopen('http://openstates.org/api/v1/committees/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce&state=ca').read()
result = json.loads(url)

db = mysql.connector.connect(user = 'root', db = 'DDDB', password = '')
dd = db.cursor(buffered = True)

try:
	select_stmt = ("SELECT * FROM Committee")
	dd.execute(select_stmt)
	while(temp != "None"):
		temp = dd.fetchone()
		print temp;

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()

