import json
import re
import sys
import mysql.connector
from pprint import pprint
from urllib import urlopen

query_insert_serveson = "INSERT INTO servesOn (pid, year, district, house, cid) VALUES(%s, %s, %s, %s, %s);"

#gets all of the committees in CA
url = urlopen('http://openstates.org/api/v1/committees/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce&state=ca').read()
result = json.loads(url)

def find_committee(temp):
	for i in range(len(result)):
		if temp in result[i]['committee']:
			return result[i]['id']
	return "invalid"

def getPerson(cursor, filer_naml, filer_namf):
	pid = 0
	print filer_naml
	print filer_namf
	select_pid = "SELECT pid FROM Person WHERE last = %(filer_naml)s AND first = %(filer_namf)s ORDER BY Person.pid;"
	cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	return pid
	
def find_district(cursor, pid, year, house):
	select_stmt = "SELECT district FROM Term where pid = %(pid)s AND house = %(house)s AND year = %(year)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year})
	if(cursor.rowcount > 0):
		return cursor.fetchone()
	return 999
	
def insert_serveson(cursor, pid, year, district, house, cid):
	select_stmt = "SELECT * FROM servesOn where pid = %(pid)s AND house = %(house)s AND year = %(year)s AND cid = %(cid)s AND district = %(district)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year, 'cid':cid, 'district':district})
	if(cursor.rowcount == 0):
		print pid
		print year
		print district
		print house
		cursor.execute(query_insert_serveson, (pid, year, district, house, cid))

db = mysql.connector.connect(user = 'root', db = 'DDDB', password = '')
dd = db.cursor(buffered = True)

try:
	select_stmt = ("SELECT * FROM Committee")
	dd.execute(select_stmt)
	for x in xrange(0,30):
		temp = dd.fetchone()
		print temp[2]
		id = find_committee(temp[2])
		cid = temp[0]
		house = temp[1]
		print "Committee {0}".format(temp[2])
		if id is not "invalid":
			str = 'http://openstates.org/api/v1/committees/' + id + '/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce'
			url2 = urlopen(str).read()
			committee = json.loads(url2)
			for m in range(len(committee['members'])):
				name = committee['members'][m]['name'].split(' ')
				last = name[1]
				print last
				first = name[0]
				pid = getPerson(dd, last, first)
				year = 2013
				district = find_district(dd, pid, year, house)
				insert_serveson(dd, pid, year, district, house, cid)

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()

