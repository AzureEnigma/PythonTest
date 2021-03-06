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

for m in range(len(result)):
	print result[m]['committee']

def find_committee(temp):
	for i in range(0, len(result)):
		if temp in result[i]['committee']:
                        print i
			temp = result[i]['id']
			print temp
			return temp
	return "invalid"

def getPerson(cursor, filer_naml, filer_namf):
	pid = 0
	#print filer_naml
	#print filer_namf
	select_pid = "SELECT pid FROM Person WHERE last LIKE %(filer_naml)s AND first = %(filer_namf)s ORDER BY Person.pid;"
	cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	if cursor.rowcount > 0:
		pid = cursor.fetchone()[0]
	return pid
	
def find_district(cursor, pid, year, house):
	select_stmt = "SELECT district FROM Term where pid = %(pid)s AND house = %(house)s AND year = %(year)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year})
	if(cursor.rowcount > 0):
		temp = cursor.fetchone()
		return temp[0]
	return 999
	
def insert_serveson(cursor, pid, year, district, house, cid):
	select_stmt = "SELECT * FROM servesOn where pid = %(pid)s AND house = %(house)s AND year = %(year)s AND cid = %(cid)s AND district = %(district)s;"
	cursor.execute(select_stmt, {'pid':pid, 'house':house, 'year':year, 'cid':cid, 'district':district})
	if(cursor.rowcount == 0):
		#print 'insert'
		#print pid
		#print year
		#print district
		#print house
		#print cid
		cursor.execute(query_insert_serveson, (pid, year, district, house, cid))

db = mysql.connector.connect(user = 'root', db = 'DDDB', password = '')
dd = db.cursor(buffered = True)
de = db.cursor(buffered = True)

try:
	select_stmt = ("SELECT * FROM Committee")
	de.execute(select_stmt)
	for x in xrange(0,30):
		temp = de.fetchone()
		print "committee is {0}".format(temp[2])
		if temp:
                        print temp[2]
			id = find_committee(temp[2].strip(' \t\n\r'))
			cid = temp[0]
			house = temp[1]
			print house
			print id
			print "Committee {0}".format(id)
			if id is not "invalid":
				print 'valid'
				str = 'http://openstates.org/api/v1/committees/' + id + '/?apikey=d1a1fe2c7d53443284d0ea62d8ce7dce'
				url2 = urlopen(str).read()
				#print url2
				#print str
				committee = json.loads(url2)
				#print len(committee['members'])
				for m in range(0, len(committee['members']) ):
					name = committee['members'][m]['name'].split(' ')
					last = name[1]
					first = name[0]
					pid = getPerson(dd, last, first)
					if pid != 0:
						year = 2013
                                                print pid
						district = find_district(dd, pid, year, house)
						if(district != 999):
                                                        insert_serveson(dd, pid, year, district, house, cid)
	
	db.commit()

except:
	db.rollback()
	print 'error!', sys.exc_info()[0], sys.exc_info()[1]
	exit()
	
db.close()

