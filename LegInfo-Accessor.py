import mysql.connector
import re
import sys
import csv

query_insert_lobbying_firm = 'INSERT INTO LobbyingFirm(filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES (%s, %s, %s, %d, %d)'

def insert_lobbying_firm(cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr):
	cursor.execute(query_insert_lobbying_firm, (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr))

db = mysql.connector.connect(user = 'root', db = 'tester', password = '')
dd = db.cursor(buffered = True)

try:

	with open('CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter='\t')

		for row in tsvin:
			form = row[3];
			sender_id = row[4];
			entity_cd = row[6];
			print form;
			if form == "F601" and entity_cd == "FRM": 
				print 'case 1'
			elif form == "F604" and entity_cd == "LBY":
				print 'case 2'
			elif form == "F604" and entity_cd == "LBY":
				print 'case 3'
			elif form == "F602" and entity_cd == "LEM":
				print 'case 4'
			elif form == "F603" and entity_cd == "LEM":
				print 'case 5'
			elif form == "F606":
				print 'case 6'
			elif form == "F607" and entity_cd == "LEM":
				print 'case 7'
				
except:
	conn.rollback()
	print 'error!', sys.exc_info()[0]
	exit()
	
db.close()
			
		