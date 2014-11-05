import mysql.connector
import re
import sys
import csv
import datetime

query_insert_lobbying_firm = "INSERT INTO LobbyingFirm (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s, %s);"

def format_date(str):
	mydate = datetime.datetime.strptime(str, "%m/%d/%Y").date()
	if datetime.date.mydate.year < 2000:
		return mydate.strftime("%Y-%d-%m")

def insert_lobbying_firm(cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT filer_id FROM LobbyingFirm WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbying_firm, (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr))

db = mysql.connector.connect(user = 'root', db = 'tester', password = '')
dd = db.cursor(buffered = True)

try:

	with open('CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter='\t')
		
		val = 0;

		for row in tsvin:
			form = row[3];
			sender_id = row[4]
			entity_cd = row[6]
			val = val + 1
			print val
			print form
			if form == "F601" and entity_cd == "FRM": 
				filer_naml = row[7]
				filer_id = row[5]
				rpt_date = row[12]
				if rpt_date:
					rpt_date = re.split(' ', rpt_date)[0]
					print rpt_date
					rpt_date = format_date(rpt_date)
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				print "naml = {0}, id = {1}, date = {2}, beg = {3}, end = {4}\n".format(filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
				insert_lobbying_firm(dd, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
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
			else:
				print 'Does not match any case!'
		db.commit()		
except:
	db.rollback()
	print 'error!', sys.exc_info()[0]
	exit()
	
db.close()
			
		