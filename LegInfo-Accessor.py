import mysql.connector
import re
import sys
import csv

query_insert_lobbying_firm = 'INSERT INTO LobbyingFirm(filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES (%s, %s, %s, %d, %d)'

def insert_lobbying_firm(cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr):
	cursor.execute(query_insert_lobbying_firm, (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr))

db = mysql.connector.connect('localhost', '', '', 'tester')
dd = db.cursor(buffered = True)

try:

	with open('CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter='\t')

		for row in tsvin:
			form = row[3];
			sender_id = row[4];
			entity_cd = row[6];
			if(form == "F601" && entity_cd == "FRM") 
				filer_naml = row[
			else if(form == "F604" && entity_cd == "LBY") 
				#do shit
			else if(form == "F604" && entity_cd == "LBY") 
				#do shit
			else if(form == "F602" && entity_cd == "LEM") 
				#do shit
			else if(form == "F603" && entity_cd == "LEM") 
				#do shit
			else if(form == "F606") 
				#ignore
			else if(form == "F607" && entity_cd == "LEM") 
				#ignore
				
except:
	conn.rollback()
	print 'error!', sys.exc_info()[0]
	exit()
	
db.close()
			
		