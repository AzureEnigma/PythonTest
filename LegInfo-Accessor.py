import mysql.connector
import re
import sys
import csv
import datetime

query_insert_lobbying_firm = "INSERT INTO LobbyingFirm (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s, %s);"
query_insert_lobbyist = "INSERT INTO Lobbyist (pid, filer_id) VALUES(%s, %s);"
query_insert_lobbyist_employer = "INSERT INTO LobbyistEmployer (filer_naml, filer_id, coalition) VALUES(%s, %s, %s);"
query_insert_lobbyist_employment = "INSERT INTO LobbyistEmployment (sender_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s);"
query_insert_lobbyist_direct_employment = "INSERT INTO LobbyistDirectEmployment (sender_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s);"
query_insert_lobbying_contracts = "INSERT INTO LobbyingContracts (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr) VALUES(%s, %s, %s, %s, %s);"


def format_date(str):
	check = str.split('/');
	mydate = datetime.datetime.strptime(str, "%m/%d/%Y").date()
	return mydate.strftime("%Y-%d-%m")

def insert_lobbying_firm(cursor, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT filer_id FROM LobbyingFirm WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbying_firm, (filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr))
		
def insert_lobbyist(cursor, val, filer_id, filer_naml, filernamf):
	select_stmt = "SELECT filer_id FROM Lobbyist WHERE filer_id = %(filer_id)s"
	select_pid = "SELECT pid FROM Person WHERE last = %(filer_naml)s AND first = %(filer_namf);"
	cursor.execute(select_pid, {'filer_naml':filer_naml, 'filer_namf':filer_namf})
	print cursor.fetchone()
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist, (val, filer_id))

def insert_lobbyist_employment(cursor, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT sender_id, rpt_date, ls_beg_yr FROM LobbyingEmployment WHERE sender_id = %(sender_id)s"
	cursor.execute(select_stmt, {'sender_id':sender_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_employment, (sender_id, rpt_date, ls_beg_yr, ls_end_yr))
		
def insert_lobbyist_direct_employment(cursor, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT sender_id, rpt_date, ls_beg_yr FROM LobbyingDirectEmployment WHERE sender_id = %(sender_id)s"
	cursor.execute(select_stmt, {'sender_id':sender_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_direct_employment, (sender_id, rpt_date, ls_beg_yr, ls_end_yr))
		
def insert_lobbyist_employer(cursor, filer_naml, filer_id, coalition):
	select_stmt = "SELECT filer_id FROM Lobbyist WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_employer, (fielr_naml, filer_id, coalition))
		
def insert_lobbying_contracts(cursor, filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr):
	select_stmt = "SELECT filer_id, sender_id, rpt_date FROM LobbyistContracts WHERE filer_id = %(filer_id)s"
	cursor.execute(select_stmt, {'filer_id':filer_id})
	if(cursor.rowcount == 0):
		cursor.execute(query_insert_lobbyist_contracts, (filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr))
				

db = mysql.connector.connect(user = 'root', db = 'tester', password = '')
dd = db.cursor(buffered = True)

try:

	with open('CVR_REGISTRATION_CD.TSV', 'rb') as tsvin:
		tsvin = csv.reader(tsvin, delimiter='\t')
		
		val = 0

		for row in tsvin:
			form = row[3]
			sender_id = row[4]
			entity_cd = row[6]
			val = val + 1
			print val
			print form
			if form == "F601" and entity_cd == "FRM": 
				filer_naml = row[7]
				filer_id = row[5]
				rpt_date = row[12]
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				print "naml = {0}, id = {1}, date = {2}, beg = {3}, end = {4}\n".format(filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
				insert_lobbying_firm(dd, filer_naml, filer_id, rpt_date, ls_beg_yr, ls_end_yr)
			elif form == "F604" and entity_cd == "LBY":
				filer_naml = row[7]
				filer_namf = row[8]
				filer_id = row[5]
				sender_id = row[4]
				rpt_date = row[12]
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				print "filer_id = {0}\n".format(filer_id)
				print "sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n".format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
				insert_lobbyist(dd, val, filer_id, filer_naml, filer_namf)
				print 'inserted lobbyist'
				insert_lobbyist_employment(dd, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
			elif form == "F604" and entity_cd == "LBY":
				filer_naml = row[7]
				filer_namf = row[8]
				filer_id = row[5]
				sender_id = row[4]
				rpt_date = row[12]
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				print "filer_id = {0}\n".format(filer_id)
				print "sender_id = {0}, rpt_date = {1}, ls_beg_yr = {2}, ls_end_yr = {3}\n".format(sender_id, rpt_date, ls_beg_yr, ls_end_yr)
				insert_lobbyist(dd, filer_id)
				insert_lobbyist_direct_employment(dd, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
			#extra attention needed for this one
			elif form == "F602" and entity_cd == "LEM":
				print 'case 4'
			elif form == "F603" and entity_cd == "LEM":
				filer_naml = row[7]
				filer_id = row[5]
				sender_id = row[4]
				rpt_date = row[12]
				ls_beg_yr = row[13]
				ls_end_yr = row[14]
				print filer_id
				coalition = (filer_id[:1] == 'C')
				print "filer_naml = {0}, filer_id = {1}, coalition = {2}\n".format(filer_naml, filer_id, coalition)
				insert_lobbyist_employer(dd, filer_naml, filer_id, coalition)
				insert_lobbyist_contracts(dd, filer_id, sender_id, rpt_date, ls_beg_yr, ls_end_yr)
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
			
		