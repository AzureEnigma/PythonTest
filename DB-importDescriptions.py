import mysql.connector
import re
import sys
import csv
import datetime

db = mysql.connector.connect(user = 'root', db = 'tester', password = '')
dd = db.cursor(buffered = True)

try:
	
except:
	db.rollback()
	exit()
	
db.close()