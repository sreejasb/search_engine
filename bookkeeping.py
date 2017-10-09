#http://l4wisdom.com/pymongo/mongoinsert.php
import json
from pymongo import MongoClient


client = MongoClient('localhost', 27017)
db = client['IC-23']


def bookkeeping(filename):
	bookkeeping = open(filename, 'r')
	parsed = json.loads(bookkeeping.read())

	for line in parsed:
		db.bookkeeping.insert({"docID": line, "URL": parsed[line]})


bookkeeping('WEBPAGES_CLEAN/bookkeeping.json')
