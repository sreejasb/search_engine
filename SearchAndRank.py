import os
import re
import json
from math import log
from collections import defaultdict
import pymongo
import json
from nltk.stem.snowball import SnowballStemmer
from pymongo import MongoClient
import time
from bs4 import BeautifulSoup
from math import log, sqrt
from operator import itemgetter
import numpy as np
from tkinter import *


###########
# GUI
########
# root = Tk()
# root.geometry( "640x480" );

# label_1 = Label(root, text="Query Input")
# entry_1 = Entry(root)

# label_1.pack(side=TOP)
# entry_1.pack(side=TOP)

# topFrame = Frame(root)
# topFrame.pack()

# bottomFrame = Frame(root)
# bottomFrame.pack(side=BOTTOM)

# searchButton = Button(bottomFrame, text='Enter', bg="blue")
# searchButton.pack(side=LEFT)

# exitButton = Button(bottomFrame, text='Exit', command=sysExit)
# exitButton.pack(side=RIGHT)

# listbox = Listbox(root)
# listbox.pack(fill=BOTH, expand=YES)

# root.mainloop()

# def sysExit():
#     sys.exit()
###########

stemmer = SnowballStemmer('english')
top = 20 #NUM OF RESULTS TO RETURN
client = MongoClient('localhost', 27017)
db = client['IC-23']

def cosineSimilarity(qterms):
	qtf = dict()
	qidf = dict()
	dtf = dict()
	N = db.indx.count()
	alldocs = set()
	for q in qterms:
		temp = []
		qtf[q] = qtf.get(q, 0)+1

		result = db.indx.find_one({"term": q})
		counts = sorted(result['postingList'], key=itemgetter('count'), reverse=True)

		#CALCULATE IDF FOR EACH QUERY TERM: USING INC.ITC
		documentFrequency = len(counts)
		qidf[q] = log(N/documentFrequency,10)

		#GATHER ALL RELEVANT DOCUMENTS AND TF
		for c in counts:
			alldocs.add(str(c['docID'][0]))
			temp.append(dict([(str(c['docID'][0]), 1+log(c['count'],10))]))

		dtf[q] = temp

	#CALCULATE TF.IDF OF QUERY AND MAGNITUDE OF QUERY VECTOR
	qtfidf = dict()
	magnitude = 0
	for x in qtf:
		qtfidf[x]=(1+log(qtf[x],10))*qidf[x]
		magnitude+=qtfidf[x]**2
	magnitude = sqrt(magnitude)

	#NORMALIZE QUERY VECTOR
	for x in qtfidf:
		qtfidf[x]=qtfidf[x]/magnitude

	#THIS IS OUR QUERY VECTOR
	qvector = qtfidf.values()

	#FOR ALL RELEVANT DOCS, CALCULATE MAGNITUDE OF DOC VECTOR
	docmagnitudes = dict()
	for s in sorted(list(alldocs)):
		for q in qterms:
			for docs in dtf[q]:
				if s in docs.keys():
					docmagnitudes[s] = docmagnitudes.get(s, 0) + (docs[s]**2)
	for m in docmagnitudes:
		docmagnitudes[m]=sqrt(docmagnitudes[m])

	#CREATE VECTORS FOR EACH DOCUMENT
	docvectors = dict()
	for s in sorted(list(alldocs)):
		tempvector = []
		for q in qterms:
			for docs in dtf[q]:
				if s in docs.keys():
					tempvector.append(docs[s]/docmagnitudes[s])
		docvectors[s] = tempvector

	#CALCULATE COSINE SIMILARITY
	cossimilarity = dict()
	for doc in docvectors:
		cossimilarity[doc]=sum(i[0] * i[1] for i in zip(qvector, docvectors[doc]))

	#SORT AND EXTRACT TOP RESULTS
	finalresults = sorted(cossimilarity.items(), key=itemgetter(1), reverse=True)
	docIDs = [str(x[0]) for x in finalresults[:top]]
	return docIDs

def oneWordQuery(qterms):
	#IF QUERY IS ONE WORD THEN ONLY THING THAT MATTERS IS TERM FREQUENCY OF DOCUMENTS IN POSTING LIST FOR THAT TERM
	result = db.indx.find_one({"term": qterms[0]})
	counts = sorted(result['postingList'], key=itemgetter('count'), reverse=True)
	docIDs = [str(x['docID'][0]) for x in counts[:top]]
	return docIDs

def giveResults(docIDs):
	#PRESENT URLS TO USER
	for doc in docIDs:
		result = db.bookkeeping.find_one({"docID": doc})
		print(result['URL'])



if __name__ == '__main__':
	try:
		querystring = raw_input("Search: ")

		qterms = [str(stemmer.stem(x)) for x in re.sub('[\W]', ' ', querystring).lower().split()]
		docIDs = []
		if len(qterms)>1:
			docIDs = cosineSimilarity(qterms)
		else:
			docIDs = oneWordQuery(qterms)
		giveResults(docIDs)
	except Exception:
		print('There was an error in your query')
