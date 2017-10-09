from glob import iglob, glob
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

#SREEJA STHOTHRA BHASHYAM 25185345
#ISH PATEL 20175396
#JEREMY PARNELL 27005248

stemmer = SnowballStemmer('english')
client = MongoClient('localhost', 27017)
db = client['IC-23']
counter = 0;

path = "/Users/sreejasb/Documents/CS121/A4/WEBPAGES_CLEAN/*/*"

stopWords = (
    "a,able,about,across,after,all,almost,also,am,among,an,and,any,are,as,at,"
    "be,because,been,but,by,can,cannot,could,dear,did,do,does,either,else,"
    "ever,every,for,from,get,got,had,has,have,he,her,hers,him,his,how,however,"
    "i,if,in,into,is,it,its,just,least,let,like,likely,may,me,might,most,must,"
    "my,neither,no,nor,not,of,off,often,on,only,or,other,our,own,rather,said,"
    "say,says,she,should,since,so,some,than,that,the,their,them,then,there,"
    "these,they,this,tis,to,too,twas,us,wants,was,we,were,what,when,where,"
    "which,while,who,whom,why,will,with,would,yet,you,your"
).split(',')
stopwords = set()
for x in stopWords:
    stopwords.add(stemmer.stem(x))


def read_File(file, size = 5242880): 
    with open(file,'r') as file:
        while True:
            chunk = file.read(size)
            if not chunk:
                break
            yield chunk

def tokenize(word_list):
    soup = BeautifulSoup(word_list, "lxml")
    stemmedwords = []

    for x in re.sub('[\W]', ' ', soup.get_text()).lower().split():
        if stemmer.stem(x) not in stopwords:
            stemmedwords.append(str(stemmer.stem(x)))
    return stemmedwords

if __name__ == '__main__':
    start_time = time.time()
    result = db.indx.create_index([("term", pymongo.ASCENDING)], unique=True)

    for filename in sorted(glob(path)):
        counter+=1
        for x in read_File(filename):
            tokens = tokenize(x)
            for token in tokens:
                docID = [filename.split('/')[-2]+"/"+filename.split('/')[-1]]
                if db.indx.find({"term": token, "postingList.docID": docID}).limit(1).count()!=0:
                    #EXISTS AND INCREMENTING COUNT
                    result = db.indx.update( 
                    {"term": token, "postingList.docID":docID},
                    { '$inc' : { "postingList.$.count" : 1 }}, 
                    upsert=True)
                else:
                    #TOKEN DNE AND/OR POSTING DNE
                    db.indx.update(
                    { 'term': token },
                    { '$push': { 'postingList': {"docID": docID, "count": 1} }}, upsert=True)
        print(filename)
print "Done"
print("--- %s seconds ---" % (time.time() - start_time))


