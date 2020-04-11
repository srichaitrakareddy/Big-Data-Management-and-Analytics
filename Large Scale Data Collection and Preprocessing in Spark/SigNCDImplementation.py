import glob
import sys
import json
import zlib
import binascii
import pandas
from conllu import parse
import pymongo
from pymongo import MongoClient
import numpy as np


client = MongoClient('mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb', 27017)
db = client.spanish_news
print(db)
myTable = db.processed_data
rows = myTable.find()

information = {}
count = 0
clengtharrays = []
clengthids = []
processedarray = []
for row in rows:
    processed = row['description']
    author = row["authors"]
    pubdate = row["date_publish"]
    title = row['title']

    information[count]=str(title)+"||"+str(pubdate)+"||"+" ".join(author)
    sentences = parse(processed)
    finalstr = []
    indexes = []
    for sentence in sentences:
        for token in sentence:
            if token['upostag'] == 'PUNCT':
                indexes.append(token['id'] - 1)
                for sentence in sentences:
                    for token in sentence:
                        if token['id'] in indexes:
                            finalstr.append(token['form'])
    sigx = " ".join(finalstr)
    csigx = str(binascii.hexlify(zlib.compress(str.encode(sigx), 2)))
    clength = len(csigx)
    clengtharrays.append(clength)
    clengthids.append(count)
    processedarray.append(sigx)
    count = count + 1

sortedindices = np.argsort(np.array(clengtharrays))
sortedclengtharray = np.array(clengtharrays)[sortedindices]
sortedclengthids = np.array(clengthids)[sortedindices]
sortedprocessedarray = np.array(processedarray)[sortedindices]
print("length :"+str(len(sortedindices)))
duplicatecount = 0
print("Duplicates are :\n")
for i in range(0, len(sortedclengtharray)-1):
    ithlength = sortedclengtharray[i]
    if i==(len(sortedclengtharray)-1):
        break
    for j in range(i+1, len(sortedclengtharray)):
        if j==i or j==(len(sortedclengtharray)-1) :
            break
        else:
            jthlength = sortedclengtharray[j]
            if ithlength/jthlength < 0.9:
                continue
            sigxy = sortedprocessedarray[i]+" "+sortedprocessedarray[j]
            csigxy = str(binascii.hexlify(zlib.compress(str.encode(sigxy), 2)))
            lengthxy = len(csigxy)
            ncd = (lengthxy - min(ithlength, jthlength))/max(ithlength, jthlength)
            if ncd <= 0.1:
                duplicatecount = duplicatecount + 1
                print(information[sortedindices[i]]+ " and "+ information[sortedindices[j]]+"\n")
            else:
                continue
print("Total Number of Duplicate Documents are: "+ str(duplicatecount))