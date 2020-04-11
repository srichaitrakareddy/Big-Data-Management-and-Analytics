# This file is part of UDPipe <http://github.com/ufal/udpipe/>.
#
# Copyright 2016 Institute of Formal and Applied Linguistics, Faculty of
# Mathematics and Physics, Charles University in Prague, Czech Republic.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import glob
import sys
import json
import zlib
import binascii
import pandas
from conllu import parse

import pymongo
from pymongo import MongoClient

from ufal.udpipe import Model, Pipeline, ProcessingError # pylint: disable=no-name-in-module

# In Python2, wrap sys.stdin and sys.stdout to work with unicode.
if sys.version_info[0] < 3:
    import codecs
    import locale
    encoding = locale.getpreferredencoding()
    sys.stdin = codecs.getreader(encoding)(sys.stdin)
    sys.stdout = codecs.getwriter(encoding)(sys.stdout)


client = MongoClient('mongodb://127.0.0.1:27017/?gssapiServiceName=mongodb', 27017)
db = client.spanish_news #We use our database now.
print(db)
myTable = db.processed_data
sys.stderr.write('Loading model: ')
model = Model.load('/home/kiranmai/Downloads/spanish-ancora-ud-2.3-181115.udpipe')
if not model:
    sys.stderr.write("Cannot load model from file '%s'\n" % '/home/kiranmai/output_udpipe.txt')
    sys.exit(1)
sys.stderr.write('done\n')

pipeline = Pipeline(model, 'tokenize', Pipeline.DEFAULT, Pipeline.DEFAULT, 'conllu')
sys.stderr.write('pipeline loading done\n')
error = ProcessingError()

sys.stderr.write('text loading:\n')

files = glob.glob("/home/kiranmai/Downloads/JSONFiles/crawled_json/*.json")
print("Number of files = ", str(len(files)))
for file in files:
    with open(file) as json_file:
        data = json.load(json_file)
        if(data['description'] != None):
            sys.stdout.write(data['description']+"\n")
            processed = pipeline.process(data['description'], error)
            processed_from_json = {
                "authors": data['authors'],
                "title": data['title'],
                "description": processed,
                "date_publish": data['date_publish']}
            insert_status = myTable.insert_one(processed_from_json)
            print("Insertion status is ", insert_status)
            print("Article inserted is ", insert_status.inserted_id)
