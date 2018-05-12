import os
from elasticsearch import Elasticsearch
import glob, json

es = Elasticsearch()

#get all folders in loop order
folders = []
for root, dirs, files in os.walk("/aps/aps_get/json"):
    folders.extend(dirs)
    break

#open bulk file for folder no. 262

with open("%s.json" % folders[263]) as c:
    myjson = c.readlines()

bulk_ids = []

for i in myjson:
    data_dict = json.loads(i)
    # get approx 25k ids using the data_dict['RecordID']
    bulk_ids.append(data_dict['RecordID'])

# loop and query by 0, 999, 1999, etc

missing_chunks = []
for z in bulk_ids:
    try:
        res = es.get(index="documents", doc_type='article', id=z)
    except:
        missing_chunks.append(z)

print(len(missing_chunks))        
