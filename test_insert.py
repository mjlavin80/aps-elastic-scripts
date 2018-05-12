import os
from elasticsearch import Elasticsearch
import glob, json

es = Elasticsearch('localhost', timeout=60, max_retries=10, retry_on_timeout=True)

#get all folders in loop order
folders = []
for root, dirs, files in os.walk("/aps/aps_get/json"):
    folders.extend(dirs)
    break

data = []
#loop bulk folders
for a, f in enumerate(folders):
    print(" ".join([str(a+1), "out of", str(len(folders))]))
    with open("%s.json" % f) as c:
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
    data.append((f, len(missing_chunks), ",".join(missing_chunks)))

with open("elastic_counts.csv", "a") as e:
    for d in data:
        e.write("".join([str(d[0]), ',', str(d[1]), ',', str(d[2]), '\n']))
e.close()        
