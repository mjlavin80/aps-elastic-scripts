from elasticsearch import Elasticsearch
import glob, json

es = Elasticsearch('localhost', timeout=60, max_retries=10, retry_on_timeout=True)

#glob and loop json files 

json_files = glob.glob("datafiles/*.json")

for a, f in enumerate(json_files):
    print(" ".join([str(a+1), "out of", str(len(folders))]))
    with open(f) as c:
        myjson = c.readlines()

    for i in myjson:
        data_dict = json.loads(i)
        # get approx 25k ids using the data_dict['RecordID']
        if data_dict['RecordID'] == "873884428":
            print("Found id 873884428")
            op_dict = {
            "index": {
                "_index": "resources",
                "_type": "_doc",
                "_id": data_dict['RecordID']
                }
            }
            bulk_data = []
            bulk_data.append(op_dict)
            bulk_data.append(data_dict)
            res = es.bulk(index = "resources", body = bulk_data, refresh = True)