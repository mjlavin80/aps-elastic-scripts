

# DELETE _search
# {
#   "query": {
#     "match_all":{}
#   }
# }

def insert_25_elastic(folder, myindex):
    import glob, json
    from elasticsearch import Elasticsearch
    print("reading json")

    try: 
        with open("%s.json" % folder) as c:
            myjson = c.readlines()
    except:
        myfiles = glob.glob("/aps/aps_get/json/%s/*.json" % folder)

        myjson = []
        for f in myfiles:                                                
           with open(f) as j:
               jsontxt = j.read()
               myjson.append(jsontxt)

        #print("writing bulkfile")
        #with open("%s.json" % folder, "a") as c: 
        #   for m in myjson:
        #       c.write(m)
        #       c.write("\n")
        #c.close() 

    print("parsing json")

    bulk_data = []

    for i in myjson:
        data_dict = json.loads(i)
        op_dict = {
            "index": {
                "_index": myindex,
                "_type": "article",
                "_id": data_dict['RecordID']
            }
        }
        bulk_data.append(op_dict)
        bulk_data.append(data_dict)

    #print(bulk_data[0])

    es = Elasticsearch('localhost')

    if es.indices.exists(myindex):
        print("deleting '%s' index..." % (myindex))
        res = es.indices.delete(index = myindex )
        print(" response: '%s'" % (res))

    request_body = {
        "settings" : {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }

    res = es.indices.create(index=myindex, body = request_body)
    print(" response: '%s'" % (res))

    print("bulk indexing...")

    buffer = []
    for e, b in enumerate(bulk_data):
        if e % 1000 == 0 and e != 0:
            res = es.bulk(index = myindex, body = buffer, refresh = True)
            buffer = []
        buffer.append(b)
    res = es.bulk(index = myindex, body = buffer, refresh = True)

if __name__ == '__main__':
    #approx 25k files
    folder = "A1_20180208224825_00001"
    myindex = "documents"
    insert_25_elastic(folder, myindex)