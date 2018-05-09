import glob, json, os
from elasticsearch import Elasticsearch

def prep_25_elastic(folder):
    
    print("reading json")
    try: 
        with open("%s.json" % folder) as c:
            myjson = c.readlines()
        print("found bulkfile")
    except:
        myfiles = glob.glob("/aps/aps_get/json/%s/*.json" % folder)
        myjson = []
        for f in myfiles:                                                
           with open(f) as j:
               jsontxt = j.read()
               myjson.append(jsontxt)

        print("writing bulkfile")
        with open("%s.json" % folder, "a") as c: 
           for m in myjson:
               c.write(m)
               c.write("\n")
        c.close() 

    
if __name__ == '__main__':
    folders = []
    for root, dirs, files in os.walk("/aps/aps_get/json"):
        folders.extend(dirs)
        break

    for e, f in enumerate(folders[:46]):
        print(e, "out of ", len(folders))
        prep_25_elastic(folder)
