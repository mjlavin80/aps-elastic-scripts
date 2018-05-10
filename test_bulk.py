import glob, json, os
from elasticsearch import Elasticsearch

with open("counts.csv") as c:
    table  = c.readlines()

rows = [i.replace("\n", "").split(",") for i in table]

ev = {}

for u,v in rows:
    ev[u] = v

def test_25(folder):
    try: 
        with open("%s.json" % folder) as c:
            myjson = c.readlines()
        print("found bulkfile, counting objects")
        expected = str(ev[folder])
        actual = str(len(myjson))
        print("%s: expected %s, found %s" % (folder, expected, actual))
        if expected != actual:
            os.remove("%s.json" % folder)
            print("Removed bad json")
    except:
        print("No file found for %s" % folder)


    
if __name__ == '__main__':
    folders = []
    for root, dirs, files in os.walk("/aps/aps_get/json"):
        folders.extend(dirs)
        break

    for e, f in enumerate(folders):
        test_25(f)
