import os
from insert_25k import insert_25_elastic

folders = []
for root, dirs, files in os.walk("/aps/aps_get/json"):
    folders.extend(dirs)
    break
print(len(folders))

for f in folders[:46]:
    myindex = "documents"
    #print(f)
    insert_25_elastic(f, myindex)

