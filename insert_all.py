import os
from insert_25k import insert_25_elastic

folders = []
for root, dirs, files in os.walk("/aps/aps_get/json"):
    folders.extend(dirs)
    break

for e, f in enumerate(folders[400:]):
    myindex = "documents"
    print(e+1, "out of ", len(folders[400:]))
    insert_25_elastic(f, myindex)
