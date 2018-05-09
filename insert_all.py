import os
from insert_25k import insert_25_elastic

folders = []
for root, dirs, files in os.walk("/aps/aps_get/json"):
    folders.extend(dirs)
    break


for e, f in enumerate(folders):
    myindex = "documents"
    print(e, "out of ", len(folders))
    if e > 16:
        insert_25_elastic(f, myindex)

