import glob, json, os
from remove_folder import remove_folder

folders = []
for root, dirs, files in os.walk("/aps/aps_get/json"):
    folders.extend(dirs)
    break

for e, f in enumerate(folders):
    print(e+1, "out of ", len(folders))
    remove_folder(f)
