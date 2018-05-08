import glob, os

folders = []
for root, dirs, files in os.walk("/aps/aps_get/json"):
    folders.extend(dirs)
    break

lengths = []
print("globbing")
for folder in folders:
    myfiles = glob.glob("/aps/aps_get/json/%s/*.json" % folder)
    c = len(myfiles)
    lengths.append([folder, str(c)])
print("writing")
with open("counts.csv", "a") as f:
    for l in lengths:
        f.write("".join([l[0], ',', l[1], '\n']))
f.close()        
