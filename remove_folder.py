import glob, json, os
def remove_folder(folder):    
    myfiles = glob.glob("/aps/aps_get/json/%s/*.json" % folder)
    for f in myfiles:                                                
        os.remove(f)
    os.rmdir("/aps/aps_get/json/%s" % folder)

