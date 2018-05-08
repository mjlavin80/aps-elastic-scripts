
from multiprocessing import Pool

p = Pool(4)

#make local directories if they don't exist
def make_dir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)

#get all subdirs

import os, glob
srcdir = "xml"
dstdir = "/media/backup/aps/xml/"

srcfolders = []
for root, dirs, files in os.walk(srcdir):
	#make folder copies at the destination so they exist
	srcfolders.extend(dirs)
	break

for f in srcfolders[0]:
	newroot = dstdir + f
	#make_dir(newroot)
	dstpattern = "xml/" + f + "/*.xml"
	source_files = glob.glob(dstpattern)
	dest_files = [i.replace(srcdir+"/", dstdir) for i in source_files]
	

#for each directory, copyfile in parallel 
p.map(f, args=(srcfolders, dstfolders))
