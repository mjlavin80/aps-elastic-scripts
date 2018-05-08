import glob
import zipfile
import os
import json
from shutil import copyfile
import xml.etree.ElementTree as ElementTree

xml_folder_1 = "/Volumes/Seagate Backup Plus Drive/1007091/1007091/XML/00010101_99991231/*.zip"
xml_folder_2 = "/Volumes/Seagate Backup Plus Drive/1006084/1006084/XML/00010101_99991231/*.zip"


#traverse folders, find all .zip files
zips1 = glob.glob(xml_folder_1)
zips2 = glob.glob(xml_folder_2)

zips = zips1 + zips2

dirs = ["zips", "xml", "json"]

#make local directories if they don't exist
def make_dir(directory):
	if not os.path.exists(directory):
		os.makedirs(directory)

class XmlListConfig(list):
    def __init__(self, aList):
        for element in aList:
            if element:
                # treat like dict
                if len(element) == 1 or element[0].tag != element[1].tag:
                    self.append(XmlDictConfig(element))
                # treat like list
                elif element[0].tag == element[1].tag:
                    self.append(XmlListConfig(element))
            elif element.text:
                text = element.text.strip()
                if text:
                    self.append(text)


class XmlDictConfig(dict):
    '''
    Example usage:

    >>> tree = ElementTree.parse('your_file.xml')
    >>> root = tree.getroot()
    >>> xmldict = XmlDictConfig(root)

    Or, if you want to use an XML string:

    >>> root = ElementTree.XML(xml_string)
    >>> xmldict = XmlDictConfig(root)

    And then use xmldict for what it is... a dict.
    '''
    def __init__(self, parent_element):
        if parent_element.items():
            self.update(dict(parent_element.items()))
        for element in parent_element:
            if element:
                # treat like dict - we assume that if the first two tags
                # in a series are different, then they are all different.
                if len(element) == 1 or element[0].tag != element[1].tag:
                    aDict = XmlDictConfig(element)
                # treat like list - we assume that if the first two tags
                # in a series are the same, then the rest are the same.
                else:
                    # here, we put the list in dictionary; the key is the
                    # tag name the list elements all share in common, and
                    # the value is the list itself 
                    aDict = {element[0].tag: XmlListConfig(element)}
                # if the tag has attributes, add those to the dict
                if element.items():
                    aDict.update(dict(element.items()))
                self.update({element.tag: aDict})
            # this assumes that if you've got an attribute in a tag,
            # you won't be having any text. This may or may not be a 
            # good idea -- time will tell. It works for the way we are
            # currently doing XML configuration files...
            elif element.items():
                self.update({element.tag: dict(element.items())})
            # finally, if there are no child tags and no attributes, extract
            # the text
            else:
                self.update({element.tag: element.text})

for d in dirs:
	make_dir(d)

#loop zips
for z in zips[:1]:
	#clone zip to local 
	myzip = z.split("/")[-1]
	dst = "zip/"+myzip
	
	copyfile(src, dst)

	#unzip local
	zip_ref = zipfile.ZipFile(dst, 'r')
	xmlsubdir = "xml/" + myzip.replace(".zip", "")

	make_dir(xmlsub)

	#unzip (each has 25,000 xml files)
	zip_ref.extractall(xmlsubdir)
	zip_ref.close()

	#loop xml files
	xml_files = glob.glob(xmlsubdir)
	#open xml, map to dict
	jsonsubdir = "json/" + xmlsubdir
	make_dir(jsonsubdir)
	for x in xml_files:
		#parse xml to dict
		tree = ElementTree.parse(x)
		root = tree.getroot()
		xmldict = XmlDictConfig(root)

		xmlfile = x.split("/")[-1]
		jsonfile = jsonsubdir + xmlfile.replace(".zip", ".json")

		#dump to json file
		with open(jsonfile, 'w') as outfile:
    		json.dump(xmldict, outfile)
		
		#clean up xml (keep zip)
		os.remove(x)

#test on 1000 xml files, 1 folder

#test glob
#(len(glob.glob(xml_1)), len(glob.glob(xml_2))) == (336, 129)

#test subdir files
#len(xmlfiles) == 25000