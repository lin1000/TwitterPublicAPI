import pandas as pd
import csv
import operator
import random
import glob
from os.path import basename
from os.path import splitext 

filenames =  glob.glob("../output/collect-follower-day2/*.csv")

for filename in filenames:
    base = basename(filename)
    (fname,extname) = splitext(base)
    outputfilename = fname + ".sorted" + extname
    
    mydict = {}

    with open(filename, newline='') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in csvreader:
            if(len(row[0].split(","))==2 and len(row[2].split(","))==1):
                if(int(row[2])>=20):
                    handle = row[0].split(",")[1]
                    status_count = int(row[2])
                    mydict[handle]=status_count

    sorted_mydict_as_list = sorted(mydict.items(), key=lambda x: x[1],reverse=True)

    num_to_select = 3332  # set the number to select here.

    list_of_random_items = random.sample(sorted_mydict_as_list, num_to_select)

    sorted_list_of_random_items = sorted(list_of_random_items,key=lambda x: x[1], reverse=True)

    with open("01SamplingAccount/"+ outputfilename, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(sorted_list_of_random_items)
