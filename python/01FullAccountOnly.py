import pandas as pd
import csv
import operator
import random
import glob
from os.path import basename
from os.path import splitext 

filenames =  glob.glob("../output/*.csv")

for filename in filenames:
    base = basename(filename)
    (fname,extname) = splitext(base)
    outputfilename = fname + ".sorted" + extname
    
    mydict = {}
    print(fname)
    print(filename)
    cnt = 0
    with open(filename, newline='') as csvfile:
        csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        for row in csvreader:
           cnt += 1
           print(cnt) 
           if(len(row[0].split(","))==2 and len(row[2].split(","))==1):
                if(int(row[2])>=20):
                    handle = row[0].split(",")[1]
                    status_count = int(row[2])
                    mydict[handle]=status_count

    sorted_mydict_as_list = sorted(mydict.items(), key=lambda x: x[1],reverse=True)


    with open("01FullAccount/"+ outputfilename, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(sorted_mydict_as_list)
