import pandas as pd
import csv
import glob
from os.path import basename
from os.path import splitext 

number_of_handle_in_a_rule = 20
max_number_of_rule = 1000

filenames =  glob.glob("./01SamplingAccount/*.csv")
outputfilename = "./02GenGNIPQuery/followers-gnip-query.txt"

outString = ""

#counters statistics
cur_number_of_handle = 0
cur_number_of_rules = 0
max_len_of_rules = 0


def _gen_rule_from_handle_list(handle_list,custom_tag,last_one=False):
    global outString
    global cur_number_of_rules
    cur_number_of_rules+=1
    print("cur_number_of_rules=" + str(cur_number_of_rules))

    if(cur_number_of_rules > max_number_of_rule):
        print("Rule " + str(cur_number_of_rules) + " is skipped due to exceed max_number_of_rules "+ str(max_number_of_rule) +" limit")
        return
    if(len(handle_list) > number_of_handle_in_a_rule):
        msg = "Number of handle in a rule has been exceeded." + "len(handle_list) =" + str(len(handle_list)) 
        raise Exception(msg)
    handle_list = list(map(lambda x: "from:{x}".format(x=x), handle_list))
    rule_str ="{\"value\":\"("
    rule_str += " OR ".join(handle_list)
    rule_str +=")\", \"tag\":\""+custom_tag+"\"}" 
    
    outString += rule_str
    if(last_one == False):
        outString += ","

def _appen_handle_on_handle_list(handle_list,handle,custom_tag):
    global cur_number_of_handle
    if(handle != ""):
        cur_number_of_handle+=1
        print("cur_number_of_handle=" + str(cur_number_of_handle))

    if(len(handle_list) == number_of_handle_in_a_rule or handle == ""):
        #the passed handle_list into _gen_rule_from_handle_list func is by value , so outer handle_list should clear itself
        _gen_rule_from_handle_list(handle_list,custom_tag)
        handle_list.clear()

    if(handle != ""):
        handle_list.append(handle)


with open(outputfilename, 'w') as outfile:
    outString += """rules = '[
"""

    handle_list = []

    for filename in filenames:
        base = basename(filename)
        (fname,extname) = splitext(base)
        major_handle = fname.split(".")[0]
        
        _appen_handle_on_handle_list(handle_list,major_handle,custom_tag=major_handle)        

        with open(filename, newline='') as csvfile: 
            csvreader = csv.reader(csvfile, delimiter=',')
            for row in csvreader:
                handle = row[0]
                _appen_handle_on_handle_list(handle_list,handle,custom_tag=major_handle)
        
        _appen_handle_on_handle_list(handle_list, handle="",custom_tag=major_handle)
        
    outString = outString[:-1]
    outString+= """
]'"""
    outfile.write(outString)
    outfile.flush()
    outfile.close()



