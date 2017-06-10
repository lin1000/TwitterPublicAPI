import sys
from pyspark import SparkContext, SparkConf
import glob
from os.path import basename
from os.path import splitext 

debug_limit = 1
debug_count = 0

def group_by_rule_tag(rule_tag_list=[]):
 
    global debug_limit
    global debug_count    

    datafiles = "../python/05GNIPData/*.json.gz"

    filenames =  glob.glob(datafiles)
    outputfilepath = "./06GNIPDataGroupByRuleTag/"

    dataRDD = sc.textFile(datafiles)

    print dataRDD.count()

    group_by_rule_tags_json = {u'group_by_rule_tags': [] } 

#    for filename in filenames:
#        base = basename(filename)
#        (fname,extname) = splitext(base)
#	print "Preparing to load %s" % (base)

#	dataRDD = sc.textFile(datafiles)
#	print dataRDD.take(1)
	#numA = dataRDD.filter(lambda s: 'tony' in s).count()
	#numB = dataRDD.filter(lambda s: 'mary' in s).count()
	#print "Lines with tony : %s , lines with mary: %s" % (numA,numB)

if __name__=='__main__':
    conf = SparkConf().setAppName("Read entire json activities app by pyspark")
    sc = SparkContext(conf=conf)
    group_by_rule_tag(['modelpress','kenichiromogi','HikaruIjuin'])
    sc.stop()
