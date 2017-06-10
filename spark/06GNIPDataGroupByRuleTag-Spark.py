import sys
from pyspark import SparkContext, SparkConf
import glob
from os.path import basename
from os.path import splitext
import json 
from pyspark.sql import SQLContext, Row

def test_if_dict_contain_rule_tag(mydict,rule_tag):
   #print "comparing mydict(%s) with %s" % (len(mydict),rule_tag)
   for tagline in mydict:
 	if("tag" in tagline and tagline['tag']==rule_tag) :
                #print "TAG FOUND"
		return True
   #print "TAG MISS"
   return False

def group_by_rule_tag(rule_tag_list=[]):
 
#    datafiles = "../python/05GNIPData/*.json.gz"
    datafiles = "../python/05GNIPData/20160601-20170601_avgg5v796n_2016_06_01_00_*_activities.json.gz"
    filenames =  glob.glob(datafiles)
    outputfilepath = "../spark/06GNIPDataGroupByRuleTag/"

    dataRDD = sc.textFile(datafiles).map(lambda x : json.loads(x))

    print "Loaded %s json records" % (dataRDD.count())
    
    dataRDD.persist()

    for rule_tag in rule_tag_list:

    	#print dataRDD.map(lambda d: d.keys()).collect()
   	#print dataRDD.flatMap(lambda d: d.keys()).distinct().collect()
   
    	#print dataRDD.map(lambda tweet: (len(tweet.keys()),1)).reduceByKey(lambda x, y: x + y).collect()
    	#print dataRDD.filter(lambda t: "body" in t).map(lambda t : (t['gnip']['matching_rules'][0]['tag'],t)).groupByKey().saveAsTextFile(outputfilepath)
    
    	#try to groupBy or groupByKey
    	#groupByRuleTag = dataRDD.filter(lambda t: "body" in t).map(lambda t : (t['gnip']['matching_rules'][0]['tag'],t)).groupBy(lambda (k,vs): k,1)
        groupByRuleTag = dataRDD.filter(lambda t: "body" in t).filter(lambda t: test_if_dict_contain_rule_tag(t['gnip']['matching_rules'],rule_tag))
        
 	#save filtered result into files
	#groupByRuleTag.saveAsTextFile(outputfilepath + "/" + rule_tag)

        #load as sparkSQL dataframe
	df = sqlContext.read.json(groupByRuleTag)
	df.registerTempTable(rule_tag)
	df_result = sqlContext.sql("SELECT _corrupt_record as spark_tweet FROM "+rule_tag)
	df_result.write.json(rule_tag+".json")

	#groupByRuleTag_list = [ t for t in groupByRuleTag.collect()]	
        #for tag in groupByRuleTag_list:
		#print "%s %s" % (tag[0],len(tag[1]))

    	#print dataRDD.filter(lambda t: "body" in t).map(lambda t : (t['gnip']['matching_rules'][0]['tag'],t)).groupByKey(3).map(lambda (k,vs): (k,len(vs))).saveAsTextFile(outputfilepath)

    #dataRDD.persist()

    #print dataRDD.count()
    #print dataRDD.take(1)

    #print dataRDD.map(lambda tweet: type(tweet)).collect()
  
    #dataRDD.take(10)

    #group_by_rule_tags_json = {u'group_by_rule_tags': [] } 

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
    sqlContext = SQLContext(sc)
    group_by_rule_tag(['modelpress','kenichiromogi','HikaruIjuin'])
    sc.stop()
