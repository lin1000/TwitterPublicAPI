import sys
from pyspark import SparkContext, SparkConf

dataFile =  "../python/05GNIPData/20160601-20170601_avgg5v796n_2016_06_01_00_00_activities.json.gz"

conf = SparkConf().setAppName("Read A sample json activities app")
sc = SparkContext(conf=conf)

dataRDD = sc.textFile(dataFile).cache()

print dataRDD.take(10)

numA = dataRDD.filter(lambda s: 'tony' in s).count()
numB = dataRDD.filter(lambda s: 'mary' in s).count()

print "Lines with tony : %s , lines with mary: %s" % (numA,numB)

sc.stop()
