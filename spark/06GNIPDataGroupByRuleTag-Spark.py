import sys
from pyspark import SparkContext, SparkConf
import glob
from os.path import basename
from os.path import splitext
import json 
from pyspark.sql import SQLContext
import format.tweet as tw

def gnip_2_csv(gniptweet):

    # Skip no tweet_id and handle
    if not gniptweet.has_key('id'):
        return
    if not gniptweet.has_key('actor') and gniptweet['actor'].has_key('preferredUsername'):
        return

    # Twitter
    tweet = tw.FIELDS()
    tweet.DOCUMENT_ID = gniptweet['id']
    ## Use Handle name as AUTHOR_ID
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('preferredUsername'):
        tweet.AUTHOR_ID = gniptweet['actor']['preferredUsername']
    else:
        tweet.AUTHOR_ID = ''
    tweet.SOURCE_NAME = 'Twitter'
    if gniptweet.has_key('link'):
        tweet.URL = gniptweet['link']
    else:
        tweet.URL = ''
    if (gniptweet.has_key('verb') and gniptweet['verb'].find('share') != -1) or (
            gniptweet.has_key('verb') and gniptweet['verb'].find('post') != -1 and gniptweet.has_key(
            'inReplyTo')):
        tweet.IS_COMMENT = 1
    else:
        tweet.IS_COMMENT = 0
    #gniptime = gniptweet['postedTime']
    #pytime = time.strptime(gniptime, '%Y-%m-%dT%H:%M:%S.000Z')
    tweet.POST_TIMESTAMP = gniptweet['postedTime']
    if gniptweet.has_key('body'):
        tweet.CONTENT = gniptweet['body']
    else:
        tweet.CONTENT = ''
    if gniptweet.has_key('twitter_lang'):
        tweet.LANGUAGE = gniptweet['twitter_lang']
    else:
        tweet.LANGUAGE = ''
    if gniptweet.has_key('location') and gniptweet['location'].has_key('displayName'):
        tweet.LOCATION_NAME = gniptweet['location']['displayName']
    else:
        tweet.LOCATION_NAME = ''
    if gniptweet.has_key('location') and gniptweet['location'].has_key('twitter_country_code'):
        tweet.COUNTRY_CODE = gniptweet['location']['twitter_country_code']
    else:
        tweet.COUNTRY_CODE = ''
    if gniptweet.has_key('geo') and gniptweet['geo'].has_key('coordinates'):
        tweet.GEO_COORDINATES = str(gniptweet['geo']['coordinates'][1]) + ',' + str(
            gniptweet['geo']['coordinates'][0])
    else:
        tweet.GEO_COORDINATES = ''
    if tweet.GEO_COORDINATES == '':
        if gniptweet.has_key('location'):
            if gniptweet['location'].has_key('geo'):
                if gniptweet['location']['geo'] is not None:
     		   if gniptweet['location']['geo'].has_key('coordinates') and gniptweet['location']['geo']['type'].find('Polygon') != -1:
       		    	xaix = (gniptweet['location']['geo']['coordinates'][0][0][0] +
                   	gniptweet['location']['geo']['coordinates'][0][1][0] +
              		gniptweet['location']['geo']['coordinates'][0][2][0] +
                    	gniptweet['location']['geo']['coordinates'][0][3][0]) / 4
            		yaix = (gniptweet['location']['geo']['coordinates'][0][0][1] +
                    	gniptweet['location']['geo']['coordinates'][0][1][1] +
                    	gniptweet['location']['geo']['coordinates'][0][2][1] +
                    	gniptweet['location']['geo']['coordinates'][0][3][1]) / 4
            		tweet.GEO_COORDINATES = str(xaix) + ',' + str(yaix)
    		   else:
           		tweet.GEO_COORDINATES = ''

    # Author
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('id'):
        tweet.AUTHOR_NAME = gniptweet['actor']['id']
    else:
        tweet.AUTHOR_NAME = ''
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('preferredUsername'):
        tweet.AUTHOR_ID = gniptweet['actor']['preferredUsername']
    else:
        tweet.AUTHOR_ID = ''
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('displayName'):
        tweet.AUTHOR_NICKNAME = gniptweet['actor']['displayName']
    else:
        tweet.AUTHOR_NICKNAME = ''
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('link'):
        tweet.AUTHOR_URL = gniptweet['actor']['link']
    else:
        tweet.AUTHOR_URL = ''
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('image'):
        tweet.AUTHOR_AVATAR_URL = gniptweet['actor']['image']
    else:
        tweet.AUTHOR_AVATAR_URL = ''
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('location') and gniptweet['actor'][
        'location'].has_key('displayName'):
        tweet.AUTHOR_LOCATION = gniptweet['actor']['location']['displayName']
    else:
        tweet.AUTHOR_LOCATION = ''
    tweet.SOURCE_NAME = 'Twitter'

    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('friendsCount'):
        tweet.FRIENDS_COUNT = gniptweet['actor']['friendsCount']
    else:
        tweet.FRIENDS_COUNT = ''
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('followersCount'):
        tweet.FOLLOWERS_COUNT = gniptweet['actor']['followersCount']
    else:
        tweet.FOLLOWERS_COUNT = ''
    if gniptweet['gnip'].has_key('klout_score'):
        tweet.KLOUT_SCORE = str(gniptweet['gnip']['klout_score'])
    else:
        tweet.KLOUT_SCORE = ''
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('favoritesCount'):
        tweet.FAVORITES_COUNT = gniptweet['actor']['favoritesCount']
    else:
        tweet.FAVORITES_COUNT = 0
    if gniptweet.has_key('actor') and gniptweet['actor'].has_key('listedCount'):
        tweet.LISTED_COUNT = gniptweet['actor']['listedCount']
    else:
        tweet.LISTED_COUNT = 0

    if gniptweet.has_key('inReplyTo') and gniptweet['inReplyTo'].has_key('link'):
        tweet.IN_REPLAT_TO_URL = gniptweet['inReplyTo']['link']
    else:
        tweet.IN_REPLAT_TO_URL = ''

    if gniptweet.has_key('twitter_entities') and gniptweet['twitter_entities'].has_key('hashtags'):
        cnt = 0
        for tag in gniptweet['twitter_entities']['hashtags']:
            tweet.HASH_TAGS.append(tag['text'])
            cnt = cnt + 1
            if cnt >= 10:
                break

    if gniptweet.has_key('twitter_entities') and gniptweet['twitter_entities'].has_key('urls'):
        cnt = 0
        for url in gniptweet['twitter_entities']['urls']:
            tweet.URL_MENTIONS.append(url['url'])
            cnt = cnt + 1
            if cnt >= 10:
                break

    if gniptweet.has_key('twitter_entities') and gniptweet['twitter_entities'].has_key('user_mentions'):
        cnt = 0
        for mention in gniptweet['twitter_entities']['user_mentions']:
            tweet.USER_MENTIONS.append(mention['screen_name'])
            cnt = cnt + 1
            if cnt >= 10:
                break

    return tweet.toCSVLine()    

def test_if_dict_contain_rule_tag(mydict,rule_tag):
   #print "comparing mydict(%s) with %s" % (len(mydict),rule_tag)
   for tagline in mydict:
	    if("tag" in tagline and tagline['tag']==rule_tag):
            #print "TAG FOUND"
		    return True
   #print "TAG MISS"
   return False

def group_by_rule_tag(rule_tag_list=[]):
 
    datafiles = "../python/05GNIPData/*.json.gz"
#    datafiles = "../python/05GNIPData/20160601-20170601_avgg5v796n_2016_06_01_00_*_activities.json.gz"
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
        groupByRuleTag = dataRDD.filter(lambda t: "body" in t).filter(lambda t: test_if_dict_contain_rule_tag(t['gnip']['matching_rules'],rule_tag)).map(lambda t: gnip_2_csv(t))
        
 	#save filtered result into files
	groupByRuleTag.coalesce(1).saveAsTextFile(outputfilepath + "/" + rule_tag)

    #load as sparkSQL dataframe
	#df = sqlContext.read.json(groupByRuleTag)
	#df.registerTempTable(rule_tag)
	#df_result = sqlContext.sql("SELECT _corrupt_record as spark_tweet FROM "+rule_tag)
	#df_result.write.json(rule_tag+".json")

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
