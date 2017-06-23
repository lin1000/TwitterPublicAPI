import sys
from pyspark import SparkContext, SparkConf
import glob
from os.path import basename
from os.path import splitext
import json 
from pyspark.sql import SQLContext
import format.tweet as tw
import argparse

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

def check_single_influencee(twitterhandle):
 
    datafiles = "../python/05GNIPData/*.json.gz"
    outputfilepath = "../spark/06GNIPDataGroupByRuleTag/"

    dataRDD = sc.textFile(datafiles).map(lambda x : json.loads(x))

    print "Loaded %s json records" % (dataRDD.count())

    filteredResult = dataRDD.filter(lambda t: "body" in t).filter(lambda t: t['actor']['preferredUsername']==twitterhandle).map(lambda t: gnip_2_csv(t))
        
    #save filtered result into files
    filteredResult.saveAsTextFile(outputfilepath + "/" + twitterhandle)

    print filteredResult.count()

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--twitterhandle", dest='twitterhandle',default=None, help="twitterhandle to be extrcted")
    args = parser.parse_args()
    if(args.twitterhandle is None):
        print "twitter handle is not specified, do nothing and exit."
        parser.print_help()
        sys.exit(1)
    else:
        print "twitter handle is {}".format(args.twitterhandle)
 
    conf = SparkConf().setAppName("Read entire json activities and Check on a single influencee")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    check_single_influencee(args.twitterhandle)
    sc.stop()
