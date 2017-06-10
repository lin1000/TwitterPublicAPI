# -*- coding: utf-8 -*-

import sys
import os
from os import listdir
from os.path import basename
import gzip
import json
import time
import csv
import format.tweet as tw

if len(sys.argv) <> 4:
   print("Error: twitter.py <Input GzDir Path> <Output Dir Path> <Num of Tweet in File>")
   """ 
   nohup python gnip2csv.py path_to_input_with_gzfiles/ path_to_output/ 100000000 > gnip2csv.log 2>&1 &
   """
   sys.exit()

gzDirPath = sys.argv[1]
if not os.path.isdir(gzDirPath):
    print("Error: "+gzDirPath+" is not a directory")
if not gzDirPath.endswith("/"):
    gzDirPath = gzDirPath + "/"
outDirPath = sys.argv[2]
if not os.path.isdir(outDirPath):
    print("Error: "+outDirPath+" is not a directory")
if not outDirPath.endswith("/"):
    outDirPath = outDirPath + "/"

loadTable_num = int(sys.argv[3])

tweetCnt = 0
tweetList = []
t = tw.FIELDS()
tweetList.append(t.header())

onlyfiles = [f for f in listdir(gzDirPath) if os.path.isfile(os.path.join(gzDirPath, f))]
for file_gz in onlyfiles:

    if(file_gz.endswith('.gz')):
        gzFilePath = gzDirPath + file_gz
        filename, file_extension = os.path.splitext(basename(file_gz))
        print gzFilePath

        with gzip.open(gzFilePath, "rb") as f:
            for line in f:

                gniptweet = json.loads(line)

                # Skip no tweet_id and handle
                if not gniptweet.has_key('id'):
                    continue
                if not gniptweet.has_key('actor') and gniptweet['actor'].has_key('preferredUsername'):
                    continue

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

                tweetList.append(tweet.toList())
                tweetCnt = tweetCnt + 1
                if tweetCnt % loadTable_num == 0:
                    tweetFile = os.path.join(outDirPath, "TWEET_" + str(int(tweetCnt / loadTable_num)) + ".csv")
                    with open(tweetFile, "wb") as f:
                        writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
                        writer.writerows(tweetList)
                    tweetList = []
                    t = tw.FIELDS()
                    tweetList.append(t.header())

if(len(tweetList) > 0):
    tweetFile = os.path.join(outDirPath, "DIM_TWEET_"+str(int(tweetCnt / loadTable_num)) + ".csv")
    with open(tweetFile, "wb") as f:
        writer = csv.writer(f, quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        writer.writerows(tweetList)
