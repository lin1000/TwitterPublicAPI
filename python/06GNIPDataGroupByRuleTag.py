import tarfile
import gzip
import glob
from os.path import basename
from os.path import splitext 
import logging
import json
import csv

log = None

def getStdErrLogger(name, level=logging.DEBUG, formatter=None):
    """ Set logging level for STDERR"""
    log = logging.getLogger(name)
    log.setLevel(level)

    if not formatter:
        formatter = logging.Formatter('%(asctime)s: %(levelname)s : %(message)s')

    consolelog = logging.StreamHandler()
    consolelog.setFormatter(formatter)
    log.addHandler(consolelog)

    return log


def extract_tweet_info(tweet,rule_tag_writers):
    
    if not isinstance(tweet, dict):
        raise TypeError('a dictionary type is required')

    if not 'gnip' in tweet:
        return {}

    out = []
    tweet_writers = {}

    """ rule tags
    'gnip': {'matching_rules': [{'tag': 'HikaruIjuin', 'id': 7988
                        882591621664552}]
    """

    if 'gnip' in tweet:
        if 'matching_rules' in tweet['gnip']:
            #log.info(tweet['gnip']['matching_rules'])
            for rule in tweet['gnip']['matching_rules']:
                log.info("rule tag : "+ rule['tag'])
                tweet_writers[rule['tag']] = rule_tag_writers[rule['tag']]['writer']


    log.debug("len(tweet_writers)"+str(len(tweet_writers)))

    if 'actor' in tweet:
        if 'preferredUsername' in tweet['actor']:
            out.append(tweet['actor']['preferredUsername'])
        else:
            return

    if 'body' in tweet:
        log.debug(tweet['body'].replace("\n"," "))
        out.append(tweet['body'].replace("\n"," "))
        
    else :
        return

    log.info(out)

    for writer_key in tweet_writers:
        tweet_writers[writer_key].writerow(out)

    

def group_by_rule_tag(rule_tag_list=[]):
    
    filenames =  glob.glob("./05GNIPData/*.json.gz")
    outputfilepath = "./06GNIPDataGroupByRuleTag/"

    rule_tag_writers = {}
    for rule_tag in rule_tag_list:
        outfile = open(outputfilepath + rule_tag + ".csv","w")
        writer = csv.writer(outfile, quotechar='"',quoting=csv.QUOTE_ALL)
        
        rule_tag_writers[rule_tag]={}
        rule_tag_writers[rule_tag]['outfile'] = outfile
        rule_tag_writers[rule_tag]['writer'] = writer

    log.info("{} files are go to be processed".format(len(filenames)))

    group_by_rule_tags_json = {u'group_by_rule_tags': [] } 

    for filename in filenames:
        base = basename(filename)
        (fname,extname) = splitext(base)
        log.info(filename)

        gzfiles = gzip.open(filename, "rb")
        for linenumber, line in enumerate(gzfiles):
            log.info(linenumber)
            try:
                tweet = json.loads(line)

                """ Sample Tweet
                    {'id': 'tag:search.twitter.com,2005:737795966648995840', 'objectType': 'activity', 'actor': {'objectType': 'person', 'id': 'i
                    d:twitter.com:189828775', 'link': 'http://www.twitter.com/sumire_mire', 'displayName': '竜の子ちゃん', 'postedTime': '2010-09-12T10:05:43.000Z', 'image': 'ht
                    tps://pbs.twimg.com/profile_images/577025960161083392/iR9QlPJj_normal.jpeg', 'summary': '海辺の気さくなバービーです。毎日のささいな日常を家人とタックを組んで
                    ブログに書いてます。よろしくね。', 'links': [{'href': 'http://plaza.rakuten.co.jp/tatsunokochan/', 'rel': 'me'}], 'friendsCount': 3463, 'followersCount': 272
                    9, 'listedCount': 4, 'statusesCount': 199283, 'twitterTimeZone': 'Tokyo', 'verified': False, 'utcOffset': '32400', 'preferredUsername': 'sumire_mire', 'langu
                    ages': ['ja'], 'location': {'objectType': 'place', 'displayName': '神奈川県'}, 'favoritesCount': 40}, 'verb': 'post', 'postedTime': '2016-06-01T00:00:40.000Z
                    ', 'generator': {'displayName': 'rakubo2', 'link': 'http://rakubots.kissa.jp/'}, 'provider': {'objectType': 'service', 'displayName': 'Twitter', 'link': 'htt
                    p://www.twitter.com'}, 'link': 'http://twitter.com/sumire_mire/statuses/737795966648995840', 'body': '「トリック・オア・トリート」、、、 https://t.co/8724GF5
                    nlR #r_blog', 'object': {'objectType': 'note', 'id': 'object:search.twitter.com,2005:737795966648995840', 'summary': '「トリック・オア・トリート」、、、 http
                    s://t.co/8724GF5nlR #r_blog', 'link': 'http://twitter.com/sumire_mire/statuses/737795966648995840', 'postedTime': '2016-06-01T00:00:40.000Z'}, 'favoritesCoun
                    t': 0, 'twitter_entities': {'hashtags': [{'text': 'r_blog', 'indices': [42, 49]}], 'user_mentions': [], 'symbols': [], 'urls': [{'url': 'https://t.co/8724GF5
                    nlR', 'expanded_url': 'http://plaza.rakuten.co.jp/tatsunokochan/diary/201509090003/?scid=we_blg_tw01', 'display_url': 'plaza.rakuten.co.jp/tatsunokochan/…',
                    'indices': [18, 41]}]}, 'twitter_filter_level': 'low', 'twitter_lang': 'ja', 'retweetCount': 0, 'gnip': {'matching_rules': [{'tag': 'HikaruIjuin', 'id': 7988
                    882591621664552}], 'urls': [{'url': 'https://t.co/8724GF5nlR', 'expanded_url': 'http://plaza.rakuten.co.jp/tatsunokochan/diary/201509090003/?scid=we_blg_tw01
                    ', 'expanded_status': 200}], 'profileLocations': [{'objectType': 'place', 'geo': {'type': 'point', 'coordinates': [135.183, 34.6913]}, 'address': {'country':
                    'Japan', 'countryCode': 'JP', 'locality': 'Kōbe-shi', 'region': 'Hyōgo'}, 'displayName': 'Kōbe-shi, Hyōgo, Japan'}]}}
                    2017-06-04 13:28:59,317: INFO : {'body': '「トリック・オア・トリート」、、、 https://t.co/8724GF5nlR #r_blog'}
                """
                
                tweet = extract_tweet_info(tweet,rule_tag_writers)
                #log.info(tweet)

            except Exception as e:
                raise e
            

    for rule_tag in rule_tag_list:
        rule_tag_writers[rule_tag]['outfile'].flush()
        rule_tag_writers[rule_tag]['outfile'].close()

if __name__=='__main__':
    log = getStdErrLogger('06GNIPDataGruopByRuleTag.log', level=logging.DEBUG)
    log.info('Setting Logging to DEBUG')
    group_by_rule_tag(['modelpress','kenichiromogi','HikaruIjuin'])