class FIELDS:

    def __init__(self):
        self.TWEET_ID = ""
        self.SOURCE_NAME = ""
        self.POST_TIMESTAMP = ""
        self.URL = ""
        self.IS_COMMENT = ""
        self.CONTENT = ""
        self.LANGUAGE = ""
        self.LOCATION_NAME = ""
        self.COUNTRY_CODE = ""
        self.GEO_COORDINATES = ""
        self.AUTHOR_ID = ""
        self.AUTHOR_NAME = ""
        self.AUTHOR_NICKNAME = ""
        self.AUTHOR_URL = ""
        self.AUTHOR_AVATAR_URL = ""
        self.AUTHOR_LOCATION = ""
        self.FRIENDS_COUNT = ""
        self.FOLLOWERS_COUNT = ""
        self.KLOUT_SCORE = ""
        self.FAVORITES_COUNT = ""
        self.LISTED_COUNT = ""
        self.IN_REPLAT_TO_URL = ""
        self.HASH_TAGS = list()
        self.URL_MENTIONS = list()
        self.USER_MENTIONS = list()

    def prints(self):
        print("DOCUMENT_ID:" + self.DOCUMENT_ID)
        print("SOURCE_NAME:" + self.SOURCE_NAME)
        print("POST_TIMESTAMP:" + self.POST_TIMESTAMP)
        print("URL:" + self.URL)
        print("IS_COMMENT:" + str(self.IS_COMMENT))
        print("CONTENT:" + self.CONTENT)
        print("LANGUAGE:" + self.LANGUAGE)
        print("LOCATION_NAME:" + self.LOCATION_NAME)
        print("COUNTRY_CODE:" + self.COUNTRY_CODE)
        print("GEO_COORDINATES:" + self.GEO_COORDINATES)
        print("AUTHOR_ID:"+self.AUTHOR_ID)
        print("AUTHOR_NAME:" + self.AUTHOR_NAME)
        print("AUTHOR_NICKNAME:" + self.AUTHOR_NICKNAME)
        print("AUTHOR_URL:" + self.AUTHOR_URL)
        print("AUTHOR_AVATAR_URL:" + self.AUTHOR_AVATAR_URL)
        print("AUTHOR_LOCATION:" + self.AUTHOR_LOCATION)
        print("FRIENDS_COUNT:" + str(self.FRIENDS_COUNT))
        print("FOLLOWERS_COUNT:" + str(self.FOLLOWERS_COUNT))
        print("KLOUT_SCORE:" + str(self.KLOUT_SCORE))
        print("FAVORITES_COUNT:" + str(self.FAVORITES_COUNT))
        print("LISTED_COUNT:" + str(self.LISTED_COUNT))
        print("IN_REPLAT_TO_URL:" + str(self.IN_REPLAT_TO_URL))
        print("HASH_TAGS:" + ",".join(self.HASH_TAGS))
        print("URL_MENTIONS:" + ",".join(self.URL_MENTIONS))
        print("USER_MENTIONS:" + ",".join(self.USER_MENTIONS))

    def header(self):
        out = []
        out.append("DOCUMENT_ID".encode("utf-8"))
        out.append("SOURCE_NAME".encode("utf-8"))
        out.append("POST_TIMESTAMP".encode("utf-8"))
        out.append("URL".encode("utf-8"))
        out.append("IS_COMMENT".encode("utf-8"))
        out.append("CONTENT".encode("utf-8"))
        out.append("LANGUAGE".encode("utf-8"))
        out.append("LOCATION_NAME".encode("utf-8"))
        out.append("COUNTRY_CODE".encode("utf-8"))
        out.append("GEO_COORDINATES".encode("utf-8"))
        out.append("AUTHOR_ID".encode("utf-8"))
        out.append("AUTHOR_NAME".encode("utf-8"))
        out.append("AUTHOR_NICKNAME".encode("utf-8"))
        out.append("AUTHOR_URL".encode("utf-8"))
        out.append("AUTHOR_AVATAR_URL".encode("utf-8"))
        out.append("AUTHOR_LOCATION".encode("utf-8"))
        out.append("FRIENDS_COUNT".encode("utf-8"))
        out.append("FOLLOWERS_COUNT".encode("utf-8"))
        out.append("KLOUT_SCORE".encode("utf-8"))
        out.append("FAVORITES_COUNT".encode("utf-8"))
        out.append("LISTED_COUNT".encode("utf-8"))
        out.append("IN_REPLAT_TO_URL".encode("utf-8"))
        out.append("HASH_TAGS1".encode("utf-8"))
        out.append("HASH_TAGS2".encode("utf-8"))
        out.append("HASH_TAGS3".encode("utf-8"))
        out.append("HASH_TAGS4".encode("utf-8"))
        out.append("HASH_TAGS5".encode("utf-8"))
        out.append("HASH_TAGS6".encode("utf-8"))
        out.append("HASH_TAGS7".encode("utf-8"))
        out.append("HASH_TAGS8".encode("utf-8"))
        out.append("HASH_TAGS9".encode("utf-8"))
        out.append("HASH_TAGS10".encode("utf-8"))
        out.append("URL_MENTIONS1".encode("utf-8"))
        out.append("URL_MENTIONS2".encode("utf-8"))
        out.append("URL_MENTIONS3".encode("utf-8"))
        out.append("URL_MENTIONS4".encode("utf-8"))
        out.append("URL_MENTIONS5".encode("utf-8"))
        out.append("URL_MENTIONS6".encode("utf-8"))
        out.append("URL_MENTIONS7".encode("utf-8"))
        out.append("URL_MENTIONS8".encode("utf-8"))
        out.append("URL_MENTIONS9".encode("utf-8"))
        out.append("URL_MENTIONS10".encode("utf-8"))
        out.append("USER_MENTIONS1".encode("utf-8"))
        out.append("USER_MENTIONS2".encode("utf-8"))
        out.append("USER_MENTIONS3".encode("utf-8"))
        out.append("USER_MENTIONS4".encode("utf-8"))
        out.append("USER_MENTIONS5".encode("utf-8"))
        out.append("USER_MENTIONS6".encode("utf-8"))
        out.append("USER_MENTIONS7".encode("utf-8"))
        out.append("USER_MENTIONS8".encode("utf-8"))
        out.append("USER_MENTIONS9".encode("utf-8"))
        out.append("USER_MENTIONS10".encode("utf-8"))
        ss = []
        ss.append(out)
        return out

    def toList(self):
        out = []
        out.append(self.DOCUMENT_ID.encode("utf-8"))
        out.append(self.SOURCE_NAME.encode("utf-8"))
        out.append(self.POST_TIMESTAMP.encode("utf-8"))
        out.append(self.URL.encode("utf-8"))
        out.append(self.IS_COMMENT)
        out.append(self.CONTENT.encode("utf-8"))
        out.append(self.LANGUAGE.encode("utf-8"))
        out.append(self.LOCATION_NAME.encode("utf-8"))
        out.append(self.COUNTRY_CODE.encode("utf-8"))
        out.append(self.GEO_COORDINATES.encode("utf-8"))
        out.append(self.AUTHOR_ID.encode("utf-8"))
        out.append(self.AUTHOR_NAME.encode("utf-8"))
        out.append(self.AUTHOR_NICKNAME.encode("utf-8"))
        out.append(self.AUTHOR_URL.encode("utf-8"))
        out.append(self.AUTHOR_AVATAR_URL.encode("utf-8"))
        out.append(self.AUTHOR_LOCATION.encode("utf-8"))
        out.append(self.FRIENDS_COUNT)
        out.append(self.FOLLOWERS_COUNT)
        out.append(self.KLOUT_SCORE)
        out.append(self.FAVORITES_COUNT)
        out.append(self.LISTED_COUNT)
        out.append(self.IN_REPLAT_TO_URL.encode("utf-8"))
        for i in range(0, 10):
            if len(self.HASH_TAGS) > i:
                out.append(self.HASH_TAGS[i].encode("utf-8"))
            else:
                out.append("")
        for i in range(0, 10):
            if len(self.URL_MENTIONS) > i:
                out.append(self.URL_MENTIONS[i].encode("utf-8"))
            else:
                out.append("")
        for i in range(0, 10):
            if len(self.USER_MENTIONS) > i:
                out.append(self.USER_MENTIONS[i].encode("utf-8"))
            else:
                out.append("")
        return out

    def toCSVLine(self):
        out = []
        out.append(self.DOCUMENT_ID.encode("utf-8"))
        out.append(self.SOURCE_NAME.encode("utf-8"))
        out.append(self.POST_TIMESTAMP.encode("utf-8"))
        out.append(self.URL.encode("utf-8"))
        out.append(str(self.IS_COMMENT))
        out.append(self.CONTENT.encode("utf-8"))
        out.append(self.LANGUAGE.encode("utf-8"))
        out.append(self.LOCATION_NAME.encode("utf-8"))
        out.append(self.COUNTRY_CODE.encode("utf-8"))
        out.append(self.GEO_COORDINATES.encode("utf-8"))
        out.append(self.AUTHOR_ID.encode("utf-8"))
        out.append(self.AUTHOR_NAME.encode("utf-8"))
        out.append(self.AUTHOR_NICKNAME.encode("utf-8"))
        out.append(self.AUTHOR_URL.encode("utf-8"))
        out.append(self.AUTHOR_AVATAR_URL.encode("utf-8"))
        out.append(self.AUTHOR_LOCATION.encode("utf-8"))
        out.append(str(self.FRIENDS_COUNT))
        out.append(str(self.FOLLOWERS_COUNT))
        out.append(str(self.KLOUT_SCORE))
        out.append(str(self.FAVORITES_COUNT))
        out.append(str(self.LISTED_COUNT))
        out.append(self.IN_REPLAT_TO_URL.encode("utf-8"))
        for i in range(0, 10):
            if len(self.HASH_TAGS) > i:
                out.append(self.HASH_TAGS[i].encode("utf-8"))
            else:
                out.append("")
        for i in range(0, 10):
            if len(self.URL_MENTIONS) > i:
                out.append(self.URL_MENTIONS[i].encode("utf-8"))
            else:
                out.append("")
        for i in range(0, 10):
            if len(self.USER_MENTIONS) > i:
                out.append(self.USER_MENTIONS[i].encode("utf-8"))
            else:
                out.append("")        
        return ",".join(out)

