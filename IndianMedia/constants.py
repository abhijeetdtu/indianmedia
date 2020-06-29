

class FlatFiles:
    WORD_CORR = "word_correlations.csv"
    WORD_BY_DATE = "word_date.csv"

class Creds:

    KEY_FILE = "_keyfile"

class MongoConsts:
    DB = "indianmedia"
    VIDEO_COLLECTION = "videos"
    WORD_DATE_COLLECTION = "worddate"
    WORD_CORR_COLLECTION = "wordcor"
    ARTICLE_COLLECTION = "articles"
    GOOD_NEWS_COLLECTION = "goodnews"
    TD_COLLECTION = "topicdist"
    TD_SVD_COMP_COLLECTION = f"{TD_COLLECTION}.svdcomp"
    TD_SVD_DF_COLLECTION = f"{TD_COLLECTION}.svddf"
    TD_TOPICS_COLLECTION = f"{TD_COLLECTION}.topics"
    TD_DOC_TOPICS_COLLECTION = f"{TD_COLLECTION}.doc.topics"

    QD_TERM_BASED_DISTANCE_ROOT = "termbaseddist"
    QD_TRBD_TERM_DIST_COLLECTION = f"{QD_TERM_BASED_DISTANCE_ROOT}.termdist"
    QD_TRBD_GRP_DIST_COLLECTION = f"{QD_TERM_BASED_DISTANCE_ROOT}.groupdist"

    TERM_RANK_COLLECTION = "tr.termrank"
    
class Channels:
    WIRE= "UChWtJey46brNr7qHQpN6KLQ"
    THE_PRINT = "UCuyRsHZILrU7ZDIAbGASHdA"
    INDIA_TODAY = "UCYPvAwZP8pZhSMW8qs7cVCw"
    REPUBLIC_WORLD = "UCwqusr8YDwM-3mEYTDeJHzw"

    LOOKUP = {
        "WIRE" : WIRE,
        "PRINT": THE_PRINT,
        "INDIA_TODAY" : INDIA_TODAY,
        "REPUBLIC_WORLD" : REPUBLIC_WORLD,
        "TIMES_NOW" : "UC6RJ7-PaXg6TIH2BzZfTV7w",
        "DD_INDIA":"UCGDQNvybfDDeGTf4GtigXaw"
    }

    MAX_VIDEOS = 2000

    VID_TYPE_LIST = "youtube#videoListResponse"

    @staticmethod
    def reverseLookup(channelId):
        for k,v in Channels.LOOKUP.items():
            if v == channelId:
                return k
