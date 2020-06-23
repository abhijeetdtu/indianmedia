import os

from pymongo import MongoClient
from IndianMedia.constants import MongoConsts

def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance

@singleton
class DBConnection():

    def __init__(self):
        if "MONGO_DEST" in os.environ:
            """
            client = pymongo.MongoClient("mongodb+srv://dbAdmin:<password>@indianmedia-cluster-dtqoj.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
            db = client.test
            """
            db = os.environ["MONGO_DB_NAME"]
            dest = os.environ["MONGO_DEST"]
            user = os.environ["MONGO_USER"]
            pwd = os.environ["MONGO_PWD"]
            connString = f"mongodb+srv://{user}:{pwd}@{dest}/{db}?retryWrites=true&w=majority"
        else:
            connString = "192.168.99.100:27017"
        self.client = MongoClient(connString)
        self.indianMediaVideoCollection = self.client[MongoConsts.DB][MongoConsts.VIDEO_COLLECTION]
        self.wordDateCollection = self.client[MongoConsts.DB][MongoConsts.WORD_DATE_COLLECTION]
        self.wordCorrCollection = self.client[MongoConsts.DB][MongoConsts.WORD_CORR_COLLECTION]
        self.articleCollection =  self.client[MongoConsts.DB][MongoConsts.ARTICLE_COLLECTION]
        self.goodNewsCollection = self.client[MongoConsts.DB][MongoConsts.GOOD_NEWS_COLLECTION]

    def getRemoteConnectionString(self):
        if "MONGO_DEST" in os.environ:
            """
            client = pymongo.MongoClient("mongodb+srv://dbAdmin:<password>@indianmedia-cluster-dtqoj.gcp.mongodb.net/<dbname>?retryWrites=true&w=majority")
            db = client.test
            """
            db = os.environ["MONGO_DB_NAME"]
            dest = os.environ["MONGO_DEST"]
            user = os.environ["MONGO_USER"]
            pwd = os.environ["MONGO_PWD"]
            return f"mongodb+srv://{user}:{pwd}@{dest}/{db}?retryWrites=true&w=majority"
        else:
            raise Exception("Please set the environment variables")


    def getLocalConnectionString(self):
        return "192.168.99.100:27017"

    def getLocalClient(self):
        return MongoClient(self.getLocalConnectionString())

    def getRemoteClient(self):
        if hasattr(self,'rmClient'):
            return self.rmClient
        self.rmClient = MongoClient(self.getRemoteConnectionString())
        return self.rmClient

    def getRemoteCollection(self,mongoConstCollection):
        return self.getRemoteClient()[MongoConsts.DB][mongoConstCollection]

    def getCollection(self , mongoConstCollection):
        return self.client[MongoConsts.DB][mongoConstCollection]

    def dropCollection(self,collection):
        return self.client[MongoConsts.DB][collection].drop()
