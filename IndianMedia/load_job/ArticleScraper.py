import requests
from bs4 import BeautifulSoup
import pathlib
import os
import pandas as pd

from IndianMedia.mongointf.pymongoconn import DBConnection
from multiprocessing.dummy import Pool as ThreadPool

import logging
logging.basicConfig(level=logging.INFO)

class ArticleScraper:

    FILE = os.path.join(pathlib.Path(__file__).resolve().parent.parent.absolute() , "data" , "mediaBias.csv")

    def __init__(self,dbConnection):
        self.setDBConnection(dbConnection)
        self.pool_size = 10
        self.RETRIEVAL_ERROR_MSG = "FAILED_TO_LOAD_PAGE"
        self.df = None

    class CSV_COLS:
        SOURCE = "Source"
        URL = "Url"
        BIAS = "Bias"
        QUALITY = "Quality"
        TEXT = "text"


    def setDBConnection(self , conn):
        self.dbConnection = conn

    def getDBConnection(self):
        return self.dbConnection

    def _initializeDF(self):
        df = self._readDF(ArticleScraper.FILE)
        self._setDF(df)

    def _readDF(self , fname):
        return pd.read_csv(fname , index_col=ArticleScraper.CSV_COLS.URL)

    def _setDF(self,df):
        self.df = df

    def getDF(self):
        if self.df is None:
            self._initializeDF()
        return self.df

    def getAllUrls(self):
        return self.getDF().index

    def getURLContent(self,url):
        try:
            resp = requests.get(url)
            return BeautifulSoup(resp.text).text
        except:
            return self.RETRIEVAL_ERROR_MSG

    def saveAllUrlContentInDB(self , df,limit=-1):
        logging.info("- Thread Initialized")
        pool = ThreadPool(self.pool_size)

        def _map(dfrow):
            _ ,dfrow = dfrow

            url = dfrow.name

            logging.info(f"-- fetching url {url}")
            text = self.getURLContent(url)
            source =  dfrow[ArticleScraper.CSV_COLS.SOURCE]
            bias =  dfrow[ArticleScraper.CSV_COLS.BIAS]
            quality =  dfrow[ArticleScraper.CSV_COLS.QUALITY]

            return self.saveArticleInDB(url , source,text,bias,quality)

        allRows = list(df.iterrows())[:limit]

        logging.info("- Getting Data")
        pool.map(_map ,allRows)

        # logging.info("- Saving To DB")
        # for u,c,s in contents:
        #     self.saveArticleInDB(u,s,c)

    def _bugFix(self):
        """we were not saving bias and quality earlier
        """
        metadf = self.getDF()
        datadf  =self.getAllArticlesFromDBAsDf()
        datadf = metadf.join(datadf , lsuffix="_left")
        #print(datadf)
        ac = self.getDBConnection().articleCollection

        def _update(r):
            ac.update_one({"url":r.name} , {"$set" : {"bias" : r.Bias , "quality" : r.Quality}})

        datadf.apply(_update,axis=1)

    def saveArticleInDB(self,url,source,text , bias,quality):
        obj = {"url" : url , "source" : source , 'text':text , "bias":bias , "quality" : quality}
        return self.getDBConnection().articleCollection.insert_one(obj)

    def deleteArticleInDB(self,url):
        return self.getDBConnection().articleCollection.delete_one({"url" : url})

    def getArticleFromDBByUrl(self,url):
        return self.getDBConnection().articleCollection.find_one({"url" :url})

    def getAllArticlesFromDB(self):
        retrival_query = {"text" : {"$not" : {"$regex" : ".*(403|Forbidden|FAILED_TO_LOAD_PAGE).*"}}}
        return self.getDBConnection().articleCollection.find(retrival_query)

    def getAllArticlesFromDBAsDf(self):
        articles = self.getAllArticlesFromDB()
        df = pd.DataFrame(articles)
        return df

    def writeAllArticlesToCSV(self,fpath):
        return self.writeArticlesToCSV(fpath ,self.getAllArticlesFromDB())

    def writeArticlesToCSV(self ,fpath, articles):
        df= pd.DataFrame(articles)
        print(df.shape)
        df.to_csv(fpath ,index=False)

    def runJob(self ,fpath="./test.csv", limit=-1):
        df = self.getDF()
        self.saveAllUrlContentInDB(df , limit)
        self.writeAllArticlesToCSV(fpath)

if __name__ == "__main__":
    dbC = DBConnection()
    ArticleScraper(dbC).writeAllArticlesToCSV("./test.csv")
