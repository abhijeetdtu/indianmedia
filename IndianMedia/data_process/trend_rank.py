import numpy as np
import pandas as pd
import json
from ast import literal_eval

from IndianMedia.utils import getLogging,singleton
from IndianMedia.constants import MongoConsts
from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.load_job.mongoToCSV import getWordDatesDF

logging = getLogging()

class TrendRank:

    class COLS:
        CHNL = "Channel Id"
        DATE = "date"

    def __init__(self ,dbconnection, term_rank_collection):
        self.dbConn = dbconnection
        self.term_rank_collection = term_rank_collection
        self.window_size = 14

    def get_term_rank_collection(self):
        return self.dbConn.getCollection(self.term_rank_collection)

    def get_df(self):
        return getWordDatesDF()

    def _get_series_ranked(self,series):
        temp = series.argsort()
        ranks = np.empty_like(temp)
        ranks[temp] = np.arange(len(series))
        idx_nan = np.isnan(series)
        ranks[idx_nan] = -1
        return pd.Series(ranks)

    def _get_all_series_ranked(self,idf):
        df = idf.apply(lambda row : self._get_series_ranked(row),axis=1)
        df.columns = idf.columns
        return df

    def get_rank_matrix(self , date_words):
        logging.info("Preparing Rank Matrix")

        date_words = date_words.replace(0,np.nan)

        date_words = date_words.groupby(date_words.index.get_level_values(1))\
                               .apply(lambda df: df.transform(lambda x : x.rolling(self.window_size,1).mean())
                                                    .rank(axis=1 , ascending=False)
                                    )
        return date_words
        # date_words = date_words.replace(0,np.nan)
        # return self._get_all_series_ranked(date_words)


    def save_rank_matrix(self,rank_matrix):
        logging.info("Saving Rank Matrix")
        #print(rank_matrix.reset_index())
        col = self.get_term_rank_collection()

        def save_term_rank_series(series):
            js = json.loads(series.to_json())
            col.update_one({"_id" : series.name } , {"$set" : js},True)

        return rank_matrix.apply(lambda col : save_term_rank_series(col))

    def load_rank_matrix_for_terms(self,terms):
        col = self.get_term_rank_collection()
        js = col.find({"_id" : {"$in":terms}})
        jsis = []

        for i,j in enumerate(js):
            name = j["_id"]
            del j["_id"]
            d =pd.read_json(json.dumps(j) , orient="index")
            d.index = pd.MultiIndex.from_tuples([literal_eval(i) for i in d.index])
            d.columns= [name]
            jsis.append(d)

        if len(jsis) < 1:
            return None
        df = pd.concat(jsis, axis=1)

        #df= self._get_all_series_ranked(df).reset_index().rename(columns={"level_0":TrendRank.COLS.DATE , "level_1":TrendRank.COLS.CHNL})
        df= df.reset_index().rename(columns={"level_0":TrendRank.COLS.DATE , "level_1":TrendRank.COLS.CHNL})
        df[TrendRank.COLS.DATE] = pd.to_datetime(df[TrendRank.COLS.DATE]  , format="%m_%d_%y")
        df = df.sort_values(TrendRank.COLS.DATE)
        return df

    def run_job(self):
        logging.info("Starting Job")
        df = self.get_df()
        rm = self.get_rank_matrix(df)
        #print(rm["coronavirus"][rm["coronavirus"] != -1])
        self.save_rank_matrix(rm)


@singleton
class TrendRankDataFrameService(TrendRank):

    def __init__(self):
        super().__init__(DBConnection() , MongoConsts.TERM_RANK_COLLECTION)

if __name__ == "__main__":
    TrendRank(DBConnection() , MongoConsts.TERM_RANK_COLLECTION).run_job()
