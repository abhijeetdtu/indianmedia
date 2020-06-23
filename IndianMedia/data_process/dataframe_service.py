import pandas as pd
from IndianMedia import utils
from IndianMedia.constants import FlatFiles
from IndianMedia.mongointf.pymongoconn import DBConnection
from ast import literal_eval

#@utils.singleton
class DataFrameService:

    def __init__(self):
        self.dtype_lookup = {
            FlatFiles.WORD_BY_DATE : {"channel_id" : "category"
            , "date":"str"
            ,"variable":"category"
            , "value":"float16"
            , "date_month":"int8"
            , "date_week":"int8"}
        }

        self.cache = {}

        self.db = DBConnection()

    def getWordCorrelations(self,word):
        docs = self.db.wordCorrCollection.find_one({"word_id" : word})
        df = pd.DataFrame(docs["corrs"])
        df = df.fillna(0).reset_index()
        df = pd.melt(df ,id_vars="index", var_name="channel_id" , value_name="corr")
        return df

    def getWordDates(self,word):
        docs = self.db.wordDateCollection.find_one({"word_id" : word})
        #print(docs)
        if docs == None:
            return None
        ts = docs["ts"]
        # ds =[]
        # cs= []
        # vs =[]
        # for k,v in ts.items():
        #     d,c = literal_eval(k)
        #     ds.append(d)
        #     cs.append(c)
        #     vs.append(v)
        #
        # ds = pd.to_datetime(ds , format="%m_%d_%y")
        data = [(literal_eval(k)[0] , literal_eval(k)[1] , v) for k,v in ts.items()]

        #df = pd.DataFrame(data, index=pd.MultiIndex.from_tuples(tuples , names=["date" , "channel_id"]) , columns=["mean_prop"])
        df = pd.DataFrame(data , columns= ["date" , "channel_id" , "mean_prop"])
        df["date"] = pd.to_datetime(df["date"] , format="%m_%d_%y")
        df = df.sort_values("date")
        #df = df.sort_index(level=['date','channel_id'], ascending=[1, 0])
        #df.columns = ["date" , "channel_id" , "mean_prop"]

        df["mean_prop"] = df["mean_prop"].fillna(0)
        #df = df.reset_index()
        #print(df)
        #df["date"] = df["date"].astype("category")
        return df
