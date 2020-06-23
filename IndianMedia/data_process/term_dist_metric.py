import pandas as pd
import numpy as np
import json

import networkx as nx

import nltk
from sklearn.metrics.pairwise import euclidean_distances
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics import pairwise_distances

from IndianMedia import utils
from IndianMedia.utils import getLogging
from IndianMedia.constants import MongoConsts
from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.load_job.mongoToCSV import getDF

logging = getLogging()

class TermDistMetric:

    def __init__(self ,dbconn, term_dist_vector_collection,group_dist_by_term_collection):
        self.CHNL = "CHNL"
        self.TERM = "_TERM"
        self.DIST_MAT = "_DIST"

        self.DIST_MAT_WORD = "word"
        self.DIST_MAT_DIST = "dist"

        self.top_n_terms_for_distance =60
        self.dbconn = dbconn
        self.term_dist_vector_collection = term_dist_vector_collection
        self.group_dist_by_term_collection = group_dist_by_term_collection

    def get_df(self):
        df = getDF()
        df = df[~(df["Title"].isna() | df["Description"].isna())]
        df["text"] = df["Title"] + "\n" + df["Description"]
        return df

    def get_vectorized_texts(self,text):
        logging.info("Getting Vectorized Docs")

        tokenizer = nltk.RegexpTokenizer("[a-z]{2,}")
        cvect = CountVectorizer(ngram_range=(1,2)
                        , min_df = 0.001
                        ,stop_words="english"
                        ,tokenizer = tokenizer.tokenize)

        return pd.DataFrame(cvect.fit_transform(text).toarray() , columns=cvect.get_feature_names())

    def get_term_by_term_distance_per_group(self,vectorized_texts,corresponding_channels):
        logging.info("Getting Term By Term Distance Per Group")

        vectorized_texts[self.CHNL] = corresponding_channels

        def findTopTermsPerGroup(df):
            dfwo= df.drop(self.CHNL , axis=1)
            dists = pairwise_distances(dfwo.T , metric="cosine")
            dists = pd.DataFrame(dists , columns=dfwo.columns , index=dfwo.columns)
            #dists = dists.apply(lambda row: row[row.argsort()][:n],axis=1)
            return dists

        return vectorized_texts.groupby(self.CHNL).apply(lambda df : findTopTermsPerGroup(df) )

    def get_term_distance_vector(self,term,t_by_t_distance):
        termdists = t_by_t_distance[term][t_by_t_distance[term].argsort()][:self.top_n_terms_for_distance]
        termdists = termdists.reset_index(name=self.DIST_MAT_DIST)
        termdists.columns = [self.CHNL,self.DIST_MAT_WORD,self.DIST_MAT_DIST]
        termdists = termdists.pivot(index=self.CHNL,columns=self.DIST_MAT_WORD  , values=self.DIST_MAT_DIST)
        return termdists.fillna(0)

    def get_group_distance_from_term_distance_vector(self,termdists):
        dists = euclidean_distances(termdists)
        cdistdf = pd.DataFrame(dists, columns=termdists.index , index=termdists.index)
        #print(cdistdf)
        cdistdf = cdistdf.reset_index()
        cdistdf = cdistdf.melt(id_vars=self.CHNL , var_name="C2" , value_name=self.DIST_MAT_DIST)
        cdistdf = cdistdf[cdistdf["C2"] !="index"]
        cdistdf.columns = ["C1" , "C2" , self.DIST_MAT_DIST]
        return cdistdf

    def get_group_distance_by_term(self,term,t_by_t_distance):

        #dists = pairwise_distances(termdists,metric="cosine")
        termdists = self.get_term_distance_vector(term,t_by_t_distance)
        return self.get_group_distance_from_term_distance_vector(termdists)


    def save_term_distance_vector(self,term,termdists):
        #print(termdists)
        #termdists = termdists.reset_index()
        js = json.loads(termdists.to_json(orient="index"))
        jobj = {"_id" : term , self.TERM : term , self.DIST_MAT : js}
        col = self.dbconn.getCollection(self.term_dist_vector_collection)
        col.update_one({"_id" : term} , {"$set" : jobj} , True)

    def process_and_save_term(self,term,t_by_t_dist):
        termdist = self.get_term_distance_vector(term,t_by_t_dist)
        gdist = self.get_group_distance_from_term_distance_vector(termdist)

        self.save_term_distance_vector(term,termdist)
        self.save_group_distance_by_term(term,gdist)

    def _load_collection_by_term(self,collection,term,orient):
        js = self.dbconn\
                   .getCollection(collection)\
                   .find_one({self.TERM : term} , {"_id" : 0})
        return pd.read_json(json.dumps(js[self.DIST_MAT]), orient=orient)

    def load_term_distance_vector(self,term):
        df = self._load_collection_by_term(self.term_dist_vector_collection, term , "index")
        #df = df.set_index([self.CHNL , self.DIST_MAT_WORD])
        #print(df)
        return df

    def save_group_distance_by_term(self,term,group_dists):
        js = json.loads(group_dists.to_json(orient="records"))
        jobj = {"_id" : term , self.TERM : term , self.DIST_MAT : js}
        col = self.dbconn.getCollection(self.group_dist_by_term_collection)
        col.update_one({"_id" : term} , {"$set" : jobj} , True)

    def load_group_distance_by_term(self,term):
        df= self._load_collection_by_term(self.group_dist_by_term_collection, term , "records")
        #df.index = df.columns
        return df

    def run_job(self):
        logging.info("Running Job")
        df = self.get_df()
        vectorized_text  =self.get_vectorized_texts(df["text"])
        t_by_t_dist = self.get_term_by_term_distance_per_group(vectorized_text, df["Channel Id"])
        logging.info(f"Processing Per term - Total Terms = {t_by_t_dist.shape[1]}")
        t_by_t_dist.apply(lambda col: self.process_and_save_term(col.name, t_by_t_dist))



@utils.singleton
class TermDistMetricDataFrameService(TermDistMetric):

    def __init__(self):
        dbconn = DBConnection()
        super().__init__(dbconn
                        , MongoConsts.QD_TRBD_TERM_DIST_COLLECTION
                        , MongoConsts.QD_TRBD_GRP_DIST_COLLECTION)

    def term_dist_as_packed_circles(self,term):
        df = self.load_term_distance_vector(term)
        df = df.drop(term,axis=1)
        return self._term_dist_as_packed_circles(df)

    def _term_dist_as_packed_circles(self,term_dist_df):
        df = term_dist_df.reset_index()

        mdf = pd.melt(df , id_vars="index" , var_name="word" , value_name="dist")
        mdf.columns = ["chnl" , "word" , "dist"]
        G=nx.Graph()
        mdf.apply(lambda row : G.add_edge(row["chnl"] , row["word"],weight=row["dist"]),axis=1)

        A = nx.fruchterman_reingold_layout(G)
        pdf = pd.DataFrame(A).T

        pdf = pdf.reset_index()

        pdf.columns = ["label","x" , "y"]
        pdf1 = pd.merge(pdf,mdf,how="inner" , left_on="label" , right_on="word")
        pdf1  = pdf1.rename(columns={"x" : "x_word" , "y" : "y_word"})
        pdf2 = pd.merge(pdf,mdf,how="inner" , left_on="label" , right_on="chnl")
        pdf2  = pdf2.rename(columns={"x" : "x_chnl" , "y" : "y_chnl"})
        pdf = pd.merge(pdf1 , pdf2, on=["chnl" , "word"])
        #spdf = pd.merge(pdf,mdf,how="inner" , left_on="index" , right_on="index")

        pdf = pdf.drop(["dist_x" ,"dist_y" , "label_x" , "label_y" ] , axis=1)
        pdf[["chnl","word"]] = pdf[["chnl","word"]].astype("category")
        pdf[ ["x_word" , "y_word" , "x_chnl" , "y_chnl"]] = pdf[[ "x_word" , "y_word" , "x_chnl" , "y_chnl"]].astype("float32")
        return pdf

if __name__ == "__main__":
    dbconn = DBConnection()
    tdm = TermDistMetric(dbconn
                    , MongoConsts.QD_TRBD_TERM_DIST_COLLECTION
                    , MongoConsts.QD_TRBD_GRP_DIST_COLLECTION)
    tdm.run_job()
