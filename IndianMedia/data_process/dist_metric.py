import pandas as pd
import numpy as np
import json
import logging


from statsmodels.stats.multicomp import pairwise_tukeyhsd
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk import RegexpTokenizer

from IndianMedia import utils
from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.load_job.mongoToCSV import getDF
from IndianMedia.constants import MongoConsts


class DistMetricCalculator:

    class DataFrameCols:
        CHANNEL = "Channel Id"
        TEXT = "text"
        TITLE = "Title"
        DESC = "Description"
        URL = "Url"
        
        TOPIC = "_TOPIC"
        TOPIC_TERM = "term"
        TOPIC_VAL = "value"
        TOPIC_SIGMA = "_SIGMA"

        class DistDF:
            G1 = "g1"
            G2 = "g2"
            MEAN_DIFF = "meandiff"
            PVAL = "p-adj"
            DIFF_LOWER = "diff_lower"
            DIFF_UPPER = "diff_upper"
            REJECT = "reject"
            SIGMA  = "_SIGMA"
            DIST = "dist"

            ALL_COLS = [G1 , G2, MEAN_DIFF , PVAL , DIFF_LOWER,DIFF_UPPER,REJECT]


    def __init__(self ,dbConnection, svd_comp_collection , svd_df_collection , topics_collection  ,doc_topics_collection):
        self.stop_words_name = "english"
        self.max_num_topics = 400
        self.max_topic_terms = 5
        self.max_topics_per_doc = 10
        self.dbConnection = dbConnection
        self.svd_comp_collection = svd_comp_collection
        self.svd_df_collection = svd_df_collection
        self.topics_collection = topics_collection
        self.doc_topics_collection = doc_topics_collection

        self.orient = "records"
        self.topic_prefix = "topic"

    def getDBConnection(self):
        return self.dbConnection

    def get_tokenizer(self):
        return RegexpTokenizer("[a-z]{2,}")

    def get_df(self):
        df = getDF()
        df[DistMetricCalculator.DataFrameCols.TEXT] = df[DistMetricCalculator.DataFrameCols.TITLE] + df[DistMetricCalculator.DataFrameCols.DESC]
        df = df.drop_duplicates()
        df = df[~df[DistMetricCalculator.DataFrameCols.TEXT].isna()]
        return df

    def _keepInformative(self,df,min_df , max_df):
        tokenizer = self.get_tokenizer()
        tfvect = TfidfVectorizer(ngram_range=(1,1)
                            , min_df = min_df
                            , max_df = max_df
                            ,tokenizer = tokenizer.tokenize
                            , stop_words=self.stop_words_name)
        tfvect.fit(df)
        return tfvect.get_feature_names()

    def get_vocab(self , df , min_df , max_df):
        vocabsWithinFilter = df.groupby(DistMetricCalculator.DataFrameCols.CHANNEL).apply(lambda df : self._keepInformative(df[DistMetricCalculator.DataFrameCols.TEXT] , min_df,max_df))
        vocab = set(vocabsWithinFilter[0])
        for v in vocabsWithinFilter[1:]:
            vocab = vocab.intersection(v)
        return vocab

    def _tokenize_vocab_only(self ,tokenizer , vocab, sent):
        return [t for t in tokenizer.tokenize(sent) if t in vocab]

    def get_tfidf_transformed(self,df):
        logging.info("Preparing TFIDF")
        tokenizer = self.get_tokenizer()
        vocab = self.get_vocab(df , 2 , 0.3)
        tfvect = TfidfVectorizer(ngram_range=(1,1)
                        , min_df=0.005
                        , tokenizer = lambda sent: self._tokenize_vocab_only(tokenizer,vocab,sent)
                        , stop_words=self.stop_words_name)
        tfdf = tfvect.fit_transform(df[DistMetricCalculator.DataFrameCols.TEXT] )
        tfdf = pd.DataFrame(tfdf.toarray() , columns=tfvect.get_feature_names())
        return tfdf

    def get_svd_transformed(self,df, tfdf , n_comp):
        logging.info("Preparing SVD Transformed")
        svd = TruncatedSVD(n_comp)
        svddf = svd.fit_transform(tfdf)
        svddf = pd.DataFrame(svddf, columns=[f"{self.topic_prefix}{i}" for i in range(svd.n_components)])
        CHNL = DistMetricCalculator.DataFrameCols.CHANNEL
        svddf[CHNL] = df[CHNL]
        svddf = svddf[~svddf[CHNL].isna()]
        return svddf,svd

    def get_top_topics_per_doc(self,svddf,sigma_vals,docs,topn=10):
        logging.info("Preparing Top Topics Per Doc")
        chnnls= svddf[ DistMetricCalculator.DataFrameCols.CHANNEL]
        svddf =  svddf.drop([ DistMetricCalculator.DataFrameCols.CHANNEL] , axis=1).apply(lambda r: r*sigma_vals,axis=1)
        svddf[ DistMetricCalculator.DataFrameCols.CHANNEL] = chnnls

        topicdf= svddf.drop([ DistMetricCalculator.DataFrameCols.CHANNEL] , axis=1)\
                              .apply(lambda row: svddf.columns[np.argsort(row)][-topn:].values,axis=1)\
                              .apply(lambda s : pd.Series(s))
        topic_val_df= svddf.drop([ DistMetricCalculator.DataFrameCols.CHANNEL] , axis=1)\
                              .apply(lambda row: np.sort(row)[-topn:],axis=1)\
                              .apply(lambda s : pd.Series(s))
        df = topicdf.join(topic_val_df, lsuffix="l", how="inner")
        #docs.name= "doc"

        df = df.join(docs,how="inner")
        df[DistMetricCalculator.DataFrameCols.CHANNEL] = svddf[DistMetricCalculator.DataFrameCols.CHANNEL]

        return df

    def save_top_topics_per_doc(self,doc_topic_df,topn):
        logging.info("Saving Top Topics Per Doc")
        coll = self.dbConnection.getCollection(self.doc_topics_collection)

        def save_row(r,topn):
            s = json.loads(pd.Series(r[topn:-2].values, index=r[:topn]).to_json(orient="index"))
            s["doc"] = r[-2]
            s[DistMetricCalculator.DataFrameCols.CHANNEL] = r[-1]
            s["_id"] = utils.get_string_hash(s["doc"])
            return coll.update_one({"_id":s["_id"]} , {"$set" : s} , True)

        return doc_topic_df.apply(lambda r : save_row(r,topn) , axis=1)

    def get_topics(self,svd , tfdf):
        topics = np.apply_along_axis(lambda d : tfdf.columns[d] ,1, np.argsort(svd.components_)[: , -self.max_topic_terms:])
        topic_term_weights = np.sort(svd.components_)[: , -self.max_topic_terms:]
        return topics,topic_term_weights


    def topic_to_df(self,topic):
        df = pd.DataFrame(topic , index=[0])
        df = df.reset_index()
        df= pd.melt(df , id_vars="index"
                          , var_name=DistMetricCalculator.DataFrameCols.TOPIC_TERM
                          , value_name=DistMetricCalculator.DataFrameCols.TOPIC_VAL)
        df= df.drop(["index"] , axis=1)

        df = df[~df[DistMetricCalculator.DataFrameCols.TOPIC_TERM].isin(
            [DistMetricCalculator.DataFrameCols.TOPIC,
            DistMetricCalculator.DataFrameCols.DistDF.SIGMA,
            ])]

        df[DistMetricCalculator.DataFrameCols.TOPIC_VAL] = df[DistMetricCalculator.DataFrameCols.TOPIC_VAL].astype("float64")
        df[DistMetricCalculator.DataFrameCols.DistDF.SIGMA] = topic[DistMetricCalculator.DataFrameCols.DistDF.SIGMA]

        return df

    def load_topics_with_term_from_db(self,topic_term):
        coll = self.dbConnection.getCollection(self.topics_collection)
        return list(coll.find({topic_term : {"$exists" : "true"}} , {"_id" : 0}).sort([(topic_term , -1)]))

    def save_topic_to_db(self, topic_row , cutoff , sigma,collection):
        s = pd.Series(data=topic_row[cutoff:].values , index=topic_row[:cutoff].values)
        topicjs = json.loads(s.to_json(orient="index"))
        topicjs[DistMetricCalculator.DataFrameCols.TOPIC] = topic_row.name
        topicjs[DistMetricCalculator.DataFrameCols.DistDF.SIGMA] = sigma
        #topicjs[DistMetricCalculator.DataFrameCols.TOPIC_TERMS] = list(topic_row[:cutoff].values)
        return collection.insert_one(topicjs)

    def save_topics_to_db(self,topics,topic_term_weights,singular_values):
        logging.info("-- Saving Topics to DB")

        coll = self.dbConnection.getCollection(self.topics_collection)
        idx = [f"{self.topic_prefix}{i}" for i in range(len(topics))]
        topicDf = pd.DataFrame(topics , index=idx)
        weightDf = pd.DataFrame(topic_term_weights , index=idx)
        fdf = topicDf.join(weightDf ,how="inner" , lsuffix="_l")
        return fdf.apply(lambda r :self.save_topic_to_db(r,topicDf.shape[1] ,singular_values[int(r.name.replace(self.topic_prefix,""))], coll) , 1)
        # dfs = []
        # for row in fdf.iterrows():
        #     df = self._topic_to_df(row)
        #   dfs.append(performTukey(svddf[col]))


    def save_svd_components_to_db(self,components,tfdf):
        logging.info("Saving SVD Components to DB")
        coll = self.dbConnection.getCollection(self.svd_comp_collection)
        compdf = pd.DataFrame(components, columns=tfdf.columns , index=[f"{self.topic_prefix}{i}" for i in range(components.shape[0])])
        compdfjs = json.loads(compdf.to_json(orient=self.orient))
        return coll.insert_many(compdfjs)

    def _load_from_collection(self , collection):
        coll = self.dbConnection.getCollection(collection)
        docs = coll.find()
        return pd.DataFrame(docs).drop(["_id"] , axis=1)

    def load_svd_components_from_db(self):
        return self._load_from_collection(self.svd_comp_collection)

    def save_svd_transformed_to_db(self,svddf):
        logging.info("Saving SVD Transformed to DB")
        coll = self.dbConnection.getCollection(self.svd_df_collection)
        compdfjs = json.loads(svddf.to_json(orient=self.orient))
        return coll.insert_many(compdfjs)

    def load_svd_transformed_from_db(self):
        return self._load_from_collection(self.svd_df_collection)

    def load_svd_transformed_by_topic(self,topic):
        topic_name = topic[DistMetricCalculator.DataFrameCols.TOPIC]
        coll = self.dbConnection.getCollection(self.svd_df_collection)
        docs = coll.find({} , {topic_name : 1 , DistMetricCalculator.DataFrameCols.CHANNEL : 1})
        df = pd.DataFrame(docs).drop(["_id"] , axis=1)
        df[DistMetricCalculator.DataFrameCols.DistDF.SIGMA] = topic[DistMetricCalculator.DataFrameCols.DistDF.SIGMA]
        return df


    def get_dist_df(self,svddf):
        CONSTS = DistMetricCalculator.DataFrameCols.DistDF
        SIGMA = svddf[CONSTS.SIGMA][0]

        series = svddf.drop([DistMetricCalculator.DataFrameCols.CHANNEL ,
        CONSTS.SIGMA], axis=1)

        s = pairwise_tukeyhsd(series , svddf[DistMetricCalculator.DataFrameCols.CHANNEL])
        df= pd.DataFrame(s._results_table.data , columns=DistMetricCalculator.DataFrameCols.DistDF.ALL_COLS).iloc[1:, :]

        df[CONSTS.G1] = df[CONSTS.G1].astype("category")
        df[CONSTS.G2] = df[CONSTS.G2].astype("category")
        df[CONSTS.REJECT] = df[CONSTS.REJECT].astype(str).str.strip(" ") == "True"
        idx = df[CONSTS.REJECT] == False
        df.loc[idx,CONSTS.MEAN_DIFF] = 0
        distfinaldf = df.groupby([CONSTS.G1 , CONSTS.G2]).apply(lambda df : np.abs(df[CONSTS.MEAN_DIFF]).mean()*SIGMA)
        distfinaldf = distfinaldf.reset_index(name=CONSTS.DIST)
        return distfinaldf

    def run_job(self):
        logging.info("Starting Job")
        df = self.get_df()
        tfdf = self.get_tfidf_transformed(df)
        svddf, svd = self.get_svd_transformed(df,tfdf,self.max_num_topics)
        topics,topic_term_weights = self.get_topics(svd,tfdf)
        topicperdoc = self.get_top_topics_per_doc(svddf
                                                ,svd.singular_values_
                                                ,df[[DistMetricCalculator.DataFrameCols.TITLE,
                                                    DistMetricCalculator.DataFrameCols.URL]]
                                                , self.max_topics_per_doc)

        self.save_svd_components_to_db(svd.components_ , tfdf)
        self.save_svd_transformed_to_db(svddf)
        self.save_topics_to_db(topics,topic_term_weights,svd.singular_values_)
        self.save_top_topics_per_doc(topicperdoc , self.max_topics_per_doc)


@utils.singleton
class DistMetricDataFrameService(DistMetricCalculator):

    def __init__(self):
        dbconn = DBConnection()
        super().__init__(dbconn
                            , MongoConsts.TD_SVD_COMP_COLLECTION
                            , MongoConsts.TD_SVD_DF_COLLECTION
                            , MongoConsts.TD_TOPICS_COLLECTION
                            , MongoConsts.TD_DOC_TOPICS_COLLECTION)

    def get_topic_by_id(self,topicId):
        coll = self.dbConnection.getCollection(self.topics_collection)
        return coll.find_one({DistMetricCalculator.DataFrameCols.TOPIC : topicId} , {"_id" : 0})

    def get_documents_by_topic(self,topicId , topn=10):
        coll = self.dbConnection.getCollection(self.doc_topics_collection)
        l= list(coll.find({topicId: {"$exists" : True}} , {"_id" : 0 }).sort([(topicId,-1)]).limit(topn))
        print(l)
        return l


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dbconn = DBConnection()
    DistMetricCalculator(dbconn
                        , MongoConsts.TD_SVD_COMP_COLLECTION
                        , MongoConsts.TD_SVD_DF_COLLECTION
                        , MongoConsts.TD_TOPICS_COLLECTION
                        , MongoConsts.TD_DOC_TOPICS_COLLECTION).run_job()
