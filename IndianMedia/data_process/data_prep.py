import pandas as pd
import numpy as np
import json

import logging
from nltk import RegexpTokenizer
from sklearn.feature_extraction.text import CountVectorizer , TfidfVectorizer

from IndianMedia import utils
from IndianMedia.load_job.mongoToCSV import getDF
from IndianMedia.constants import FlatFiles
from IndianMedia.data_process.nltk_wrap import NLTKWrap
from IndianMedia.mongointf.pymongoconn import DBConnection

class DataPrepJob:

    def _getPromotionText(self):
        promotion = """
        like our work?  click here to support the wire: https://thewire.in/support

        the founding premise of the wire is this: if good journalism is to survive and thrive, it can only do so by being both editorially and financially independent. this means relying principally on contributions from readers and concerned citizens who have no interest other than to sustain a space for quality journalism. as a publication, the wire will be firmly committed to the public interest and democratic values.
        we publish in four different languages
        for english, visit www.thewire.in
        for hindi: http://thewirehindi.com/
        for urdu: http://thewireurdu.com
        for marathi: https://marathi.thewire.in
        if you are a young writer or a creator, you can submit articles, essays, photos, poetry – anything that’s straight out of your imagination – to livewire, the wire’s portal for the young, by the young. https://livewire.thewire.in/
        you can also follow the wire’s social media platforms and engage with us.
        facebook
        https://www.facebook.com/thewire/
        https://www.facebook.com/thewirehindi/
        https://www.facebook.com/thewireurdu/
        https://www.facebook.com/thewiremarathi/
        twitter
        https://twitter.com/thewire_in
        https://twitter.com/thewirehindi
        https://twitter.com/thewireurdu
        https://twitter.com/thewiremarathi
        https://twitter.com/livewire
        instagram
        https://www.instagram.com/thewirein/
        https://www.instagram.com/livewirein/
        don’t forget to hit the subscribe button to never miss a video from the wire!
        """
        return promotion

    def __init__(self):
        self.df = getDF()
        print(self.df.head())
        self.dbcon = DBConnection()

    def getVocab(self,df):
        vectorizer = CountVectorizer(min_df=5)
        vectorizer.fit(df["text"])
        return set(vectorizer.get_feature_names())


    def _correlationMatrix(self, writeToDisk=False):
        logging.info("Preparing Correlation Matrix")
        #import pdb


        #wordCorrelations = self.countdf.corr()
        cols = self.cvect.get_feature_names()

        #pdb.set_trace()

        corrs_df = self.countdf.groupby("channel_id").apply(lambda df : pd.DataFrame(np.corrcoef(df,rowvar=False) , columns=cols , index=cols)  )

        def word_corr(col):
            col_corr = col.reset_index().pivot_table(index="level_1",columns="channel_id" )
            col_corr = col_corr.droplevel(0 , axis=1)
            js = {"word_id" : col.name , "corrs" : json.loads(col_corr.to_json(orient="columns"))}
            self.dbcon.wordCorrCollection.update_one({"word_id" : col.name} , {"$set" : js} , True)

        corrs_df.apply(word_corr)

        # for col in corrs_df:
        #     col_corr = corrs_df[col].reset_index().pivot_table(index="level_1",columns="channel_id" )
        #     col_corr = col_corr.droplevel(0 , axis=1)
        #     js = {"word_id" : col , "corrs" : json.loads(col_corr.to_json(orient="columns"))}
        #     self.dbcon.wordCorrCollection.insert_one(js)


        # corr_f.apply(lambda col : pd.DataFrame([col , channel_id] , columns=["word" , "channel_id"]))
        #
        # words =  np.repeat(cols, len(corrs_df.index.values))
        #
        #
        # wordCorrelations = pd.DataFrame(np.corrcoef(self.countdf.drop("channel_id" , axis=1) , rowvar=False)
        # , index=pd.Index(cols)
        # ,columns=cols)
        #
        #
        # wordCorrelations = pd.DataFrame(np.corrcoef(self.countdf.drop("channel_id" , axis=1) , rowvar=False)
        # , index=pd.Index(cols)
        # ,columns=cols)
        #
        # corr = self.countdf.groupby("channel_id").apply(np.corrcoef).reset_index().pivot(columns="channel_id")
        #
        #
        # print(corr)
        # wordCorrelations = pd.DataFrame(np.corrcoef(self.countdf.drop("channel_id" , axis=1) , rowvar=False)
        # , index=pd.Index(cols)
        # ,columns=cols)
        # wordCorrelations[np.abs(wordCorrelations) == 1] = np.NaN
        #
        # js = json.loads(wordCorrelations.to_json(orient="index"))
        # for k,v in js.items():
        #     self.dbcon.wordCorrCollection.insert_one({"word_id" : k , "corrs" :v})
        #
        # if writeToDisk:
        #     wordCorrelations.to_csv(utils.pathToDataDIR(FlatFiles.WORD_CORR))

    def _prepareCountDf(self):
        logging.info("Preparing Count DF")

        self.df = self.df.drop_duplicates()
        self.df = self.df.dropna()
        self.df["text"] = self.df["Title"] + "\n" + self.df["Description"]
        self.df["text"] = self.df["text"].str.lower()

        self.stopwords_set = NLTKWrap().getStopwords()
        self.promotion= self._getPromotionText()
        self.tokenizer = RegexpTokenizer(r"\w{3,}")
        self.promotion = set(self.tokenizer.tokenize(self.promotion))
        vocabs = self.df.groupby("Channel Id").apply(self.getVocab)
        self.final_vocab = CountVectorizer(min_df=2).fit([" ".join(list(v)) for v in vocabs])\
                                               .get_feature_names()

        def tokenizeRows(sent):
            try:
            #print(sent)
                tokens = [w for w in self.tokenizer.tokenize(sent) if w not in self.stopwords_set and w not in self.promotion and w in self.final_vocab]
                return " ".join(tokens)
            except Exception as e:
                print(e)
                return None

        logging.info("---- Tokenizing")
        self.df["filtered_text"] = self.df["text"].apply(tokenizeRows)
        self.df = self.df.dropna()

        logging.info("---- Vectorizing")
        self.cvect = TfidfVectorizer(ngram_range=(1,1) , min_df=10 ,max_df=0.5)

        allSentsCounts = self.cvect.fit_transform(self.df["filtered_text"])

        self.countdf = pd.DataFrame(allSentsCounts.toarray()  , columns=self.cvect.get_feature_names() , index=self.df.index)
        self.countdf["channel_id"] = self.df["Channel Id"]

    def _timeSeries(self , writeToDisk):
        logging.info("Preparing Time Series")


        self.countdf["date"] = pd.to_datetime(self.df["Date"])
        self.countdf["date"] = self.countdf["date"].dt.strftime("%m_%d_%y").astype("category")
        #word_by_date = pd.melt(self.countdf , id_vars=["channel_id" , "date"])

        word_by_date = self.countdf.groupby(["date" , "channel_id"]).agg("mean")

        logging.info("-- Saving Time series into Mongo")
        js = json.loads(word_by_date.to_json(orient="columns"))
        for k,v in js.items():
            self.dbcon.wordDateCollection.update_one({"word_id" : k} ,{ "$set": {"word_id" : k, "ts" :v}} , True)


        #word_by_date["date"] = pd.to_datetime(word_by_date["date"] , format="%m_%d_%y")
        #word_by_date["date_month"] = word_by_date["date"].dt.month
        #word_by_date["date_week"] = word_by_date["date"].dt.week
        # logging.info("-- Saving Time series into Mongo")
        # js = json.loads(word_by_date.to_json(orient="records"))
        # self.dbcon.wordDateCollection.insert_many(js)

        if writeToDisk:
            word_by_date.to_csv(utils.pathToDataDIR(FlatFiles.WORD_BY_DATE))

    def prepare(self):
        self._prepareCountDf()
        self._correlationMatrix(False)
        self._timeSeries(False)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    DataPrepJob().prepare()
