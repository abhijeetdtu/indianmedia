import unittest
import itertools

import pandas as pd
import numpy as np

from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.unit_tests.helper import get_lorum_ipsum_text_count_vectorized , get_vocab
from IndianMedia.data_process.term_dist_metric import TermDistMetric,TermDistMetricDataFrameService


class TermDistMetricTest(unittest.TestCase):

    TermDistCollection  = "TESTING.TermCollection"
    GroupDistCollection  = "TESTING.GroupCollection"

    @classmethod
    def setUpClass(self):
        dbconn = DBConnection()
        self.sut = TermDistMetric(dbconn
                                , TermDistMetricTest.TermDistCollection
                                , TermDistMetricTest.GroupDistCollection)

        self.n=100
        self.words = get_vocab(self.n)
        self.chnls =["a" ,"b" , "c","d"]
        self.default_term = self.words[0]

    @classmethod
    def tearDownClass(self):
        dbconn = DBConnection()
        dbconn.dropCollection(TermDistMetricTest.TermDistCollection)
        dbconn.dropCollection(TermDistMetricTest.GroupDistCollection)
        pass

    def test_get_df(self):
        df = self.sut.get_df()
        self.assertEqual(df.shape[1] , 7)

    def test_get_vectorized_text(self):
        text = ["hello there whats up" , "nice to meet you" , "asdasd asdasdoaisjd asda sda sd asd"]

        df = self.sut.get_vectorized_texts(text)
        self.assertGreater(df.shape[1] , 13)

    def test_get_term_by_term_distance_per_group(self):
        n = 100
        text = get_lorum_ipsum_text_count_vectorized(n,10)
        channels = np.random.choice(["a" , "b" , "c" , "d"] , n)
        df = self.sut.get_term_by_term_distance_per_group(text , channels)
        self.assertEqual(len(np.unique(df.index.get_level_values(0))) , len(np.unique(channels)))

    def _create_per_group_term_by_term_distance_matrix(self,words,chnls):
        n = len(words)
        distmats = pd.DataFrame(np.random.rand(n*len(chnls),n) , columns=words,index=pd.MultiIndex.from_product([chnls,words]))
        #distmats[self.sut.CHNL] = np.random.choice(chnls,n*len(chnls))
        #return distmats.set_index(self.sut.CHNL)
        return distmats

    def _mock_term_dist_vector(self ,words,chnls):
        return pd.DataFrame(np.random.rand(len(chnls),len(words)) , columns = words, index=chnls)

    def test_get_group_distance_by_term(self):
        distmat = self._create_per_group_term_by_term_distance_matrix(self.words , self.chnls)
        #distmats.index = pd.MultiIndex(distmats[[self.sut.CHNL, "words"]])
        df = self.sut.get_group_distance_by_term(self.words[0] , distmat)
        self.assertEqual(df.shape[0] ,len(self.chnls)*len(self.chnls))

    def test_save_and_load_term_distance_vector(self):
        mock_dist = self._mock_term_dist_vector(self.words,self.chnls)
        self.sut.save_term_distance_vector(self.default_term , mock_dist)

        col = self.sut.dbconn.getCollection(self.sut.term_dist_vector_collection)
        j = col.find_one({"_id":self.default_term})
        self.assertIsNotNone(j)

        df = self.sut.load_term_distance_vector(self.default_term)
        self.assertEqual(df.shape[0] , len(self.chnls))
        self.assertEqual(df.shape[1] , len(self.words))

    def test_save_and_load_group_distance_by_term(self):
        n = len(self.chnls)
        distmat = pd.DataFrame(np.random.rand(n,n) , columns=self.chnls,index=self.chnls)

        self.sut.save_group_distance_by_term(self.default_term , distmat)

        col = self.sut.dbconn.getCollection(self.sut.group_dist_by_term_collection)
        j = col.find_one({"_id":self.default_term})
        self.assertIsNotNone(j)

        df = self.sut.load_group_distance_by_term(self.default_term)
        self.assertEqual(df.shape , (n,n))

    def test_term_dist_as_packed_circles(self):
        sut = TermDistMetricDataFrameService()
        df = sut.term_dist_as_packed_circles("modi")
        print(df)


if __name__ == "__main__":
    unittest.TestLoader.sortTestMethodsUsing = None
    unittest.main()
