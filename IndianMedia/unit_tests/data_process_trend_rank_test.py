import unittest
import pandas as pd
import numpy as np

from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.load_job.mongoToCSV import getWordDatesDF
from IndianMedia.data_process.trend_rank import TrendRank


class TrendRankTest(unittest.TestCase):

    TermRankCollection  ="TESTING.term_rank_collection"

    def setUp(self):
        dbconn = DBConnection()
        self.sut = TrendRank(dbconn, TrendRankTest.TermRankCollection)

    def test_getWordDatesDF(self):
        df = getWordDatesDF(10)
        print(df)
        self.assertEqual(df.shape[1] , 10)
        self.assertGreater(df.shape[0] , 1)

    def test_get_rank_matrix(self):
        word_dates = pd.DataFrame(np.random.rand(100,10) ,columns=[f"word{i}" for i in range(10)],index=pd.MultiIndex.from_product([[f"date-{i}" for i in range(25)] , ["a","b","c","d"]]))

        df = self.sut.get_rank_matrix(word_dates)
        #print(df)
        self.assertEqual(df.shape[1] , 10)
        self.assertEqual(df.shape[0] , 100)

    def test_save_load_rank_matrix(self):
        rank_matrix = pd.DataFrame(np.random.rand(100,10) ,columns=[f"word{i}" for i in range(10)],index=pd.MultiIndex.from_product([[f"date-{i}" for i in range(25)] , ["a","b","c","d"]]))

        self.sut.save_rank_matrix(rank_matrix)

        col = self.sut.get_term_rank_collection()
        count = col.find({"_id" : "word1"}).count()
        self.assertGreater(count , 0)

        df = self.sut.load_rank_matrix_for_terms(["word1" , "word2" , "word3"])

        print(df)
        self.assertEqual(df.shape[1] , 3)

        df = self.sut.load_rank_matrix_for_terms(["abcssdads"])
        self.assertIsNone(df)

        df = self.sut.load_rank_matrix_for_terms(["word1"])
        self.assertIsNotNone(df)
        col.drop()


if __name__ == "__main__":
    unittest.main()
