import unittest

import os
import pandas as pd
from IndianMedia.load_job.ArticleScraper import ArticleScraper
from IndianMedia.mongointf.pymongoconn import DBConnection

class ArticleScraperTest(unittest.TestCase):

    def setUp(self):
        self.sut = ArticleScraper(DBConnection())

    def test_readDFReadsInDataFrame(self):
        df = self.sut.getDF()
        self.assertTrue(df is not None)
        self.assertEqual(len(df.columns) , 3)

    def test_getAllUrls(self):
        urls = self.sut.getAllUrls()
        self.assertTrue(urls.shape[0] > 0)

    def test_getURLContent(self):
        url  ="https://abcnews.go.com/Politics/us-disrupted-alleged-russian-trolls-internet-access-midterms/story?id=61333049"
        text  =self.sut.getURLContent(url)
        self.assertTrue(len(text) > 0)

    def test_articleFunctionalities(self):
        self.sut.saveArticleInDB("abc","def" , "high" , "asd" ,"ads")
        a  =self.sut.getArticleFromDBByUrl("abc")
        self.assertEqual(a["source"],"def")
        self.sut.deleteArticleInDB("abc")
        a  =self.sut.getArticleFromDBByUrl("abc")
        self.assertEqual(a , None)

    def test_saveAllUrlContentInDB(self):
        df = self.sut.getDF()
        self.sut.saveAllUrlContentInDB(df,2)
        aa = self.sut.getAllArticlesFromDB()
        aa = list(aa)
        self.assertGreater(len(aa), 0)
        self.assertIsNotNone(aa[0][ArticleScraper.CSV_COLS.TEXT])

        self.sut.deleteArticleInDB(df.iloc[0].name)
        self.sut.deleteArticleInDB(df.iloc[1].name)

    def test_getAllArticlesFromDBAsDf(self):
        df = self.sut.getAllArticlesFromDBAsDf()
        self.assertEqual(len(df.columns) , 6)
        self.assertGreater(df.shape[0] , 0)

    def test_writeAllArticlesToCSV(self):
        fp = "./test.csv"
        self.sut.writeAllArticlesToCSV(fp)

        self.assertTrue(os.path.exists(fp))

        df = pd.read_csv(fp)
        self.assertEqual(len(df.columns) , 6)
        self.assertGreater(df.shape[0] , 0)

        os.remove(fp)




if __name__ == "__main__":
    unittest.main()
