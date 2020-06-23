import unittest
import pandas as pd
import numpy as np


from IndianMedia.data_process.dist_metric import DistMetricDataFrameService
from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.constants import MongoConsts

class DistMetricCalculatorTest(unittest.TestCase):

    def setUp(self):
        self.sut = DistMetricDataFrameService()

    def test_get_df(self):
        df = self.sut.get_df()
        self.assertTrue(df.shape[0] > 0)
        self.assertEqual(df.shape[1] ,  6)

    def test_get_vocab(self):
        df = pd.DataFrame({"text" : ["hello there whats"
                                    ,"hello there whats"
                                    ,"hello there whats"
                                    ,"hello there whats"
                                    ,"hello there whats"
                                    ,"how is it there"
                                    ,"did not unit test hello there"] , "Channel Id":["A" ,"A" ,"A" ,"A" , "B" , "A" , "C"]})

        vocab = self.sut.get_vocab(df , 1,100)
        self.assertTrue(len(vocab) > 0)

    def test_get_tfidf_transformed(self):
        df = self.sut.get_df()
        tfdf = self.sut.get_tfidf_transformed(df)

        self.assertEqual(tfdf.shape[0] , df.shape[0])
        #self.assertGreater(tfdf.shape[1] , df.shape[1])

    def test_load_svd_components_from_db(self):
        df = self.sut.load_svd_components_from_db()
        self.assertTrue(len(df.columns) > 0)
        self.assertTrue(df.shape[0] > 0)

    def test_load_svd_df_from_db(self):
        df = self.sut.load_svd_transformed_from_db()
        self.assertTrue(len(df.columns) > 0)
        self.assertTrue(df.shape[0] > 0)

    def test_save_topics_to_db(self):
        topics = [["testingASDF" , "Asdfd" , "yuiyui" , "gfdgtrtytrydf"]
                  ,["testingASDF" , "fghfgh" , "rty" , "ryr"]
                  ,["testingASDF" , "erwer" , "rwer" , "asdasd"]
                  ,["testingASDF" , "tyuytu" , "rtyrty" , "gfyttrdgdf"]]

        topic_weights = [[1,2,3,4],[1,2,3,4],[1,2,3,4],[1,2,3,4]]

        s = self.sut.save_topics_to_db(topics , topic_weights , [1,2,3,4])
        self.assertEqual(s.shape[0] , len(topics))

        conn = self.sut.getDBConnection()
        col = conn.getCollection(MongoConsts.TD_TOPICS_COLLECTION)
        q = {"testingASDF" : {"$exists" : "true"}}
        self.assertEqual(len(list(col.find(q))),4)

        col.delete_many(q)

    def test_load_topics_with_term_from_db(self):
        topic_list = self.sut.load_topics_with_term_from_db("modi")
        self.assertTrue(len(topic_list) > 0)

    def load_svd_transformed_by_topic(self):
        tdf = self.sut.load_svd_transformed_by_topic({"_TOPIC" : "topic0"})
        self.assertTrue(tdf.shape[0] > 0)

    def test_topic_to_df(self):
        topic = {'party': 0.1015512665, 'election': 0.1019614081, 'political': 0.1023140277
        , '_TOPIC': 'topic0'
        , "_SIGMA":2}

        df = self.sut.topic_to_df(topic)

        self.assertTrue("term" in df.columns)
        self.assertTrue("value" in df.columns)

    def test_get_dist_df(self):
        svddf = pd.DataFrame({"topic0" : [1,2,3,4,5,6,1,2,3]
                            , "Channel Id" :["a" , "b","a" , "b" , "c","a","c" , "b" , "b"]
                            , "_SIGMA":[2,2,2,2,2,2,2,2,2]})
        df = self.sut.get_dist_df(svddf)

        self.assertTrue("dist" in df.columns)
        self.assertTrue("g1" in df.columns)
        self.assertTrue("g2" in df.columns)

    def test_get_top_topics_per_doc(self):
        n = 9
        sigmas = np.random.randint(0,100,30)
        topics = { f"topic{i}": np.random.randint(0,10,n) for i in range(0,30)}
        topics["Channel Id"] = ["a" , "b","a" , "b" , "c","a","c" , "b" , "b"]
        #topics["_SIGMA"] = np.random.randint(0,100,30)
        vocab = list("qwetrutiyoupiasdhfgjklzxcvnmbbm ")
        docs = pd.Series(["".join(np.random.choice(vocab , 100)) for i in range(n)])
        svddf = pd.DataFrame(topics)

        df = self.sut.get_top_topics_per_doc(svddf ,sigmas,docs, 5)
        self.assertTrue(df.shape[1] , 5)

    def test_save_top_topics_per_doc(self):
        topic_df = pd.DataFrame({
            "top1": ["t1" , "t2"]
            ,"top2": ["t6" , "t8"]
            ,"v1" :[4,5]
            ,"v2" :[7,8]
            ,"doc":["TESTA" , "TESTB"]
        })

        self.sut.save_top_topics_per_doc(topic_df , 2)

        coll = self.sut.dbConnection.getCollection(self.sut.doc_topics_collection)
        t = coll.find_one({"t1" : {"$exists" : "true"}})
        self.assertIsNotNone(t)
        self.assertEqual(t["t1"] , 4)
        self.assertEqual(t["doc"] , "TESTA")

        coll.delete_many({"doc" : {"$regex" : "TESTA|TESTB"}})

if __name__ == "__main__":
    unittest.main()
