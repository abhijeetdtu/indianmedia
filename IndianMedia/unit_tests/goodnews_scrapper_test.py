import unittest
from unittest.mock import patch

from IndianMedia.constants import MongoConsts
from IndianMedia.load_job.someGoodNews import GoodNewsNetwork
from IndianMedia.mongointf.pymongoconn import DBConnection

class GoodNewsNetworkTests(unittest.TestCase):

    @patch('IndianMedia.mongointf.pymongoconn.DBConnection')
    def setUp(self,dbconn):
        self.sut = GoodNewsNetwork(dbconn ,1, 1)

    def test_collect_links(self):
        links = self.sut._collect_links(1,1)
        #print(links)
        self.assertTrue(len(links) > 0)
        self.assertTrue(type(links[0]) , str)

    def test_get_article_content(self):
        URL = "https://www.goodnewsnetwork.org/canadian-government-buys-hotels-to-house-homeless-people-during-covid/"
        content = self.sut.get_article_content(URL)
        #print(links)
        self.assertTrue(len(content) > 0)
        self.assertTrue(type(content) , str)

    def test_get_url_hash(self):
        hash = self.sut.get_url_hash("asdsad/asdasdsad/asdsad")
        #print(hash)
        self.assertTrue(len(hash) > 0)
        self.assertTrue(type(hash) , str)

    def test_save_article_to_db(self):
        self.sut = GoodNewsNetwork(DBConnection() ,1, 1)

        self.sut.save_article_to_db({"_id":"abc" , "content":"asd"})

        q = {"_id" : "abc"}
        coll = self.sut.dbconn.getCollection(MongoConsts.GOOD_NEWS_COLLECTION)
        a = coll.find_one(q)
        self.assertIsNotNone(a)
        coll.delete_one(q)
        a = coll.find_one(q)
        self.assertIsNone(a)

    def test_scrape_articles(self):
        articles = self.sut.scrape_articles()
        #print(hash)
        self.assertTrue(len(articles) > 0)

if __name__ == "__main__":
    unittest.main()
