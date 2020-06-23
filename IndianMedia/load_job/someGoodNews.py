import requests
from bs4 import BeautifulSoup
import hashlib
import logging
import time

from multiprocessing.dummy import Pool as ThreadPool

from IndianMedia.constants import MongoConsts
from IndianMedia.mongointf.pymongoconn import DBConnection

logging.basicConfig(level=logging.INFO)

class GoodNewsNetwork:

    class Selectors:
        ARTICLE_LINK = ".entry-title.td-module-title > a"
        POST_CONTENT = "div.td-post-content"

    def __init__(self , dbconnection ,start_page, max_pages):
        self.dbconn = dbconnection
        self.SLEEP_TIME = 5
        self.NUM_PROCESSES = 10
        self.start_page = start_page
        self.max_pages = max_pages
        self.START_URL = "https://www.goodnewsnetwork.org/category/news/world/page/{PAGE_NUM}"
        self.pool = None

    def _collect_links(self ,start_page, max_page=10):
        total = []
        for i in range(start_page,max_page,1):
            resp = requests.get(self.START_URL.format(PAGE_NUM=i+1))
            soup = BeautifulSoup(resp.text)
            allLinks = soup.select(GoodNewsNetwork.Selectors.ARTICLE_LINK)
            links = [link["href"] for link in allLinks]
            total.extend(links)
        return total

    def get_url_hash(self,url):
        return hashlib.sha224(url.encode("utf-8")).hexdigest()

    def save_article_to_db(self,article):
        self.dbconn.getCollection(MongoConsts.GOOD_NEWS_COLLECTION).update_one({"_id" : article["_id"]}, {"$set" :article},True)

    def _process_url_chunk(self, url_chunk):
        articles = []
        pool  = self.get_pool()
        for i,article in enumerate(pool.map(lambda l: self.get_article_content(l) , url_chunk)):
            if article is None:
                continue
            a = {"_id" :self.get_url_hash(url_chunk[i]) , "content":article}
            self.save_article_to_db(a)
            articles.append(a)

        logging.info(f"sleeping now for {self.SLEEP_TIME} at - {i}")
        time.sleep(self.SLEEP_TIME)
        return articles

    def get_pool(self):
        if self.pool is None:
            self.pool = ThreadPool(self.NUM_PROCESSES)
        return self.pool

    def scrape_articles(self):
        links = self._collect_links(self.start_page,self.max_pages)

        articles = []
        STEP = 20
        for i in range(0,len(links) , STEP):
            contents = self._process_url_chunk(links[i : i+STEP])
            articles.extend(contents)

        return articles

    def get_article_content(self,url):
        try:
            resp = requests.get(url)
            soup = BeautifulSoup(resp.text)
            content = soup.select_one(GoodNewsNetwork.Selectors.POST_CONTENT).text
        except Exception as e:
            logging.error(f"Failed to get article - {url} - {e}")
            content = None

        return content


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--start-page" , default=1 , type=int,help="Page to start scraping from")
    parser.add_argument("--max-pages" , default=1 , type=int,help="Maximum Number of Pages to extract from the site")
    args = parser.parse_args()

    dbconn = DBConnection()
    GoodNewsNetwork(dbconn,args.start_page,args.max_pages).scrape_articles()
