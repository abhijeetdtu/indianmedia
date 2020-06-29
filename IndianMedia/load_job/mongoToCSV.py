    # %load_ext autoreload
# %autoreload 2

from IndianMedia.constants import MongoConsts
from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.utils import getCurrentDIR
from IndianMedia.constants import Channels

import pandas as pd
import json
import csv
import os
from ast import literal_eval

def getWordDatesDF(limit=None):
    connection  =DBConnection()
    collection  =connection.getCollection(MongoConsts.WORD_DATE_COLLECTION)
    dfs = []
    r = collection.find() if limit == None else collection.find().limit(limit)
    for vid in r:
        df = pd.read_json(json.dumps(vid["ts"]),orient="index")
        df.columns = [vid["word_id"]]
        dfs.append(df)

    merged = pd.concat(dfs , axis=1)
    merged.index = pd.MultiIndex.from_tuples([literal_eval(i) for i in merged.index])

    merged = merged.reset_index()

    merged["level_0"] = pd.to_datetime(merged["level_0"]  , format="%m_%d_%y")
    merged = merged.sort_values("level_0")
    merged["level_0"] = merged["level_0"].dt.strftime("%m_%d_%y")
    merged = merged.set_index(["level_0" , "level_1"])

    return merged

def getDF():
    connection  =DBConnection()

    collection  = connection.indianMediaVideoCollection

    rows = []
    for vid in collection.find():
        try:
            typeList = vid["kind"] == Channels.VID_TYPE_LIST

            info = vid["items"][0] if typeList else vid
            url = f"www.youtube.com/watch?v={info['id']}"
            info = info["snippet"]

            channelId = Channels.reverseLookup(info["channelId"])
            ptitle = vid["playlist"]["snippet"]["title"] if "snippet" in vid["playlist"] else ""
            title = info["title"]
            desc = info["description"]
            date = info["publishedAt"]

        except Exception as e:
            print(vid.keys())
            print(vid)
            print(e)
            raise e



        rows.append([channelId,ptitle,date,title,desc , url])

    header = ["Channel Id" , "Playlist Title" , "Date" , "Title" , "Description" , "Url"]

    df = pd.DataFrame(rows , columns=header)
    return df


def mongoToCSV():
    df = getDF()
    dir = getCurrentDIR()
    f = os.path.abspath(os.path.join(dir , "text.csv"))
    df.to_csv(f)


if __name__ == "__main__":
    mongoToCSV()
    #if ('items' in vid and len( vid["items"]) > 0) or vid["kind"].lower().find("playlistitem") > -1:
