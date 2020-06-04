# %load_ext autoreload
# %autoreload 2

from IndianMedia.mongointf.pymongoconn import DBConnection
from IndianMedia.utils import getCurrentDIR
from IndianMedia.constants import Channels

import pandas as pd

import csv
import os


def getDF():
    connection  =DBConnection()

    collection  = connection.indianMediaVideoCollection

    rows = []
    for vid in collection.find():
        typeList = vid["kind"] == Channels.VID_TYPE_LIST

        info = vid["items"][0] if typeList else vid
        info = info["snippet"]

        channelId = Channels.reverseLookup(info["channelId"])
        ptitle = vid["playlist"]["snippet"]["title"]
        title = info["title"]
        desc = info["description"]
        date = info["publishedAt"]

        rows.append([channelId,ptitle,date,title,desc])

    header = ["Channel Id" , "Playlist Title" , "Date" , "Title" , "Description"]

    df = pd.DataFrame(rows , columns=header)
    return df


def mongoToCSV():
    df = getDF()
    dir = getCurrentDIR()
    f = os.path.abspath(os.path.join(dir , "text.csv"))
    df.to_csv(f)


    #if ('items' in vid and len( vid["items"]) > 0) or vid["kind"].lower().find("playlistitem") > -1:
