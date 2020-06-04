from IndianMedia.web.dash.DashApp import DashApp
from IndianMedia.web.dash.WordProportion import WordProportion
from IndianMedia.web.dash.WordTrend import WordTrend
from IndianMedia.plotting_constants import THEME , ColorPalette
from IndianMedia.data_process.dataframe_service import DataFrameService

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from plotnine import *

import pandas as pd
import numpy as np
import random
import json

class WordCorrelations(WordTrend):

    def __init__(self,route,flaskApp):
        super().__init__(route,flaskApp)


    def _filteredDf(self,checklist, word):
        # dfs = []
        # for word in selected_words:
        #
        #     df["word"] = word
        #     dfs.append(df)
        #
        # df = pd.concat(dfs)
        df = DataFrameService().getWordCorrelations(word)

        if WordProportion.HTML_IDS.CHECKLIST_ALL in checklist:
            # if "All" selected
            checklist = df["channel_id"].unique()

        df = df[df["channel_id"].isin(checklist)]

        df["abs_corr"] = np.abs(df["corr"])
        df = df.groupby(["channel_id"]).apply(lambda x: x.sort_values(["abs_corr"], ascending = False).head(10)).reset_index(drop=True)
        df = df[df["index"] != word]
        #print(df)
        return df

    def _chart(self,df):

        if df is None:
            return self.getErrorPlot(self.ERROR_MSG.format(word=word))


        channels = df["channel_id"].unique()
        colors = scale_color_manual(values= self.colors)

        p = ggplot(df , aes(x="corr" , y=1)) \
      + geom_label(aes(label="index") , position=position_jitter(width=0,height=1)) \
      + facet_wrap("~channel_id") \
      + colors \
      + THEME.mt \
      + theme(figure_size=(20,5) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())

        return p

    def setupOptions(self):
        df = DataFrameService().getWordCorrelations(self.default_word)
        print(df)
        if df.shape[0] > 0:
            self.colors = ColorPalette.mapRandomColors( df["channel_id"], THEME.COLOR)
