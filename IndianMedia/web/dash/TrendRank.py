import pandas as pd
import numpy as np
from sklearn.preprocessing import MinMaxScaler as MMS

from IndianMedia.plotting_constants import THEME , ColorPalette
from IndianMedia.web.dash.DashApp import DashApp
from IndianMedia.web.dash.WordProportion import WordProportion
from IndianMedia.data_process.trend_rank import TrendRank as TR, TrendRankDataFrameService

from plotnine import *
from mizani.formatters import date_format

class TrendRank(WordProportion):

    class Consts:
        VAR_NAME = "word"
        VAL_NAME = "rank"
        ROLLING = "rolling"

    def __init__(self,route,flaskApp):
        self.default_words = ["modi","china"]
        super().__init__(route,flaskApp)

    def _filteredDf(self,checklist , selected_words):


        gdf = TrendRankDataFrameService().load_rank_matrix_for_terms(selected_words)

        if DashApp.HTML_IDS.CHECKLIST_ALL in checklist:
            checklist = set(gdf[TR.COLS.CHNL])

        gdf = gdf[gdf[TR.COLS.CHNL].isin(checklist)]

        #gdf = gdf.fillna(0)

        cols = gdf.drop([TR.COLS.DATE,TR.COLS.CHNL],axis=1).columns

        # gdf.loc[:,cols] = gdf.groupby([TR.COLS.CHNL])\
        #          .apply(lambda df: df.drop([TR.COLS.DATE,TR.COLS.CHNL],axis=1)\
        #                             .transform(lambda x : x.rolling(20,1).mean())\
        #                             .rank(axis=1,method="min" , ascending=False))
        #
        # df = pd.melt(gdf , id_vars=[TR.COLS.DATE , TR.COLS.CHNL] , var_name=TrendRank.Consts.VAR_NAME , value_name=TrendRank.Consts.VAL_NAME)


        def get_top_rank(row):
            r = row.dropna().index[row.dropna().argsort()].values
            if len(r) > 0:
                return r[0]
            return None


        per_group_topdf = gdf.groupby([TR.COLS.CHNL])\
                           .apply(lambda df: df.join(
                                                    pd.Series(
                                                        df.drop([TR.COLS.DATE,TR.COLS.CHNL],axis=1)\
                                                           .apply(lambda row: get_top_rank(row),axis=1)
                                                        ,name=TrendRank.Consts.VAR_NAME)
                                                , how="inner")
                                )

        #per_group_topdf = pd.merge(per_group_topdf,gdf.reset_index() , left_on="level_1" ,right_on="index" , how="inner")
        #per_group_topdf[TR.COLS.DATE] = gdf[TR.COLS.DATE]

        per_group_topdf = per_group_topdf.drop(cols, axis=1)

        #df[TR.COLS.CHNL] = df[TR.COLS.CHNL].astype("category")
        #df[TrendRank.Consts.VAR_NAME] = df[TrendRank.Consts.VAR_NAME].astype("category")
        return per_group_topdf

    def _chart(self,df,checklist , selected_words):

        if df is None:
            return self.getErrorPlot(self.ERROR_MSG.format(word=selected_words))

        p = (#ggplot(df , aes(x=TR.COLS.DATE  , y=TrendRank.Consts.VAL_NAME))
            ggplot(df )
            #+ geom_tile(aes(x=TR.COLS.DATE , y=TrendRank.Consts.VAL_NAME , fill=TrendRank.Consts.VAR_NAME))
            + geom_tile(aes(x=TR.COLS.DATE , y=TR.COLS.CHNL , fill=TrendRank.Consts.VAR_NAME))
            + scale_x_datetime(labels=date_format('%m/%y'))
            #+ geom_point(aes(fill=TrendRank.Consts.VAR_NAME, alpha=TrendRank.Consts.VAL_NAME) , stroke=0)
            #+ geom_smooth(aes(group=TrendRank.Consts.VAR_NAME , color=TrendRank.Consts.VAR_NAME),se=False)
            #+ geom_line(aes(group=TrendRank.Consts.VAR_NAME , color=TrendRank.Consts.VAR_NAME))
            #+ scale_y_discrete(limits = list(reversed(np.arange(len(selected_words)))))
            + ggtitle("Top Term over Time Across Categories")
            + THEME.mt
            + theme(figure_size=(20,5)
            , panel_grid_major=element_blank()
            , panel_grid_minor=element_blank()))

        # p = ggplot(df , aes(x=TR.COLS.DATE  , y=TrendRank.Consts.VAR_NAME))\
        #     + geom_tile(aes(fill=TrendRank.Consts.VAL_NAME))\
        #     + facet_grid(f"~{TR.COLS.CHNL}")\
        #     + THEME.mt \
        #     + theme(figure_size=(20,5) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())

        return p
