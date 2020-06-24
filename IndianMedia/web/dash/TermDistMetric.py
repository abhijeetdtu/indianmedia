from IndianMedia.web.dash.DashApp import DashApp
from IndianMedia.web.dash.WordTrend import WordTrend
from IndianMedia.data_process.term_dist_metric import TermDistMetricDataFrameService
from IndianMedia.plotting_constants import THEME, ColorPalette

from multiprocessing.dummy import Pool as ThreadPool

#from plotnine import ggplot,geom_tile,aes,element_blank,theme,scale_fill_manual,geom_line,geom_text
import pandas as pd
from plotnine import *

class TermDistMetric(WordTrend):

    def setupOptions(self):
        df = TermDistMetricDataFrameService().term_dist_as_packed_circles(self.default_word)
        if df.shape[0] > 0:
            self.colors = ColorPalette.mapRandomColors( df["chnl"], THEME.COLOR)

    def _filteredDf(self,checklist , word):
        tdf = TermDistMetricDataFrameService().term_dist_as_packed_circles(word)
        gdf = TermDistMetricDataFrameService().load_group_distance_by_term(word)

        if DashApp.HTML_IDS.CHECKLIST_ALL in checklist:
            checklist = set(gdf["C1"]).union(set(gdf["C2"]))

        gdf = gdf[(gdf["C1"].isin(checklist)) | (gdf["C2"].isin(checklist))]
        tdf = tdf[tdf["chnl"].isin(checklist)]

        return tdf,gdf

    def _chart(self,dfs,checklist, word):
        tdf,gdf = dfs

        if dfs is None:
            return self.getErrorPlot(self.ERROR_MSG)

        colors = scale_color_manual(values= self.colors)

        # p = ggplot(gdf , aes(x="C1" , y="C2" , fill="dist")) \
        #     + geom_tile() \
        #     + THEME.gradient_colors\
        #     + THEME.mt \
        #     + theme(figure_size=(20,5) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())

        # + geom_text(tdf.drop_duplicates("word"), aes(x="x_word" , y="y_word" ,label="word"), color="white")\
        # + geom_label(tdf.drop_duplicates("chnl"),aes(x="x_chnl" , y="y_chnl" ,label="chnl",fill="chnl") , size=20,color="white")\
        # + annotate("text",x=chnls["x_chnl"] , y=chnls["y_chnl"] ,label=chnls["chnl"] , size=20,color="white")\
        #print(tdf.drop_duplicates("word").shape)
        #print(tdf.drop_duplicates("chnl").shape)

        p2 = ggplot(tdf)\
            + geom_segment( aes(x="x_chnl" , y="y_chnl",xend="x_word" , yend="y_word" , color="chnl")
                        , alpha=0.2
                        , linetype='dashed')\
            + geom_text(tdf.drop_duplicates("word"), aes(x="x_word" , y="y_word" ,label="word"), color="white")\
            + geom_label(tdf.drop_duplicates("chnl"),aes(x="x_chnl" , y="y_chnl" ,label="chnl",fill="chnl") , size=20,color="white")\
            + colors\
            + THEME.mt\
            + theme(figure_size=(20,7)
            , panel_grid_major=element_blank()
            , panel_grid_minor=element_blank()
            , axis_title=element_blank())

        return p2
