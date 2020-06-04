from IndianMedia.web.dash.DashApp import DashApp
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

class WordTrend(DashApp):

    def __init__(self,route,flaskApp):
        self.default_word = "modi"
        self.INPUT_PLACEHOLDER = "Search for something like Election, Coronavirus , Delhi"
        self.ERROR_MSG = "Term not found, try something else"
        self.NO_SELECTION_MSG = "Please select at least one option."

        super().__init__(route,flaskApp)


    def _filteredDf(self,checklist , word):
        df = DataFrameService().getWordDates(word)

        if df is None:
            return None

        if WordTrend.HTML_IDS.CHECKLIST_ALL in checklist:
            # if "All" selected
            checklist = df["channel_id"].unique()

        df = df[df["channel_id"].isin(checklist)]

        df["rolling"] = df.groupby(["channel_id"])["mean_prop"].transform(lambda x : x.rolling(14,1).mean())
        #df["rolling"] = df["mean_prop"]
        df["date"] = pd.to_datetime(df["date"],format="%m_%d_%y")

        return df

    def _chart(self,df):

        if df is None:
            return self.getErrorPlot(self.ERROR_MSG)

        channels = df["channel_id"].unique()

        colors = scale_color_manual(values= self.colors)
        #+ colors \
        p = ggplot(df , aes(x="date" , y="rolling" , color="channel_id"  ,group="channel_id")) \
            + geom_line(size=3) \
            + colors \
            + THEME.mt \
            + theme(figure_size=(20,5) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())

        return p


    def plot(self,checklist, selected_words):
        df = self._filteredDf(checklist , selected_words)
        p = self._chart(df)
        return p


    def setupOptions(self):
        df = DataFrameService().getWordDates(self.default_word)
        if df.shape[0] > 0:
            self.colors = ColorPalette.mapRandomColors( df["channel_id"], THEME.COLOR)


    def makeLayout(self):

        self.app.layout = html.Div(className="dash-container container p-0 m-0", children=[
        html.Div(className="row" , children=[
            html.Div(className="col-md-3" , children=[
                dcc.Input(id=DashApp.HTML_IDS.WORD_INPUT
                            , placeholder=self.INPUT_PLACEHOLDER
                            ,value=self.default_word
                            , type='text'
                            , className="form-control"
                            ,debounce=True),

            ]),
            html.Div(className="col-md-9" , children=[
                dcc.Checklist(
                    id=DashApp.HTML_IDS.CHECKLIST,
                    className=f"dash-checklist {DashApp.HTML_IDS.SHADOW_SM_CLS} form-check form-check-inline",
                    options=[],
                    value=[DashApp.HTML_IDS.CHECKLIST_ALL]
                ),

            ])
        ]),
        html.Div(className="row-fluid" , children=[
            html.Img(id=DashApp.HTML_IDS.IMG,  className="plot-img img-fluid")
        ])])
        #
        # self.app.layout = html.Div(children=[
        # dcc.Input(id=WordTrend.HTML_IDS.WORD_INPUT
        #             , placeholder=self.INPUT_PLACEHOLDER
        #             ,value=self.default_word
        #             , type='text'
        #             , className="form-control"
        #             ,debounce=True),
        # dcc.Checklist(
        #     id=WordTrend.HTML_IDS.CHECKLIST,
        #     className="dash-checklist form-check form-check-inline",
        #     options=[],
        #     value=[WordTrend.HTML_IDS.CHECKLIST_ALL]
        # ),
        # #self.dashImg(id=WordTrend.HTML_IDS.IMG , word=self.default_word)
        # html.Img(id=WordTrend.HTML_IDS.IMG,  className="plot-img img-fluid")
        # ]
        #
        # )

        @self.app.callback(
            Output(component_id=WordTrend.HTML_IDS.CHECKLIST, component_property='options'),
            [Input(component_id=WordTrend.HTML_IDS.WORD_INPUT, component_property='value')]
        )
        def update_checklist(input_value):
            word = input_value.lower()
            df = DataFrameService().getWordDates(word)
            options = [{"label" : c , 'value':c} for c in df["channel_id"].unique()]
            options.append({"label" : WordTrend.HTML_IDS.CHECKLIST_ALL , 'value':WordTrend.HTML_IDS.CHECKLIST_ALL})
            return options


        @self.app.callback(
            Output(component_id=WordTrend.HTML_IDS.IMG, component_property='src'),
            [Input(component_id=WordTrend.HTML_IDS.CHECKLIST, component_property='value'),
            Input(component_id=WordTrend.HTML_IDS.WORD_INPUT, component_property='value')]
        )
        def filter_based_on_checklist(checklist , word):
            if len(checklist) == 0:
                # Nothing selected then show error image
                p = self.getErrorPlot(self.NO_SELECTION_MSG)
            else:
                p = self.plot(checklist ,word)

            src = self.plotToImgSrc(p)
            return src