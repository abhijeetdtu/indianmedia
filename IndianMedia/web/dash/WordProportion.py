from IndianMedia.web.dash.DashApp import DashApp
from IndianMedia.plotting_constants import THEME , ColorPalette
from IndianMedia.data_process.dataframe_service import DataFrameService

import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output , State

from plotnine import *

import pandas as pd
import numpy as np
import random

class WordProportion(DashApp):

    class HTML_IDS(DashApp.HTML_IDS):
        CHECKLIST_SELECTED_WORDS = "checklist_selected_words"


    def __init__(self,route,flaskApp):
        self.default_word = "modi"
        self.INPUT_PLACEHOLDER = "Search for something like Election, Coronavirus , Delhi"
        self.ERROR_MSG = "Term '{word}' not found, try something else"
        self.NO_SELECTION_MSG = "Please select at least one option."

        super().__init__(route,flaskApp)


    def _filteredDf(self,checklist, selected_words):
        dfs = []
        for word in selected_words:
            df = DataFrameService().getWordDates(word)
            df["word"] = word
            dfs.append(df)

        if len(dfs) == 0:
            return None

        df = pd.concat(dfs)

        if WordProportion.HTML_IDS.CHECKLIST_ALL in checklist:
            # if "All" selected
            checklist = df["channel_id"].unique()

        df = df[df["channel_id"].isin(checklist)]
        df = df.groupby(["channel_id" , "word"]).agg(overall_prop=("mean_prop" , "mean")).reset_index()

        return df

    def _chart(self,df,checklist , selected_words):
        if df is None:
            return self.getErrorPlot(self.ERROR_MSG.format(word=selected_words))

        colors = scale_fill_manual(values= self.colors)
        channels = df["channel_id"].unique()
        p = ggplot(df , aes(x="word" , y="overall_prop" , fill="channel_id")) \
              + geom_col(position=("fill" if len(channels) > 1 else "identity")) \
              + coord_flip() \
              + colors \
              + THEME.mt \
              + theme(figure_size=(20,5) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())

        return p

    def plot(self,checklist, selected_words):

        df = self._filteredDf(checklist , selected_words)
        p = self._chart(df,checklist , selected_words)
        return p

    def getDataFrameMethod(self):
        return DataFrameService().getWordDates

    def setupOptions(self):
        df = self.getDataFrameMethod()(self.default_word)
        self.colors = ColorPalette.mapRandomColors( df["channel_id"], THEME.COLOR)


    def makeLayout(self):
        self.app.layout = html.Div(className="dash-container container p-0 m-0", children=[
        html.Div(className="row" , children=[
            html.Div(className="col-md-3" , children=[
                dcc.Input(id=WordProportion.HTML_IDS.WORD_INPUT
                            , placeholder=self.INPUT_PLACEHOLDER
                            ,value=self.default_word
                            , type='text'
                            , className="form-control"
                            ,debounce=True),

            ]),
            html.Div(className="col-md-9" , children=[
                dcc.Checklist(
                    id=WordProportion.HTML_IDS.CHECKLIST,
                    className=f"dash-checklist {WordProportion.HTML_IDS.SHADOW_SM_CLS} form-check form-check-inline",
                    options=[],
                    value=[WordProportion.HTML_IDS.CHECKLIST_ALL]
                ),

            ])
        ]),
        html.Div(className="row-fluid" , children=[
            dcc.Checklist(
                id=WordProportion.HTML_IDS.CHECKLIST_SELECTED_WORDS,
                className=f"selected-checklist   {WordProportion.HTML_IDS.SHADOW_SM_CLS} dash-checklist form-check form-check-inline",
                options=[],
                value=[]
            )
        ])
        ,
        # html.Div(className="row-fluid" , children=[
        #     html.Img(id=WordProportion.HTML_IDS.IMG,  className="plot-img img-fluid")
        # ])
        #self.dashImg(id=WordProportion.HTML_IDS.IMG , word=self.default_word)
        dcc.Loading(
            id="loading-holder",
            color=THEME.LOADER_COLOR,
            type=THEME.LOADER_TYPE,
            children=[
                html.Div(className="row-fluid" , children=[
                    html.Img(id=WordProportion.HTML_IDS.IMG,  className="plot-img img-fluid plot-holder-div")
                ])
            ]
        )
        ]

        )

        @self.app.callback(
            Output(component_id=WordProportion.HTML_IDS.CHECKLIST, component_property='options'),
            [Input(component_id=WordProportion.HTML_IDS.WORD_INPUT, component_property='value')]
        )
        def update_checklist(input_value):
            word = input_value.lower()
            df = DataFrameService().getWordDates(word)
            options = [{"label" : c , 'value':c} for c in df["channel_id"].unique()]
            options.append({"label" : WordProportion.HTML_IDS.CHECKLIST_ALL , 'value':WordProportion.HTML_IDS.CHECKLIST_ALL})
            return options


        @self.app.callback(
            Output(component_id=WordProportion.HTML_IDS.CHECKLIST_SELECTED_WORDS, component_property='options'),
            [Input(component_id=WordProportion.HTML_IDS.WORD_INPUT, component_property='value')],
            [State(component_id=WordProportion.HTML_IDS.CHECKLIST_SELECTED_WORDS, component_property='options')]
        )
        def update_selected_words(input_word , already_selected):
            if already_selected is None:
                already_selected = []
            return already_selected + [{"label" : input_word , "value":input_word}]

        @self.app.callback(
            Output(component_id=WordProportion.HTML_IDS.CHECKLIST_SELECTED_WORDS, component_property='value'),
            [Input(component_id=WordProportion.HTML_IDS.WORD_INPUT, component_property='value')],
            [State(component_id=WordProportion.HTML_IDS.CHECKLIST_SELECTED_WORDS, component_property='value')]
        )
        def update_selected_words(input_word , already_selected):
            if already_selected is None:
                already_selected = []
            return already_selected + [input_word]


        @self.app.callback(
            Output(component_id=WordProportion.HTML_IDS.IMG, component_property='src'),
            [Input(component_id=WordProportion.HTML_IDS.CHECKLIST, component_property='value'),
            Input(component_id=WordProportion.HTML_IDS.CHECKLIST_SELECTED_WORDS, component_property='value')]
        )
        def filter_based_on_checklist(checklist , selected_words):
            if len(checklist) == 0:
                # Nothing selected then show error image
                p = self.getErrorPlot(self.NO_SELECTION_MSG)
            else:
                p = self.plot(checklist,selected_words)

            src = self.plotToImgSrc(p)
            return src
