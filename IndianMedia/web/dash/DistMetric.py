
from IndianMedia.plotting_constants import *
from IndianMedia.web.dash.WordProportion import WordProportion
from IndianMedia.web.dash.WordTrend import WordTrend
from IndianMedia.data_process.dist_metric import DistMetricDataFrameService ,DistMetricCalculator

from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from plotnine import *
import matplotlib.pyplot as plt

import logging
logging.basicConfig(level=logging.INFO)

class DistMetric(WordTrend):

    def __init__(self,route,flaskApp):
        self.CHNL = "Channel Id"
        self.TOPIC_DOC = "doc"
        self.TOPIC_DROPDOWN_ID = "topic-selector-dropdown"
        self.TOPICS_DISPLAY = "topics-display"
        super().__init__(route,flaskApp)

    def _filteredDf(self,checklist, topicId):
        # dfs = []
        # for word in selected_words:
        #
        #     df["word"] = word
        #     dfs.append(df)
        #
        # df = pd.concat(dfs)

        distmatserv = DistMetricDataFrameService()
        topic = distmatserv.get_topic_by_id(topicId)

        #topic = sorted(topics.items() , lambda x:x[1] , reverse=True)[0]
        df = distmatserv.load_svd_transformed_by_topic(topic)
        distdf = distmatserv.get_dist_df(df)

        if WordProportion.HTML_IDS.CHECKLIST_ALL in checklist:
            # if "All" selected
            checklist = df[self.CHNL].unique()

        df = df[df[self.CHNL].isin(checklist)]
        distdf = distdf[(distdf[DistMetricCalculator.DataFrameCols.DistDF.G1].isin(checklist))
                        | (distdf[DistMetricCalculator.DataFrameCols.DistDF.G2].isin(checklist))]
        #print(df)
        return [df,distdf]

    def _chart(self,df_dist_df,checklist, topicId):

        if df_dist_df is None:
            p = self.getErrorPlot(self.ERROR_MSG.format(word=topicId))
            return [p,p]

        df , dist_df = df_dist_df

        distMatServ = DistMetricDataFrameService()
        topic = distMatServ.get_topic_by_id(topicId)
        topicDf = distMatServ.topic_to_df(topic)

        tname = topic[DistMetricCalculator.DataFrameCols.TOPIC]

        channels = df[self.CHNL].unique()
        colors = scale_fill_manual(values= self.colors)

        p1 = ggplot(topicDf,aes(x=1 , y="value" , fill="term")) \
        + geom_col(position="fill")\
        +  scale_fill_manual(values = ColorPalette.mapRandomColors( topicDf["term"], THEME.COLOR)) \
        + ggtitle("Topic Composition")\
      + THEME.mt \
      + theme(figure_size=(1,5) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())


        p2 = ggplot(df,aes(x=tname , fill=self.CHNL)) \
        + geom_density(color= None , alpha=0.8)\
      + colors \
      + ggtitle("Topic Frequency")\
      + THEME.mt \
      + theme(figure_size=(5,4) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())

        DIST_CONSTS = DistMetricCalculator.DataFrameCols.DistDF
        p3 = ggplot(dist_df,aes(x=DIST_CONSTS.G1 ,y=DIST_CONSTS.G2, fill=DIST_CONSTS.DIST)) \
          + ggtitle("Distance Matrix")\
        + geom_tile()\
      + THEME.gradient_colors \
      + THEME.mt \
      + theme(figure_size=(5,5) , panel_grid_major=element_blank() , panel_grid_minor=element_blank())

        return [p1, p2 ,p3]


    def _update_dropdown_values(self,word):
        topics = DistMetricDataFrameService().load_topics_with_term_from_db(word)
        keys = lambda topic : [k for k in topic.keys() if k not in [DistMetricCalculator.DataFrameCols.TOPIC
                                                                    , DistMetricCalculator.DataFrameCols.TOPIC_SIGMA]]
        options = [{"label" : "-".join(keys(topic)) , "value":topic[DistMetricCalculator.DataFrameCols.TOPIC]} for topic in topics]
        return options

    def _update_dropdown_default_value(self,word):
        topics = DistMetricDataFrameService().load_topics_with_term_from_db(word)
        return topics[0][DistMetricCalculator.DataFrameCols.TOPIC]

    def setupCallBacks(self):
        @self.app.callback(
            Output(component_id=WordTrend.HTML_IDS.CHECKLIST, component_property='options'),
            [Input(component_id=WordTrend.HTML_IDS.WORD_INPUT, component_property='value')]
        )
        def update_checklist(input_value):
            return self._update_checklist(input_value)

        @self.app.callback(
            Output(component_id=self.TOPIC_DROPDOWN_ID, component_property='options'),
            [Input(component_id=WordTrend.HTML_IDS.WORD_INPUT, component_property='value')]
        )
        def update_dropdown_values(word):
            return self._update_dropdown_values(word)

        # @self.app.callback(
        #     Output(component_id=self.TOPICS_DISPLAY, component_property='children'),
        #     [Input(component_id=self.TOPIC_DROPDOWN_ID, component_property='value')]
        # )
        # def update_available_topicss(value):
        #     distmatserv = DistMetricDataFrameService()
        #     topics = distmatserv.get_documents_by_topic(value , 10)
        #     return html.Div(children=[
        #         html.Div(children=[html.Span(t[self.CHNL]), html.Br(),html.Span(t[self.TOPIC_DOC ])]) for t in topics
        #     ])

        @self.app.callback(
            Output(component_id=self.TOPIC_DROPDOWN_ID, component_property='value'),
            [Input(component_id=WordTrend.HTML_IDS.WORD_INPUT, component_property='value')]
        )
        def update_dropdown_default_value(word):
            return self._update_dropdown_default_value(word)

        @self.app.callback(
            Output(component_id=WordTrend.HTML_IDS.IMG, component_property='children'),
            [Input(component_id=WordTrend.HTML_IDS.CHECKLIST, component_property='value'),
            Input(component_id=self.TOPIC_DROPDOWN_ID, component_property='value')]
        )
        def filter_based_on_checklist(checklist , topicId):
            return self._filter_based_on_checklist(checklist , topicId)

    def makeInputLayout(self):
        return html.Div(
                    [super().makeInputLayout(),
                        html.Div(className="row", children=[
                            html.Div(className="col-md-1 topic-indicator" , children=[
                                html.Span("TOPIC")
                            ]),
                            html.Div(className="col-md-11" , children=[
                                dcc.Dropdown(
                                    id='topic-selector-dropdown',
                                    options=[],
                                    value='')]

                            )]),
                        html.Div(className="row", children=[
                            html.Span(id="topics-display")
                        ])
                    ]
                )

    def makePlotImgsLayout(self,plotImgs):
        return html.Div(className="row imgs-container"
        ,children=[
                html.Div(className="col-md-2 pr-0" , children=[plotImgs[0]])
            , html.Div(className="col-md-6 p-0" , children=[plotImgs[1]])
            , html.Div(className="col-md-4 p-0" , children=[plotImgs[2]])
        ])
