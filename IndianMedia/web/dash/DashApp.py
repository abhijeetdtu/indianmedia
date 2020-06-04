import dash
import base64
from io import BytesIO
import dash_html_components as html
import pathlib
import os

from pandas import DataFrame
from plotnine import ggplot ,aes, geom_label ,geom_text, theme, element_blank , element_text

from IndianMedia.plotting_constants import THEME

class DashApp:

    class HTML_IDS:
        IMG = "plot"
        WORD_INPUT = 'word_input'
        CHECKLIST = "checklist"

        CHECKLIST_ALL = "All"
        SHADOW_REG_CLS = "shadow p-1 bg-white rounded"
        SHADOW_SM_CLS = "shadow-sm p-1 bg-white rounded"

    def __init__(self, route , flaskApp):
        external_stylesheets = [
            'https://stackpath.bootstrapcdn.com/bootstrap/4.5.0/css/bootstrap.min.css'
        ]
        self.app = dash.Dash(__name__,server=flaskApp
                                    , routes_pathname_prefix=route
                                    , external_stylesheets=external_stylesheets)

        self.app.index_string = """
        <!DOCTYPE html>
        <html>
            <head>
                {%metas%}
                <title>{%title%}</title>
                {%favicon%}
                {%css%}
                <link rel="stylesheet" href="/static/css/dash.css"/>
            </head>
            <body>
                {%app_entry%}
                <footer>
                    {%config%}
                    {%scripts%}
                    {%renderer%}
                </footer>
            </body>
        </html>
        """

        self.create()

    def getErrorPlot(self , msg="Error Occured"):
        df = DataFrame({"x" : [10] , "y":[2] , "label":[msg]})
        p = ggplot(df , aes(x="x" , y="y" , label="label")) + geom_text(color="white") \
            + THEME.cat_colors_lines \
              + THEME.mt \
              + theme(figure_size=(20,4) ,axis_text=element_blank(), panel_grid_major=element_blank() , panel_grid_minor=element_blank())
        return p

    def plotToImgSrc(self,p):
        fig = p.draw()

        tmpfile = BytesIO()
        fig.savefig(tmpfile, format='png' , bbox_inches='tight')
        encoded = base64.b64encode(tmpfile.getvalue()).decode('utf-8')
        plot_img = 'data:image/png;base64,{}'.format(encoded)

        return plot_img

    def plotToDashImg(self , p , id):
        src = self.plotToImgSrc(p)
        return html.Img(id=id,src=src, className="plot-img")

    def dashImg(self ,id=None, *args , **kwargs):
        return self.plotToDashImg(self.plot(*args , **kwargs), id)

    def dashSrc(self, *args ,**kwargs):
        return self.plotToImgSrc(self.plot(*args ,**kwargs))

    def plot(self):
        raise NotImplementedError()

    def setupOptions(self):
        raise NotImplementedError()

    def makeLayout(self):
        raise NotImplementedError()

    def create(self):
        self.setupOptions()
        self.makeLayout()
