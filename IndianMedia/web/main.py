from IndianMedia.web.dash.WordTrend import WordTrend
from IndianMedia.web.dash.WordProportion import WordProportion
from IndianMedia.web.dash.WordCorrelations import WordCorrelations
from IndianMedia.web.dash.DistMetric import DistMetric
from IndianMedia.web.dash.TrendRank import TrendRank
from IndianMedia.web.dash.TermDistMetric import TermDistMetric

from flask import render_template
from flask import Flask
app = Flask(__name__)

#
# WordTrend("/dash/wordtrend/" , app)
# WordProportion("/dash/wordprop/" , app)
# WordCorrelations("/dash/wordcorr/" , app)
# # DistMetric("/dash/distmat/", app)
# TermDistMetric("/dash/termdist/",app)
# TrendRank("/dash/trendrank/" , app)

dash_endpoints = {
    "/dash/wordtrend/" : [WordTrend , "Word Trend"],
    "/dash/wordprop/" : [WordProportion , "Word Proportions"],
    "/dash/wordcorr/" : [WordCorrelations , "Word Correlations"],
    #"/dash/termdist/" : [TermDistMetric , "Term Distance"],
    "/dash/trendrank/" : [TrendRank , "Word Rank Trend"]
}

def initialize_dash(app):
    return [v[0](k , app) for k,v in dash_endpoints.items()]

initialize_dash(app)

@app.route('/')
def hello_world():
    dash_urls = [[k,v[1]] for k,v in dash_endpoints.items()]
    return render_template('landing.html' , dash_urls = dash_urls)


@app.route('/help/<string:page_id>')
def help_page(page_id):
    return render_template("Indian_Media.html")

if __name__ == "__main__":
    app.run(debug=True)
