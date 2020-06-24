from IndianMedia.web.dash.WordTrend import WordTrend
from IndianMedia.web.dash.WordProportion import WordProportion
from IndianMedia.web.dash.WordCorrelations import WordCorrelations
from IndianMedia.web.dash.DistMetric import DistMetric
from IndianMedia.web.dash.TermDistMetric import TermDistMetric

from flask import render_template
from flask import Flask
app = Flask(__name__)

WordTrend("/dash/wordtrend/" , app)
WordProportion("/dash/wordprop/" , app)
WordCorrelations("/dash/wordcorr/" , app)
# DistMetric("/dash/distmat/", app)
TermDistMetric("/dash/termdist/",app)

@app.route('/')
def hello_world():
    return render_template('landing.html' , dash_urls = ["/dash/wordtrend/"
                                                        , "/dash/wordprop/"
                                                        ,"/dash/wordcorr/"
                                                        ,"/dash/termdist/"])

if __name__ == "__main__":
    app.run(debug=True)
