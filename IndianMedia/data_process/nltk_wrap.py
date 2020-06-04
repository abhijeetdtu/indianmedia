import nltk
from nltk.corpus import stopwords

class NLTKWrap:

    def __init__(self):
        nltk.download('stopwords')
        nltk.download('punkt')
        self.stopwords_set = stopwords.words('english')
        self.stopwords_set = set(self.stopwords_set).union(set(["episode"]))

    def getStopwords(self):
        return self.stopwords_set
