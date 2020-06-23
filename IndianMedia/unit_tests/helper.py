import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer

def get_vocab(num_words):
    vocab = list("qwertyuiopsadfghjklzxcvbnm")
    return  ["".join(np.random.choice(vocab , np.random.randint(3,6))) for j in range(num_words)]

def get_lorum_ipsum_text(num_sent , num_word_per_sent):
    vocab_words = get_vocab(5*num_word_per_sent)
    return [" ".join(np.random.choice(vocab_words ,num_word_per_sent)) for i in range(num_sent) ]

def get_lorum_ipsum_text_count_vectorized(num_sent , num_word_per_sent):
    text = get_lorum_ipsum_text(num_sent , num_word_per_sent)
    cvect = CountVectorizer()
    tf = pd.DataFrame(cvect.fit_transform(text).toarray() , columns=cvect.get_feature_names())
    return tf
