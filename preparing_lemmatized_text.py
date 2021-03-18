
import os
import re
import numpy as np
import pandas as pd
from pprint import pprint
import pickle

# Gensim
import gensim
import gensim.corpora as corpora
from gensim.utils import simple_preprocess
from gensim.models import CoherenceModel,TfidfModel
from gensim.corpora import Dictionary

# spacy for lemmatization
import spacy
from _collections import defaultdict

# Plotting tools
# import pyLDAvis
# import pyLDAvis.gensim  # don't skip this
# import matplotlib.pyplot as plt
# %matplotlib inline

# Enable logging for gensim - optional
import logging
logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.ERROR)

import warnings
warnings.filterwarnings("ignore",category=DeprecationWarning)


import nltk
nltk.download('stopwords')
#
# from sklearn.model_selection import GridSearchCV

from nltk.corpus import stopwords
stop_words = stopwords.words('english')


def remove_stopwords(texts):
    print("remove_stopwords")
    return [[word for word in simple_preprocess(str(doc)) if word not in stop_words] for doc in texts]


def make_bigrams(texts, bigram_mod):
    print("make_bigrams started")
    return [bigram_mod[doc] for doc in texts]


def make_trigrams(texts, bigram_mod, trigram_mod):
    print("make_trigrams started")
    return [trigram_mod[bigram_mod[doc]] for doc in texts]


# ['NOUN', 'ADJ', 'VERB', 'ADV']
def lemmatization(texts, nlp, allowed_postags=['NOUN', 'VERB']):
    print("lemmatization started")
    """https://spacy.io/api/annotation"""
    texts_out = []
    for sent in texts:
        doc = nlp(" ".join(sent)[:1000000])
        texts_out.append([token.lemma_ for token in doc if token.pos_ in allowed_postags])
    return texts_out


def setup_bigrams(data_words):
    print("setup_bigrams started")
    bigram = gensim.models.Phrases(data_words, min_count=5, threshold=10)  # higher threshold fewer phrases.
    trigram = gensim.models.Phrases(bigram[data_words], threshold=10)
    bigram_mod = gensim.models.phrases.Phraser(bigram)
    trigram_mod = gensim.models.phrases.Phraser(trigram)
    return bigram_mod, trigram_mod


# df_full_text=pd.DataFrame(columns=['doi', 'link', 'index'])
# final_corpus=[]
# id2word=Dictionary()

# error_index = {}
# error_list = []

pickle_path = r"C:\Users\Public\Downloads\elsapy\Final_Approach\Pickle_Object"
for root, directories, files in os.walk(pickle_path, topdown=True):
    for i in files:
        if ".pkl" in i :
            data_lemmatized_full_text = defaultdict(str)
            name=os.path.join(root, i).split("_")[-1]
            print(name)
            print(os.path.join(root, i).split("_")[-1])
            error_list = []
            print(os.path.join(root, i))


            df2 = pd.read_pickle(os.path.join(root, i))
            data_words = df2['full_text'].values.tolist()

            try:
                    bigram_mod, trigram_mod = setup_bigrams(data_words)
                    data_words_nostops = remove_stopwords(data_words)
                    data_words_bigrams = make_bigrams(data_words_nostops, bigram_mod)

                    # Initialize spacy 'en' model, keeping only tagger component (for efficiency)
                    # python3 -m spacy download en
                    nlp = spacy.load('en', disable=['parser', 'ner'])

                    # Do lemmatization keeping only noun, adj, vb, adv
                    data_lemmatized = lemmatization(data_words_bigrams, nlp, allowed_postags=['NOUN', 'VERB'])
                    data_lemmatized_full_text[os.path.join(root, i).split("_")[-1]]=data_lemmatized
                    with open(r"C:\Users\Public\Downloads\elsapy\Final_Approach\Lemmatised_Text\data_lemmatized_full_text_{}.pkl".format(name),"wb") as f:
                        pickle.dump(data_lemmatized_full_text,f)
                    # print(data_lemmatized[:1])


            except Exception as e:
                    print(e)
                    data_lemmatized_full_text[os.path.join(root, i).split("_")[-1]] = "Exception occurred: {}".format(e)
                    with open(r"C:\Users\Public\Downloads\elsapy\Final_Approach\Lemmatised_Text\data_lemmatized_full_text{}.pkl".format(name),"wb") as f:
                        pickle.dump(data_lemmatized_full_text, f)





