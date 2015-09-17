__author__ = 'jpradas'

import pymongo
import datetime
import nltk, re, pprint
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import PlaintextCorpusReader
from nltk.tokenize import sent_tokenize
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
import mongodb

def LeeBagWords():
    #MONGODB_URI = 'mongodb://takesibatch:takesi2015@ds053439.mongolab.com:53439/docs'
    client = pymongo.MongoClient(MONGODB_URI)
    db = client.docs
    bag=db.BAGWORDS
    tags=bag.find(projection={"_id":0})
    vector=""
    for t in tags:

        for i,j in t.items():
                vector=j
        #data=json.loads(t)
        #print(data['tag'])
    return vector


def vocab_words(text):
    #f = open('C:/Users/jpradas/Documents/MASTER/TFM/code/data/bag/BAGWORDS.txt', 'r')
    tokens=word_tokenize(text)
    vocab=LeeBagWords()
    vocab=[x.lower() for x in vocab]
    tokens=[x.lower() for x in tokens if len(x) > 3]
    spanish_stops = set(stopwords.words('spanish'))
    texto=[w.lower() for w in tokens if w in vocab and w not in spanish_stops]
    texto_todo=[w.lower() for w in tokens if w not in spanish_stops]

    voc_words=nltk.FreqDist(texto)
    voc_words=[w for w in set(voc_words) if voc_words[w]>5]
    all_words=nltk.FreqDist(texto_todo).most_common(5)
    all_words=[w[0].lower() for w in all_words]
    return [list(all_words),list(voc_words)]


spanish_tokenizer = nltk.data.load('tokenizers/punkt/spanish.pickle')

# Conexion con MongoDB


MONGODB_URI = 'mongodb://takesibatch:takesi2015@ds053439.mongolab.com:53439/docs'
#MONGODB_URI = 'mongodb://localhost:27017/'
client = pymongo.MongoClient(MONGODB_URI)
db = client.docs
docs=db.DOCS

spanish_stops = set(stopwords.words('spanish'))

corpus_root = "C:/Users/jpradas/Documents/MASTER/TFM/code/data/corpus/"
newcorpus = PlaintextCorpusReader(corpus_root, '.*')
newcorpus.fileids()

for fileid in newcorpus.fileids():
    num_chars = len(newcorpus.raw(fileid))
    num_words = len(newcorpus.words(fileid))
    words = newcorpus.words(fileid)
    # num_sents = len(newcorpus.sents(fileid))
    num_vocab = len(set(w.lower() for w in newcorpus.words(fileid)))
    # print(newcorpus.raw(fileid))
    bcf = BigramCollocationFinder.from_words(words)
    filter_stops = lambda w: len(w) < 3 or w in spanish_stops
    bcf.apply_word_filter(filter_stops)

    tag_bi=bcf.nbest(BigramAssocMeasures.likelihood_ratio, 5)
    tags_array=vocab_words(newcorpus.raw(fileid))
    tags=tags_array[0]
    tags_vocab=tags_array[1]
    # insertamos el documento
    post = {"nombre": fileid, "fecha": datetime.datetime.utcnow(), "texto":newcorpus.raw(fileid), "tags_vocab":tags_vocab, "tags":tags, "enc":0, "pos":0, "neg":0, "num_words":num_words}
    post_id = docs.insert_one(post).inserted_id
    # insertamos el indice
    # indexa ={"nombre": fileid, "id_doc": post_id, "tag_vocab":tags, "tag_bi":tag_bi,"tag_vocab":tags, "enc":0, "pos":0, "neg":0, "num_words":num_words}
    # index.insert_one(indexa)
