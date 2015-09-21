__author__ = 'jpradas'
# -*- coding: utf-8 -*-

from text_comparer.vectorizer import compare_texts
from nltk.corpus import PlaintextCorpusReader
import pymongo

MONGODB_URI = 'mongodb://takesibatch:takesi2015@ds053439.mongolab.com:53439/docs'
corpus_root = "C:/Users/jpradas/Documents/MASTER/TFM/CODIGO/LOADER/CORPUS/TXT/"

client = pymongo.MongoClient(MONGODB_URI)
db = client.docs
docs=db.SIMILITUD

completo=[]
newcorpus = PlaintextCorpusReader(corpus_root, '.*')
result={}
for fileid in newcorpus.fileids():
    for file2 in newcorpus.fileids():
        result= {"f1": fileid, "f2":file2, "value": compare_texts(newcorpus.raw(fileid), newcorpus.raw(file2))}
        docs.insert_one(result).inserted_id

print("FIN")









