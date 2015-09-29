__author__ = 'jpradas'
# -*- coding: utf-8 -*-

from text_comparer.vectorizer import compare_texts
from nltk.corpus import PlaintextCorpusReader
import codecs
import pymongo
from bson.code import Code

MONGODB_URI = 'mongodb://takesibatch:takesi2015@ds053439.mongolab.com:53439/docs'
CSV_root = "C:/Users/jpradas/Documents/MASTER/TFM/CODIGO/WEB/static/"
corpus_root = "C:/Users/jpradas/Documents/MASTER/TFM/CODIGO/LOADER/CORPUS/TXT/"

def carga():
    client = pymongo.MongoClient(MONGODB_URI)
    db = client.docs
    docs=db.SIMILITUD


    newcorpus = PlaintextCorpusReader(corpus_root, '.*')

    for fileid in newcorpus.fileids():
        for file2 in newcorpus.fileids():
            try:
                result= {"f1": fileid, "f2":file2, "value": compare_texts(newcorpus.raw(fileid), newcorpus.raw(file2))}
                docs.insert_one(result).inserted_id
            except:
                pass

def crea_csv():

    f = open(CSV_root + '/data.csv', 'w',encoding='utf8')
    client = pymongo.MongoClient(MONGODB_URI)
    db = client.docs
    docs=db.SIMILITUD
    fich = docs.find({}).sort('f1')

    f.write("f1,f2,value\n")
    for doc in fich:
        f.write('"' + doc['f1'] + '","'+ doc['f2'] +'",'+ str(doc['value']) +'\n')



carga()
crea_csv()
