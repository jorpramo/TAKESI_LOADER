# -*- coding: utf-8 -*-

__author__ = 'jpradas'

import pymongo
import datetime
import nltk
import random
from nltk import word_tokenize
from nltk.corpus import stopwords
from nltk.corpus import PlaintextCorpusReader
from nltk.collocations import BigramCollocationFinder
from nltk.metrics import BigramAssocMeasures
from cStringIO import StringIO
from os import listdir
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.pdfpage import PDFPage
from pdfminer.converter import XMLConverter, HTMLConverter, TextConverter
from pdfminer.layout import LAParams
from bson.code import Code
import time

#VARIABLES GENERALES
MONGODB_URI = 'mongodb://takesibatch:takesi2015@ds053439.mongolab.com:53439/docs'
corpus_root = "C:/Users/jpradas/Documents/MASTER/TFM/CODIGO/LOADER/CORPUS/TXT/"
corpuspdf_root = "C:/Users/jpradas/Documents/MASTER/TFM/CODIGO/LOADER/CORPUS/PDF/"

def convert_pdf(path, fichero):
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    codec = 'utf-8'
    laparams = LAParams()
    outfile=corpus_root + fichero.replace('.pdf','.txt')
    outfp = file(outfile, 'w')
    device = TextConverter(rsrcmgr, outfp, codec=codec, laparams=laparams)
    fp = file(path + fichero, 'rb')

    interpreter = PDFPageInterpreter(rsrcmgr, device)
    print(fp)
    for page in PDFPage.get_pages(fp, check_extractable=True):
        #page.rotate = (page.rotate+rotation) % 360
        interpreter.process_page(page)

    fp.close()
    device.close()

    str = retstr.getvalue()
    retstr.close()
    outfp.close()
    return str

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
    cloud=nltk.FreqDist(texto_todo).most_common(100)
    all_words=[w[0].lower() for w in all_words]
    return [list(all_words),list(voc_words),list(cloud)]

def preparar_texto(texto):
    texto_final=texto[texto.index("1.1 "):]
    texto_final=texto_final.replace('..','')
    texto_final=texto_final.lower()
    return texto_final


def carga_mongodb():

    client = pymongo.MongoClient(MONGODB_URI)
    db = client.docs
    docs=db.DOCS
    spanish_stops = set(stopwords.words('spanish'))
    newcorpus = PlaintextCorpusReader(corpus_root, '.*')
    newcorpus.fileids()

    for fileid in newcorpus.fileids():

        try:
            num_words = len(newcorpus.words(fileid))
            words = newcorpus.words(fileid)
            # num_sents = len(newcorpus.sents(fileid))
            # print(newcorpus.raw(fileid))
            #bcf = BigramCollocationFinder.from_words(words)
            #filter_stops = lambda w: len(w) < 3 or w in spanish_stops
            #bcf.apply_word_filter(filter_stops)
            tags_array=vocab_words(newcorpus.raw(fileid))
            tags=tags_array[0]
            tags_vocab=tags_array[1]
            cloud=tags_array[2]
            total_cloud=[]

            for c in cloud:
                reg={}
                reg['word']=c[0]
                reg['total']=c[1]
                total_cloud.append(reg)

            #insertamos el documento
            post = {"nombre":  fileid, "fecha": datetime.datetime.utcnow(), "texto":preparar_texto(newcorpus.raw(fileid)), "tags_vocab":tags_vocab, "tags":tags, "enc":random.randint(1, 50), "pos":random.randint(1, 10), "neg":random.randint(1, 5), "num_words":num_words, "cloud":total_cloud}
            post_id = docs.insert_one(post).inserted_id

        except:
            print("Importacion Fallida:" + fileid)
def procesado_corpus():
    for fichero in listdir(corpuspdf_root):
        convert_pdf(corpuspdf_root, fichero)



def get_data_cloud():
    client = pymongo.MongoClient(MONGODB_URI)
    db = client.docs
    docs=db.DOCS

    map = Code("function m() { for(var i in this.cloud)  {emit(this.cloud[i].word, this.cloud[i].total);}}")
    reduce = Code("function(key, values) { return Array.sum(values);};")
    docs.map_reduce(map, reduce, "CLOUD")


#carga_mongodb()
get_data_cloud()
#procesado_corpus()

