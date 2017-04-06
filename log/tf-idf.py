import json
import time
import codecs
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF,IDF

appName = "tf-idf"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def extract_title(line):
	title = line['TITLE']
	title = title.replace('"','').replace('!','').replace(':','').replace('?','').replace('(','').replace(')','').split(' ')
	return title



path = 'data/raw-video-title-tag.json'
f = codecs.open(path,'r')
obj = json.loads(f.read())
rdd = sc.parallelize(obj)
documents = rdd.map(extract_title)

hashingTF = HashingTF()

def hashmap(line):
	l = []
	for i in line:
		t = (hashingTF.indexOf(i),i)
		l += [t]
	return l

m = documents.flatMap(hashmap)
tf = hashingTF.transform(documents)

l = m.collect()

def l2m(l):
	d = {}
	for i in l:
		d[i[0]] = i[1]
	return d

word_hashvalue_map = l2m(l)

tf.cache()
idf = IDF().fit(tf)
tfidf = idf.transform(tf)

print(len(word_hashvalue_map))
def solve_result(item):
	s = str(item)
	a = json.loads(s.replace('(','[').replace(')',']'))
	print a
	print len(a)
	print a[1]
	l = []
	for i in a[1]:
		l += [word_hashvalue_map[i]]
	return l
print solve_result(tfidf.collect()[0])
