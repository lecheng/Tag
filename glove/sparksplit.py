# coding=utf-8
from pyspark import SparkContext, SparkConf
import json
import nltk

def fit(line):
	#筛选glove语料库中的名词，专有名词和动词原形
	vals = line.rstrip().split(' ')
	word = vals[0]
	f_word = nltk.pos_tag([word])[0]
	if f_word[1] in ['NN','NNP','VB']:
		return [(word,map(float, vals[1:]))]
	else:
		return []

# def fit2(line):
# 	#筛选处理后的glove语料库中的名词，专有名词和动词原形
# 	obj = json.loads(line)
# 	word = obj['key']
# 	f_word = nltk.pos_tag([word])[0]
# 	if f_word[1] in ['NN','NNP','VB']:
# 		return [(word,obj['value'])]
# 	else:
# 		return []

def list2file(l):
	#将list内容保存到文件中
	f = open('newvectors.txt','w')
	for i in range(0,len(l)):
		d = {}
		d['key'] = l[i][0]
		d['value'] = l[i][1]
		f.write(json.dumps(d)+'\n')
	f.close()

appName = "test"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

rdd = sc.textFile('vectors.txt')
rdd = rdd.flatMap(fit)
l = rdd.collect()
list2file(l)

