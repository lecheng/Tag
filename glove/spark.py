# coding=utf-8
import os
import json
import time
import numpy as np
from pyspark import SparkContext, SparkConf

def cos(l1,l2):
	#余弦相似度
	A = np.array(l1)
	B = np.array(l2)
	num = float(np.dot(A,B))
	denom = np.linalg.norm(A) * np.linalg.norm(B)
	cos = num/denom
	return cos

def manhattan(l1,l2):
	#曼哈顿距离
	A = np.array(l1)
	B = np.array(l2)
	dist = np.sum(np.fabs(A-B),axis=0)
	return dist

def fit(line):
	obj = json.loads(line)
	word = obj['key']
	return [(word,obj['value'])]

appName = "test"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def get_rdd_from_file():
	path = 'newvectors.txt'
	rdd = sc.textFile(path)
	rdd = rdd.flatMap(fit)
	return rdd

def related_words():
	rdd = get_rdd_from_file()
	l = rdd.collect()
	f = open('wordsdict.txt','w')
	for i in range(0,len(l)):
		v1 = l[i][1]
		sim_list = []
		d = {}
		d['key'] = l[i][0]
		for j in range(0,len(l)):
			if i != j:
				v2 = l[j][1]
				sim = cos(v1,v2)
				temp_l = [l[j][0],sim]
				sim_list += [temp_l]
		sim_list = sorted(sim_list,key= lambda a:a[1],reverse=True)
		d['value'] = sim_list[:10]
		f.write(json.dumps(d)+'\n')
	f.close()

if __name__ == '__main__':
	t1 = time.time()
	related_words()
	t2 = time.time()
	print 'Time consumption: ' + str(t2-t1) + 's'