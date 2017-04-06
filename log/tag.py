import json
import time
import numpy as np
from pyspark import SparkContext, SparkConf

appName = "test"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def fit(line):
	obj = json.loads(line)
	event = obj['event']
	if event == 'video_detail':
		return [obj['result']['item']]
	else:
		return []

def id_tag(line):
	obj = json.loads(line)
	tag = obj['VIDEO_TAGS']
	return [(obj['ID'],tag.split(','))]

def get_rdd_from_file():
	path = 'data/raw-video-title-tag.json'
	f = open(path)
	obj = json.loads(f.read())
	rdd = sc.parallelize(obj)
	rdd = rdd.map(id_tag)
	return rdd

video_rdd = get_rdd_from_file()

def video_to_tag(line):
	taglist = video_rdd.lookup(line)[0]
	l = []
	for i in taglist:
		if i:
			l += [(i,1)]
	return l

def save_to_file(l):
	f = open('log-analysis-tagcount.txt','w')
	for i in l:
		f.write(json.dumps(i)+'\n')
	f.close()


def read_from_log():
	path = 'log/user-log-20160523-mermaid1.txt'
	rdd = sc.textFile(path)
	print '1:'+ rdd.collect()
	rdd = rdd.flatMap(fit)
	print '2:'+ rdd.collect()
	rdd = rdd.flatMap(video_to_tag)
	print '3:'+ rdd.collect()
	rdd = rdd.reduceByKey(lambda a,b:a+b)
	rdd = rdd.sortBy(lambda a:a[1],False)
	l = rdd.collect()
	save_to_file(l)

if __name__ == '__main__':
	read_from_log()