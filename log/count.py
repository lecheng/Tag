# coding=utf-8
#统计视频tag的种类和数量

import json
import time
import codecs
import numpy as np
from pyspark import SparkContext, SparkConf

appName = "count"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def extract_title(line):
	title = []
	if line['CHANNEL_TAGS']:
		title = line['CHANNEL_TAGS'].lower().replace(' ','').split(',')
	return title

path = 'data/raw-video-title-tag.json'
f = codecs.open(path,'r')
obj = json.loads(f.read())
rdd = sc.parallelize(obj)
rdd = rdd.flatMap(extract_title)
rdd = rdd.map(lambda a:(a,1))
rdd = rdd.reduceByKey(lambda a,b:a+b)
rdd = rdd.sortBy(lambda a:a[1],False)
l = rdd.collect()

f = codecs.open('count.json','w')
print(len(l))
for i in l:
	d = {"key":i[0],"value":i[1]}
	f.write(json.dumps(d) + '\n')
f.close()
