# coding=utf-8
#统计一天内更新tag的数量
import json
import time
import codecs
import numpy as np
from pyspark import SparkContext, SparkConf

appName = "count"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

def parse(line):
	obj = json.loads(line)
	l = []
	for key,value in obj.iteritems():
		l += [(key,value)]
	print l
	return l

rdd = sc.textFile('data/update-tag-0524.json')
rdd = rdd.flatMap(parse)
rdd = rdd.reduceByKey(lambda a,b:a+b)
rdd = rdd.sortBy(lambda a:a[1],False)
l = rdd.collect()

f = codecs.open('update-tag-0524.txt','w')
for i in l:
	f.write(str(i)+'\n')
f.close()
