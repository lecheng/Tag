# coding=utf-8
import json
import time
import codecs
import numpy as np
from pyspark import SparkContext, SparkConf

appName = "count"
master = "local"

conf = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=conf)

remain_list = set([])
select_list = []
rdd1 = sc.textFile('data/normalized-hashtag-table.json')
rdd2 = sc.textFile('data/count.json')

def get_map(ll):
	m = {}
	l = []
	for i in ll:
		obj = json.loads(i)
		tags = obj['value']
		templ = []
		for tag in tags[:5]:
			templ += [tag[0]]
		m[obj['key']] = templ
		l += [obj['key']]
	return m,set(l)

hashtag_list = rdd1.collect()
tag_map,remain_list = get_map(hashtag_list)
print len(tag_map)
print 'remaining length:' + str(len(remain_list))


channeltag_list = rdd2.collect()
max_num = 1
flag_tag = []
flag_related_tag = []
while max_num!=0:
	max_num = 0
	for j in range(0,len(channeltag_list)):
		tag = json.loads(channeltag_list[j])['key']
		try:
			result = tag_map[tag]
		except Exception, e:
			pass
		if result:
			related_tags = result
			intersection = set(related_tags).intersection(remain_list)
			if len(intersection) > max_num:
				max_num = len(intersection)
				flag_tag = tag
				flag_related_tag = related_tags
	select_list += [flag_tag]
	try:
		channeltag_list.remove(flag_tag)
	except Exception, e:
		pass
	remain_list = remain_list - set(flag_related_tag)
print remain_list
print 'remaining length:' + str(len(remain_list))
print 'selected length' + str(len(set(select_list)))
for i in set(select_list):
	print i