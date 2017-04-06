# coding=utf-8
import re
import sys,os
import urllib
import json
import codecs #tag中存在特殊字符

filelist = ['better_life.txt','cute.txt','entertainment.txt','funny.txt','music.txt','sports.txt']
typelist = ['better_life','cute','entertainment','funny','music','sports']

def get_layer1():
	#获取根据hashtag拓展的tag
	tags = []
	for file in filelist:
		tagtype = file.split('.')[0]
		path = sys.path[0] + '/hashtag/' + file
		f = codecs.open(path,'r','utf-8')
		lines = f.readlines()
		for line in lines:
			keyword = line.replace(' ','').strip()
			tags = get_related_tags(keyword)
			if tags:
				save_to_layer(keyword,tags,tagtype=tagtype,layer='layer1')

def get_layer2():
	#获取根据第一次拓展的tag再进行拓展的tag
	tags = []
	for t in typelist:
		print t
		filelist = os.listdir(sys.path[0] + '/data/layer1/' + t)
		if filelist:
			keywords = []
			for file in filelist:
				path = sys.path[0] + '/data/layer1/' + t + '/' + file
				print ("extrating" + path)
				f = codecs.open(path,'r','utf-8')
				lines = f.readlines()
				for line in lines:
					keyword = line.replace(' ','').strip()
					if keyword not in keywords:
						keywords += [keyword]
			for k in keywords:
				tags = get_related_tags(k)
				if tags:
					save_to_layer(k,tags,tagtype=t,layer='layer2')


def save_to_layer(parenttag,tags,tagtype='music',layer='layer1'):
	#保存结果到文件中
	path = sys.path[0] + '/data/' + layer + '/' + tagtype + '/' + parenttag + '.txt'
	f = codecs.open(path,'w','utf-8')
	for tag in tags:
		f.write(tag + '\n')
	f.close()

def get_related_tags(keyword,save=True):
	#根据flipboard接口获取相关tag
	url = "https://flipboard.com/api/social/recommendedTopics?topic=" + keyword
	print url
	try:
		reponse = urllib.urlopen(url)
		html = reponse.read()
		jsondata = json.loads(html)
		tags = []
		if jsondata['code'] == 200 and len(jsondata['results']) !=0:
			results = jsondata['results']
			for result in results:
				print result['title']
				tags += [result['title']]
		return tags
	except Exception, e:
		print e

