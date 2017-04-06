# coding=utf-8
import re
import sys,os
import urllib
import json
import codecs
from crawler import get_related_tags

filelist = ['better_life.txt','cute.txt','entertainment.txt','funny.txt','music.txt','sports.txt',]
typelist = ['better_life','cute','entertainment','funny','music','sports']
filenames = []
filenames = os.listdir('tags')

def get_staff_tags():
	wf = open('data/staffdata.json','w')
	staff_tags = []
	for filename in filenames:
		print filename
		path = 'tags/' + filename
		f = open(path,'r')
		lines = f.readlines()
		for line in lines:
			if line.strip():
				d = {}
				d['source'] = 'STAFF'
				d['tags'] = [filename.upper().split('.')[0],line.strip().upper()]
				wf.write(json.dumps(d) + '\n')
		f.close()
	wf.close()


def get_hashtag():
	hashtag_list = []
	for file in filelist:
		path = sys.path[0] + '/hashtag/' + file
		f = codecs.open(path,'r','utf-8')
		lines = f.readlines()
		for line in lines:
			hashtag_list+= [line.replace(' ','').strip().lower()]
	return hashtag_list

def get_flipboard_co_tags():
	#获取flipboard标签的共现标签
	tags = []
	count = 0
	for file in filelist:
		tagtype = file.split('.')[0]
		path = sys.path[0] + '/hashtag/' + file
		f = codecs.open(path,'r','utf-8')
		lines = f.readlines()
		for line in lines:
			#print 'line:'+line.strip()
			keyword1 = line.replace(' ','').strip()
			tags1 = get_related_tag_from_layer1(count,keyword1)
			if tags1:
				for tag1 in tags1:
					#print 'tag1:'+tag1
					keyword2 = tag1.replace(' ','').strip()
					tags2 = get_related_tag_from_layer2(count,keyword2)
					if tags2:
						for tag2 in tags2:
							#print 'tag2:'+tag2
							t = [line.strip(),tag1,tag2]
							d = {}
							d['source'] = 'FLIPBOARD'
							d['tags'] = t
							tags += [d]
					else:
						t = [line.strip(),tag1]
						d = {}
						d['source'] = 'FLIPBOARD'
						d['tags'] = t
						tags += [d]
			else:
				t = [line.strip()]
				d = {}
				d['source'] = 'FLIPBOARD'
				d['tags'] = t
				tags += [d]
		count += 1
	print len(tags)
	return tags

def get_related_tag_from_layer1(t_index,tagname):
	#根据第一次拓展结果拓展tag
	path = 'data/layer1/' + typelist[t_index] + '/' + tagname + '.txt'
	tags = []
	try:
		f = codecs.open(path,'r','utf-8')
		lines = f.readlines()
		for line in lines:
			if line:
				tags += [line.strip()]
		f.close()
	except Exception, e:
		print e
	return tags

def get_related_tag_from_layer2(t_index,tagname):
	#根据第二次拓展结果拓展tag
	path = 'data/layer2/' + typelist[t_index] + '/' + tagname + '.txt'
	tags = []
	try:
		f = codecs.open(path,'r','utf-8')
		lines = f.readlines()
		for line in lines:
			if line:
				tags += [line.strip()]
		f.close()
	except Exception, e:
		print e
	return tags

def flipborad_co_tags_to_file(tags):
	#将共现tag保存到文件
	path = 'data/data.json'
	f = codecs.open(path,'w','utf-8')
	for tag in tags:
		s = json.dumps(tag)
		#print s
		f.write(s+'\n')
	f.close()

def run():
	tags = get_flipboard_co_tags()
	flipborad_co_tags_to_file(tags)

if __name__ == '__main__':
	get_staff_tags()