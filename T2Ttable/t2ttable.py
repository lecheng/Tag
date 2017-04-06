# coding=utf-8
from optparse import OptionParser
import json
import codecs
import re
import sys

filelist = ['better_life.txt','cute.txt','entertainment.txt','funny.txt','music.txt','sports.txt']
def get_hashtag():
	#获取hashtag列表
	hashtag_list = []
	for file in filelist:
		path = sys.path[0] + '/hashtag/' + file
		f = codecs.open(path,'r','utf-8')
		lines = f.readlines()
		for line in lines:
			hashtag_list+= [line.replace(' ','').strip().lower()]
	return hashtag_list

def read_lines(path):
	#按行读取文件
	f = open(path,'r')
	lines = f.readlines()
	for line in lines:
		#print line
		yield line

def save_table_to_file(tag_tables,path):
	#保存表到文件
	f = open(path,'w')
	for key,value in tag_tables.items():
		d = {}
		d['value'] = value
		d['key'] = key.replace(' ','')
		f.write(json.dumps(d) + '\n')
	f.close()

def pre(tags):
	#对原始数据进行预处理 tag变小写去空格
	pred_tags = []
	for tag in tags:
		pattern = '.*[^a-zA-Z0-9_\s\'-.&].*'
		if not re.match(pattern,tag.lower()):
			#print tag
			pred_tags += [tag.lower().replace(' ','')]
	return list(set(pred_tags))

def getdict(path):
	#获取共现tag字典
	tagdict = {}
	for i in read_lines(path):
		jsonobj = json.loads(i)
		tags = jsonobj['tags']
		source = jsonobj['source']
		templist = []
		templist = get_co_list(pre(tags))
		if templist:
			for j in templist:
				if tagdict.has_key(j):
					tagdict[j] += 1
				else:
					tagdict[j] = 1
	print 'tagdict size:' + str(len(tagdict))
	return tagdict

def get_co_list(l):
	#获取列表内元素的共现列表
	co_list = []
	for i in range(0,len(l)):
		for j in range(i,len(l)):
			s = frozenset([l[i],l[j]])
			co_list += [s]
	return co_list

def get_table(path):
	#获取表
	tag_tables = {}
	tagdict = {}
	tagdict = getdict(path)
	for key,value in tagdict.items():
		templist = list(key)
		valuelist = []
		if len(templist)==1:
			tag = templist[0]
			if tag_tables.has_key(tag):
				pass
			else:
				tag_tables[tag] = []
		elif len(templist)==2:
			tag1 = templist[0]
			tag2 = templist[1]
			if tag_tables.has_key(tag1):
				d = (tag2,value)
				tag_tables[tag1] += [d]
			else:
				d = (tag2,value)
				tag_tables[tag1] = [d]
			if tag_tables.has_key(tag2):
				d = (tag1,value)
				tag_tables[tag2] += [d]
			else:
				d = (tag1,value)
				tag_tables[tag2] = [d]
	print 'table length: ' + str(len(tag_tables))
	# print tag_tables
	return tag_tables

def normalize_table(tag_tables):
	#归一化倒排表
	print 'Start normalizing...'
	for key,value in tag_tables.items():
		count = 0.0
		normalize_value = []
		for item in value:
			count += float(item[1])
		for item in value:
			d = (item[0],item[1]/count)
			normalize_value += [d]
		tag_tables[key] = normalize_value
	print 'End normalizing...'
	return tag_tables

def filter_by_hashtag(tags,hashtag):
	#筛选出tag列表中的hashtag
	new_list = []
	for tag in tags:
		if tag[0] in hashtag:
			new_list += [tag]
	return new_list

def get_hashtag_table(table):
	#对table进行hashtag的筛选
	hashtag = get_hashtag()
	new_table = {}
	for key,value in table.items():
		if key not in hashtag:
			continue
		hash_value = filter_by_hashtag(value,hashtag)
		new_table[key] = hash_value
	return new_table

if __name__ == '__main__':
	optparser = OptionParser()
	optparser.add_option('-f','--inputFile',
		dest='input',
		help='filename of co-tag json file',
		default=None)
	optparser.add_option('-i','--isHash',
		dest='ishash',
		help='if filter by hashtag or not',
		default=True)

	(options,args) = optparser.parse_args()

	inFile = None
	if options.ishash == 'False':
		flag = False
	else:
		flag = options.ishash
	if options.input is None:
		inFile = sys.stdin
	elif options.input is not None:
		table = get_table(options.input)
	else:
		print 'No dataset filename specified, system with exit\n'
		sys.exit('System will exit')

	if flag:
		table = get_hashtag_table(table)
		table = normalize_table(table)
		save_table_to_file(table,path='normalized-hashtag-table.json')
	else:
		table = normalize_table(table)
		save_table_to_file(table,path='normalized-table.json')