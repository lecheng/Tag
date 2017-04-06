import fileinput
import json
import time

def get_data1(path='newvectors.txt'):
	with open(path, 'r') as f:
		vectors = []
		for line in f:
			vectors += [line]
	print len(vectors)
	return vectors

def get_data2(path='newvectors.txt'):
	vectors = []
	for line in open(path,'r').readlines():
		vectors += [line]
	print len(vectors)
	return vectors

def get_data3(path='newvectors.txt'):
	vectors = []
	for line in open(path):
		vectors += [line]
	print len(vectors)
	return vectors

def get_data4(path='newvectors.txt'):
	vectors = []
	for line in fileinput.input([path]):
		vectors += [line]
	print len(vectors)
	return vectors

def run():
	t1 = time.time()
	get_data1()
	t2 = time.time()
	print 'Methold1 time consumption:' + str(t2-t1) + 's'
	get_data2()
	t3 = time.time()
	print 'Methold2 time consumption:' + str(t3-t2) + 's'
	get_data3()
	t4 = time.time()
	print 'Methold3 time consumption:' + str(t4-t3) + 's'
	get_data4()
	t5 = time.time()
	print 'Methold4 time consumption:' + str(t5-t4) + 's'

if __name__ == '__main__':
	run()