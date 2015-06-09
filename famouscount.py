import fileinput
import numpy
UserCount = dict()
CountList = []

for line in fileinput.input():
	user,count = line.split('\t')
	UserCount[user] = int(count)
	CountList.append(int(count))
	
CountList = numpy.array(CountList)
count_std = numpy.std(CountList)
count_mean = numpy.mean(CountList)
#print count_std
#print count_mean

for key in UserCount:
	if UserCount[key] > count_mean+3*count_std:
		print('{}\t{}'.format(key, UserCount[key]))