#!/usr/bin/python3

infile = open('mr43d.txt', 'r')
outfile = open('hadoop-final-3d-sorted.txt', 'w')

def sorter(infile, outfile):
	mylist = []
	for line in infile:
		data = line.strip().split('\t')
		key = int(data[0])
		values = data[1:]
		myline = []
		myline.append(key)
		if len(values) == 1:
			values2 = values[0].split('|')
			myline += values2
		else:
			myline += values
		mylist.append(myline)
	mylist = sorted(mylist, key = lambda elem:elem[0])
	for line in mylist:
		value = ""
		for j in line:
			value += str(j) + '\t'
		outvalue = value.strip()
		newline = '{0}\n'.format(outvalue)
		outfile.write(newline)
	outfile.close()
	infile.close()
	
sorter(infile, outfile)
