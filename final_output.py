import datetime
import glob,time
import os,re,sys
import string
import ast
from sets import Set
import nltk
from collections import defaultdict,Counter
from decimal import *

def analyze_output(in_folder,fname):
	outfile=open(in_folder+'/'+fname[:-4]+'_output.txt','w')
	events={}
	in_file=open(in_folder+'/event_contents.txt','r')
	lines = [line.strip() for line in in_file.readlines()]
	for line in lines:
		d=ast.literal_eval(line)
		events[d['event']]=d['cluster']
	del lines
	in_file.close()
	
	in_file=open(in_folder+'/'+fname,'r')
	lines = [line.strip() for line in in_file.readlines()]
	if len(lines)>1:
		print "Output cannot have more than one line"
	else:
		line = lines[0]
		d=ast.literal_eval(line)
		for it in d:
			pair_1=it[0]
			pair_2=it[1]
			score=it[2]
			strg=pair_1+'\t'+pair_2+'\t'+str(score)
			print >> outfile,strg
			#output pair 1 cluster
			cluster=events[pair_1]
			for cl_it in cluster:
				cl_it=string.replace(cl_it,'\n','')
				print >> outfile, pair_1+'\t'+cl_it.encode('ascii','ignore')
			#output pair 2 cluster
			cluster=events[pair_2]
			for cl_it in cluster:
				cl_it=string.replace(cl_it,'\n','')
				print >> outfile, pair_2+'\t'+cl_it.encode('ascii','ignore')
			print >> outfile,''
	outfile.close()
	in_file.close()

def main(argv):
	in_folder=argv[1]
	fname=argv[2]
	analyze_output(in_folder,fname)
	
if __name__ == "__main__":
	main(sys.argv)
	print "Quitting program."
