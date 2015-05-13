import glob,time
import os,re,sys
import string
import ast
import urllib
from sets import Set
from math import log,ceil
from datetime import datetime,timedelta
from collections import defaultdict
from decimal import *
import nltk
from nltk.corpus import stopwords
import json_localization
from collections import Counter
from nltk.tag.stanford import NERTagger
from apollo_lib import util

def most_common_element(lst):
    return max(set(lst), key=lst.count)

def getData(input_file,outfile,cluster_desc_file,original_input):
	st = NERTagger('classifiers/english.all.3class.distsim.crf.ser.gz','stanford-ner/stanford-ner.jar')
	localization_count=0
	clusters = {}
	cdf = open(cluster_desc_file,'r')
	lines = [line.strip() for line in cdf.readlines()]
	for line in lines:
		row = line.split()
		clusters[row[1]] = row[2:]
	cdf.close()
	del lines

	tid_content = {}
	lines = util.read_and_parse_tweets_from_file(original_input)
	for line in lines:
		d=line
		tid = str(d['id'])
		text = str(d['text'].encode('ascii','ignore'))
		tid_content[tid] = text
	del lines


	f = open(input_file,'r')
	lines = [line.strip() for line in f.readlines()]
	events = {}
	# dall = ast.literal_eval(lines[0])
	for line in lines:
		d = ast.literal_eval(line)
		tids = d['tweet_ids']
		key = '_'.join(d['kw_pair'])
		events[key]=[]
		for tid in tids:
			events[key].append((tid_content[str(tid)],str(tid)))

	e_address={}
	#e_desc={}
	e_count=0
	for e in events:
		e_count+=1
		e_desc=''
		#cityinfo = Counter()
		for d in events[e]:
			text = d[0].encode('ascii','ignore')
			text = re.sub(r'http:(.*)|https:(.*)', '', text)
			text = re.sub(r'@([A-Za-z0-9_]+)', '', text)
			text = re.sub(r'&amp;', 'and', text)
			for ch in """":#%/;-?.()@!',&_|[]$+*""":
				text = string.replace(text, ch,' ')
			e_desc += text + ' '
                ner_tags = []
                try:
    		    ner_tags.extend(st.tag(e_desc.split()))
                except UnicodeDecodeError:
                    print e_desc
                #ner_tags.extend(st.tag(e_desc.split()))
		#print ner_tags
		locs=[]
		for tags in ner_tags:
			#print tags[1]
			if tags[1]==u'LOCATION':
				locs.append(tags[0])	
		#print locs
		if len(locs)>0:
			localization_count+=1
		try:
			e_address[e]=most_common_element(locs)
		except ValueError:
			e_address[e]=''
		printer = {}
		printer['pair'] = e
		printer['address_formatted'] = e_address[e]
		print >> outfile, printer
		#if e_count>0:
		#	break
	ntot = len(events)
	print "Localized ",localization_count," out of ",ntot," events"
	return localization_count,ntot

def main(argv):
        input_folder = argv[1]
        lcount=0
        ecount=0
        fpath = input_folder + '/information_gain_files/'
        subdirectories = os.listdir(fpath)
        out_dir = os.path.join(input_folder, 'keyword_pair_localization')
        if not os.path.exists(out_dir):
            os.makedirs(out_dir)
        for fn1 in subdirectories:
			print "Localizing ",fn1
			input_file = os.path.join(fpath, fn1, 'pairs')
			output_file = os.path.join(out_dir, fn1.split('_')[1])
			dt = fn1.split('_')
			cluster_desc =  input_folder + '/clustered_dated_files/' + dt[1]+'/desc'
			original_input = input_folder + '/sliding_window_chunked_files/' + dt[1]
			outfile = open(output_file,'w')
			l,e=getData(input_file,outfile,cluster_desc,original_input)
			lcount+=l
			ecount+=e
			outfile.close()
	print "Total localized ",lcount," out of ",ecount," events"

if __name__ == "__main__":
	main(sys.argv)
	print "Qutting program"
