import datetime
import glob,time
import os,re,sys
import string
import ast
from sets import Set
import nltk
from collections import defaultdict,Counter
from decimal import *
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

def getDesc(in_folder,dt,tid_desc,tid_content):
	stops = set(nltk.corpus.stopwords.words("english"))
	fpath = in_folder+'/clustered_dated_files/'+dt
	in_file = open(fpath+'/head','r')
	lines = [line.strip() for line in in_file.readlines()]
	for line in lines:
		d = ast.literal_eval(line)
		key = str(d['id'])
                tid_content[key]=d['text']
                text = string.lower(d['text'].encode('ascii','ignore'))
		text = re.sub(r'http:(.*)|https:(.*)', '', text)
		text = re.sub(r'@([A-Za-z0-9_]+)', '', text)
		text = re.sub(r'&amp;', 'and', text)
		text = string.replace(text,'\n',' ')
		for ch in """":#%/;-?.()@!""":
			text = string.replace(text, ch,' ')
		tokens = nltk.word_tokenize(text)
		filtered_tokens = [w for w in tokens if not w in stops]
		filtered_tokens = [w for w in tokens if len(w)>4]
		tid_desc[key] = filtered_tokens
	del lines
	in_file.close()
	return tid_desc,tid_content

def analyze_tweets(in_folder,no_events):
	fpath = in_folder+'/information_gain_files/'
        #event_file = open(in_folder+'/information_gain_files/events.txt','w')
        outfile = open(in_folder+'/event_contents.txt','w')
	subdirectories = os.listdir(fpath)
	for fn1 in subdirectories:
                print fn1
		in_file = open(fpath+fn1+'/pairs','r')
                event_file = open(fpath+fn1+'/events.txt','w')
                event_printer={}
		lines = [line.strip() for line in in_file.readlines()]
		if len(lines)>0:
			dates=fn1.split('_')
			tid_desc = {}
                        tid_content = {}
			for dt in dates:
				tid_desc,tid_content = getDesc(in_folder,dt,tid_desc,tid_content)
                        for line in lines[:no_events]:
                                printer={}
				d = ast.literal_eval(line)
				event_name = d['kw_pair'][0]+'_'+d['kw_pair'][1]
                                event_printer['event']=event_name.encode('ascii','ignore')
                                printer['event']=event_name
				list_ids = d['tweet_ids']
				event_counter = Counter()
                                event_cluster=[]
				for ids in list_ids:
					event_counter+=Counter(tid_desc[str(ids)])
                                        event_cluster.append(tid_content[str(ids)])
                                event_printer['token_counter']=dict(event_counter)
                                printer['cluster']=event_cluster
                                print >> outfile,printer
                                print >> event_file, event_printer
				#Plotting histogram for top 10 words of the event
				event_counter=event_counter.most_common(10)
				labels, values = zip(*event_counter)
				indexes = np.arange(len(labels))
				width = 0.5
				plt.bar(indexes, values, width)
				plt.xticks(indexes, labels,rotation=45)
				plt.savefig(fpath+fn1+'/'+event_name+'.png')
				
		else:
			print fn1," has 0 events"
		in_file.close()
                event_file.close()
        outfile.close()

def summarize_events(in_folder):
    print "Summarizing events into a single file for all days"
    fpath = in_folder+'/information_gain_files/'
    subdirectories = os.listdir(fpath)
    outfile = open(in_folder+'/event_summary.txt','w')
    total_list = []
    for fn in subdirectories:
        print fn
        e_list=[]
        in_file=open(fpath+fn+'/events.txt','r')
        lines = [line.strip() for line in in_file.readlines()]
        for line in lines:
            try:
                #print line
                d=ast.literal_eval(line)
                e_list.append(d)
            except ValueError:
                pass
        total_list.append(e_list)
        print >> outfile,e_list
    outfile.close()
    #from pprint import pprint as pp
    #pp(total_list)
    return total_list

def main(argv):
	font = {'family':'normal','weight':'bold','size':8}
	matplotlib.rc('font', **font)   
	in_folder=argv[1]
        no_events=int(argv[2])
	#analyze_tweets(in_folder,no_events)
        summarize_events(in_folder)
	
if __name__ == "__main__":
	main(sys.argv)
	print "Quitting program."
