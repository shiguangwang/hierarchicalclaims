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
	stops = set(nltk.corpus.stopwords.words())
	fpath = in_folder+'/clustered_dated_files/'+dt
        print fpath
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
		filtered_tokens = [w for w in filtered_tokens if len(w)>4]
		tid_desc[key] = filtered_tokens
	del lines
	in_file.close()
	return tid_desc,tid_content

def analyze_consolidated_tweets(data_dir):
    indir = 'consolidated_events'
    outdir= 'consolidated_events_token_frequency'
    consolidated_events_fn = os.listdir(os.path.join(data_dir, 
                                        indir))
    for fn in consolidated_events_fn:
        print fn
        tid_desc = {}
        tid_content = {}
        [tid_desc, tid_content] = getDesc(data_dir, fn.split('_')[1], tid_desc, 
                tid_content)
        token_frequency = []
        fi = open(os.path.join(data_dir, indir, fn))
        if not os.path.exists(os.path.join(data_dir, outdir)):
            os.makedirs(os.path.join(data_dir, outdir))
        fo = open(os.path.join(data_dir, outdir, fn.split('_')[1]), 'w')
        line = fi.readline().strip()
        elist = ast.literal_eval(line)
        for item in elist:
            dic = {}
            dic['signature'] = item['signature']
            tokens = Counter()
            for tweet_id in item['tweet_ids']:
                tokens += Counter(tid_desc[str(tweet_id)])
            dic['tokens'] = dict(tokens)
            token_frequency.append(dic)
        print >> fo, token_frequency


def analyze_tweets(data_dir, infogain_folder, token_frequency_folder, fi_name, fo_name, no_events):
        ''' 
        data_dir the root dir of the tweets.json
        infogain_folder the folder contains all the IG events.
        no_events is the top K interesting events.
        fo_name is the result name.
        fi_name is the IG standard file name.
        '''
	fpath = os.path.join(data_dir, infogain_folder)
        #event_file = open(in_folder+'/information_gain_files/events.txt','w')
        outfile = open(os.path.join(data_dir, token_frequency_folder, fo_name),'w')
	subdirectories = os.listdir(fpath)
	for fn1 in subdirectories:
                print fn1
		in_file = open(os.path.join(fpath, fn1, fi_name),'r')
                event_file = open(os.path.join(fpath, fn1, fo_name),'w')
                event_printer={}
		lines = [line.strip() for line in in_file.readlines()]
		if len(lines)>0:
			dates=fn1.split('_')
			tid_desc = {}
                        tid_content = {}
			tid_desc,tid_content = getDesc(data_dir,dates[1],tid_desc,tid_content)
                        for line in lines[:no_events]:
                                printer={}
				d = ast.literal_eval(line)
				event_name = d['kw_pair'][0]+'_'+d['kw_pair'][1]
                                event_printer['event']=event_name
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
				#event_counter=event_counter.most_common(10)
				#labels, values = zip(*event_counter)
				#indexes = np.arange(len(labels))
				#width = 0.5
				#plt.bar(indexes, values, width)
				#plt.xticks(indexes, labels,rotation=45)
				#plt.savefig(fpath+fn1+'/'+event_name+'.png')
				
		else:
			print fn1," has 0 events"
		in_file.close()
                event_file.close()
        outfile.close()

def summarize_events(in_folder, dir_name, fi_name, fo_name):
    print "Summarizing events into a single file for all days"
    fpath = in_folder+'/information_gain_files/'
    subdirectories = os.listdir(fpath)

    def cmp_date_str(date_str1, date_str2):
        from datetime import datetime
        dt1 = datetime.strptime(date_str1.split('_')[0], '%Y-%m-%d-%H')
        dt2 = datetime.strptime(date_str2.split('_')[0], '%Y-%m-%d-%H')
        if dt1 < dt2:
            return -1
        if dt1 > dt2:
            return 1
        return 0

    subdirectories.sort(cmp=cmp_date_str)
    outfile = open(os.path.join(in_folder, dir_name, fo_name),'w')
    total_list = []
    for fn in subdirectories:
        print fn
        e_list=[]
        in_file=open(os.path.join(fpath, fn, fi_name),'r')
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
	analyze_tweets(in_folder,no_events)
        summarize_events(in_folder)

def main2(data_dir):
    analyze_consolidated_tweets(data_dir)
	
if __name__ == "__main__":
#	main(sys.argv)
        main2(sys.argv[1])
	print "Quitting program."
