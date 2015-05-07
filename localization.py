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
from apollo_lib import util

def getData(input_file,outfile,cluster_desc_file,original_input):
	clusters = {}
	cdf = open(cluster_desc_file,'r')
	lines = [line.strip() for line in cdf.readlines()]
	for line in lines:
		row = line.split()
		clusters[row[1]] = row[2:]
	cdf.close()
	del lines
	
	tid_meta = {}
        tid_content = {}
	#of = open(original_input,'r')
	#lines = [line.strip() for line in of.readlines()]
        lines = util.read_and_parse_tweets_from_file(original_input)
	for line in lines:
		d=line
		tid = str(d['id'])
                text = str(d['text'].encode('ascii','ignore'))
                tid_content[tid] = text
		user_info = d['user']
		if 'location' in user_info:
			tid_meta[tid]=user_info['location']
		else:
			tid_meta[tid]=''
            #except ValueError:
               # print "error"
	#of.close()
	del lines
	
	f = open(input_file,'r')
	lines = [line.strip() for line in f.readlines()]
	events = {}
	for line in lines:
		d = ast.literal_eval(line)
		key = d['kw_pair'][0]+'_'+d['kw_pair'][1]
		tids = d['tweet_ids']
                events[key]=[]
                for tid in tids:
                    events[key].append((tid_content[str(tid)],str(tid)))
	cachedStopWords=stopwords.words("english")
	#stop_words=cachedStopWords+['traffic','delay','weekend','accident','hour','spent','stuck','today','smoke','fire','stop','jam']
	stop_words=cachedStopWords
	grammar = r"""
	LOC: {<IN><DT>?<JJ>?<NN>*<PRP>?<CD>?<CC|POS>?<DT>?<JJ>?<NN>*<PRP>?<CD>?}   
	"""
	cp = nltk.RegexpParser(grammar)
	e_address={}
	e_desc={}
        e_count=0
	for e in events:
                e_count+=1
		e_address[e]=[]
		e_desc[e]=[]
		cityinfo = Counter()
		for d in events[e]:
			e_desc[e].append(d[0])
			text = string.lower(d[0].encode('ascii','ignore'))
			text = re.sub(r'http:(.*)|https:(.*)', '', text)
			text = re.sub(r'@([A-Za-z0-9_]+)', '', text)
			text = re.sub(r'&amp;', 'and', text)
			#for ch in """!~"#$%&()*+,-./:;<=>?@[\\]?_'`{|}?""":
			for ch in """":#%/;-?.()@!""":
				text = string.replace(text, ch,' ')
			tokens = nltk.word_tokenize(text)
			
			cityinfo[tid_meta[d[1]]]+=1
			rids = clusters[d[1]]
			for r in rids:
				cityinfo[r]+=1
			
			tagged = nltk.pos_tag(tokens)
			tree=cp.parse(tagged)
			filtered_tag = []
			for subtree in tree.subtrees():
				if subtree.label()=='LOC':
					for s in subtree:
						if s[1] in ('NN','CD') and s[0] not in stop_words:
							filtered_tag.append(s[0])
			address = '+'.join(filtered_tag[:3])
			clabel = cityinfo.most_common(1)
			clabel = clabel[0][0]			
			clabel = urllib.quote_plus(clabel.encode('utf-8'))
			if len(filtered_tag)>0:
				address=address+clabel
				format_add,lat,lng,status=json_localization.find_add(address)
				if status==1:
					try:
						e_address[e].append(format_add)
					except UnicodeEncodeError:
						pass
		printer = {}
		printer['pair'] = e
		printer['address_formatted'] = e_address[e]
		printer['latitude'] = lat
		printer['longitude'] = lng
		printer['description'] = e_desc[e]
		print >> outfile, printer
		if e_count>1:
			break

def main(argv):
        input_folder = argv[1]
        tid_meta = {}
        tid_content ={}
        #tweets = util.read_and_parse_tweets_from_file(input_folder+'/tweets.json')
        #print len(tweets)
        #of = open(input_folder+'/tweets.json','r')

        """
        lines = [line.strip() for line in of.readlines()]
        for line in lines:
            d=ast.literal_eval(line)
            tid = str(d['id'])
            text = str(d['text'])
            tid_content[tid] = text
            user_info = d['user']
            if 'location' in user_info:
                tid_meta[tid]=user_info['location']
            else:
                tid_meta[tid]=''
        of.close()
        del lines
        """

        fpath = input_folder + '/information_gain_files/'
        subdirectories = os.listdir(fpath)
        for fn1 in subdirectories:
            print "Localizing ",fn1
	    input_file = fpath + fn1 + '/pairs'
            output_file = fpath + fn1 + '/localized_events.txt' 
            dt = fn1.split('_')
	    cluster_desc =  input_folder + '/clustered_dated_files/' + dt[1]+'/desc'
	    original_input = input_folder + '/date_chunked_files/' + dt[1] 
	    outfile = open(output_file,'w')
	    getData(input_file,outfile,cluster_desc,original_input)
	    outfile.close()
	
if __name__ == "__main__":
	main(sys.argv)
        print "Qutting program"
