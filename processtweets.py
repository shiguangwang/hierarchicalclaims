import string
import ast
from sets import Set
from math import log, ceil
from nltk.tag import pos_tag
from nltk.corpus import stopwords
# from nltk import stem
import re
# import difflib
import sys
# import os
# from datetime import datetime, timedelta
# import os.path
from apollo_lib import util
# APOLLO_HOME = os.environ['APOLLO_HOME']

global intervals
stop = stopwords.words()
# lancaster = stem.lancaster.LancasterStemmer()
precision = 0.0001


def compareItems((w1, c1), (w2, c2)):
    if c1 > c2:
        return -1
    elif c1 == c2:
        return cmp(w1, w2)
    else:
        return 1

def info_gain(normal_set,anom_set,xi,first_total_intervals,second_total_intervals):
    try:
      a = len(normal_set[xi])
    except KeyError:
      a = 0
    try:
      b = len(anom_set[xi])
    except KeyError:
      b = 0
    c = first_total_intervals - a
    d = second_total_intervals - b
    Px1 = float(a+b)/float(a+b+c+d)
    Px0 = float(c+d)/float(a+b+c+d)
    Py1 = float(a+c)/float(a+b+c+d)
    Py0 = float(b+d)/float(a+b+c+d)
    Hy = (-1*(Py1*log(Py1,2))) + (-1*(Py0*log(Py0,2)))
    if c==0 and d==0:
      H = 2.0
    elif a==0 and b==0:
      H = 2.0
    else:
      Py1x1 = float(a)/float(a+b)
      Py0x1 = float(b)/float(a+b)
      Py1x0 = float(c)/float(c+d)
      Py0x0 = float(d)/float(c+d)
      EntPy1x1 = Py1x1*log(Py1x1,2) if not(Py1x1 ==0) else 0
      EntPy0x1 = Py0x1*log(Py0x1,2) if not(Py0x1 ==0) else 0
      EntPy1x0 = Py1x0*log(Py1x0,2) if not(Py1x0 ==0) else 0
      EntPy0x0 = Py0x0*log(Py0x0,2) if not(Py0x0 ==0) else 0
      H = ((-1*Px1)*(EntPy1x1 + EntPy0x1)) + ((-1*Px0)*(EntPy1x0 + EntPy0x0))
    IG = Hy - H
    return IG

def get_analysis_by_intervals(lines):
    line_count=0
    intervalno=0
    counts = {}
    score = {}
    pos = {}
    filtered_words = []
    tweets = []
    print >> sys.stderr, "Total number of lines :"+str(len(lines))
    cnt=0
    for line in lines:
      cnt+=1
      if cnt%100==0:
        print >> sys.stderr, "analyzing line:"+str(cnt)+"/"+str(len(lines))
      #line_count=line_count+1
      try:
        d = ast.literal_eval(line)
        org_text = util.get_tweet_text(d)
        tweet_id = util.get_tweet_id(d)

        #text = string.lower(d['text'].encode('ascii','ignore'))
        text = org_text.lower()

        #tweet_time = d['created_at'].split(' ')[3]
        # the tweet time in python date time format
        dname_dt =  util.get_tweet_created_at(d)
        # tweet time in string
        tweet_time = str(dname_dt)

        text = re.sub(r'http:(.*)|https:(.*)', '', text)
        #text = re.sub(r'rt', '', text)
        text = re.sub(r'@([A-Za-z0-9_]+)', '', text)

        for ch in """!~"#$%&()*+,-./:;<=>?@[\\]?_'`{|}?""":
          text = string.replace(text, ch,' ')
        #tokens = nltk.word_tokenize(text)
        tokens = text.split(' ')
        tokens = [i for i in tokens if i not in stop]
        #tokens = [lancaster.stem(i) for i in tokens]
        # getting rid of pos_tag
        POStag = pos_tag(tokens)
        # dummy just say every token is 'N'
        # POStag = map(lambda x: (x,'N'), tokens)


        for w in POStag:
          if (len(w[0])>5):
            filtered_words.append(w)
        line_count = line_count + 1
        tweets.append((org_text, tweet_id, tweet_time))
        if (line_count%intervals)==0 and line_count !=0:
          seen = {}
          for w in filtered_words:
            if not(seen.has_key(w[0])):
              pos[w[0]] = w[1]
              try:
                counts[w[0]].add(intervalno)
              except KeyError:
                counts[w[0]] = Set([intervalno])
              seen[w[0]]=1
            try:
              score[(w[0],intervalno)] = score[(w[0],intervalno)] + 1
            except KeyError:
              score[(w[0],intervalno)]=1
          filtered_words=[]
          intervalno = intervalno + 1
      except SyntaxError:
        pass
    seen = {}
    for w in filtered_words:
      if not(seen.has_key(w[0])):
        pos[w[0]] = w[1]
        try:
          counts[w[0]].add(intervalno)
        except KeyError:
          counts[w[0]] = Set([intervalno])
        seen[w[0]]=1
      try:
        score[(w[0],intervalno)] = score[(w[0],intervalno)] + 1
      except KeyError:
        score[(w[0],intervalno)]=1
    return counts,tweets,score,pos,line_count

def get_pair_info(first_set,second_set,word_list):
    normal_pair = {}
    anomalous_pair = {}
    pair_list = []
    n = len(word_list)
    for i in range(n):
      for j in range(i+1,n):
        x,y = word_list[i],word_list[j]
        pair_list.append((x,y))
        if first_set.has_key(x) and first_set.has_key(y):
          normal_pair[(x,y)] = first_set[x].intersection(first_set[y])
        else:
          normal_pair[(x,y)] = Set([])

        if second_set.has_key(x) and second_set.has_key(y):
          anomalous_pair[(x,y)] = second_set[x].intersection(second_set[y])
        else:
          anomalous_pair[(x,y)] = Set([])

    return normal_pair,anomalous_pair,pair_list

def word_select(w,first_set,second_set):
    if second_set.has_key(w):
      if first_set.has_key(w):
        if len(first_set[w])<len(second_set[w]):
          return True
        else:
          return False
      else:
        return True
    else:
      return False

def generate_output(file_2, file_1, first_output, second_output, third_output):
  # file_2 is present file
  # file_1 is previous file
  interval_period = intervals
  print >> sys.stderr, "Reading first interval file"
  ff1 = open(file_1)
  lines_final = [line.strip() for line in ff1.readlines()]
  ff1.close()
  print >> sys.stderr, "Read the first file, passing to get interval function"
  first_set,tweet_normal,first_score,first_pos,first_count = get_analysis_by_intervals(lines_final)
  print >> sys.stderr, "Reading second interval file"
  ff2 = open(file_2)
  lines = [line.strip() for line in ff2.readlines()]
  ff2.close()
  second_set,tweet_anomalous,second_score,second_pos,second_count = get_analysis_by_intervals(lines)

  del lines
  del lines_final

  word_list = []
  entropy_dic = {}
  for w in zip(first_set.iterkeys(),second_set.iterkeys()):
    if w[0] in word_list:
      pass
    else:
      word_list.append(w[0])
    if w[1] in word_list:
      pass
    else:
      word_list.append(w[1])
  first_total_intervals = int(ceil(float(first_count)/float(interval_period)))
  second_total_intervals = int(ceil(float(second_count)/float(interval_period)))
  print >> sys.stderr, "Finding entropy for each word"
  for w in word_list:
    entropy_dic[w] = info_gain(first_set,second_set,w,first_total_intervals,second_total_intervals)
  print >> sys.stderr, "Writing down first output"
  w_file = open(first_output,'w')
  rank_set = {}
  items = entropy_dic.items()
  items.sort(compareItems)
  if len(items)>0:
    #strg = "Word" + "\t" + "POS_TAG" + "\t" + "Count_Normal" + "\t" + "Count_Anom" + "\t" + "Entropy" + "\n"
    #w_file.write(strg)
    for i in range(len(items)):
      rank_set[items[i][0]] = len(items)-i
      if items[i][1]>0:
        c1,c2=0,0
        if first_set.has_key(items[i][0]):
          c1 = len(first_set[items[i][0]])
        if second_set.has_key(items[i][0]):
          c2 = len(second_set[items[i][0]])
        if first_pos.has_key(items[i][0]) and second_pos.has_key(items[i][0]):
          w_pos_tag = (first_pos[items[i][0]],second_pos[items[i][0]])
        elif first_pos.has_key(items[i][0]):
          w_pos_tag = first_pos[items[i][0]]
        elif second_pos.has_key(items[i][0]):
          w_pos_tag = second_pos[items[i][0]]

        strg = {'word': items[i][0],\
                'pos_tag': w_pos_tag,\
                'count_normal': c1,\
                'count_anom': c2,\
                'entropy': items[i][1]}
        #strg = str(items[i][0])+"\t"+str(w_pos_tag)+"\t"+str(c1)+"\t"+str(c2)+"\t"+ str(items[i][1])+"\n"
        #w_file.write(strg)
        print >> w_file, strg
  else:
      #print "\nNo match found for the keywords entered."
      pass
  w_file.close()
  #removing words below threshold
  word_list = []
  for k,v in entropy_dic.items():
    w_pos_tag_1,w_pos_tag_2='#Z','#Z'
    if first_pos.has_key(k) and second_pos.has_key(k):
        w_pos_tag_1 = first_pos[k]
        w_pos_tag_2 = second_pos[k]
    elif first_pos.has_key(k):
        w_pos_tag_1 = first_pos[k]
    elif second_pos.has_key(k):
        w_pos_tag_2 = second_pos[k]

    if v>precision and word_select(k,first_set,second_set) and (w_pos_tag_1.startswith('N') or w_pos_tag_2.startswith('N')):
      word_list.append(k)
  print >> sys.stderr, "Finding entropy for keyword pairs"
  pair_entropy={}
  normal_pair,anomalous_pair,pair_list = get_pair_info(first_set,second_set,word_list)

  #free the word_list variable
  del word_list

  for w in pair_list:
    if len(normal_pair[w]) != len(anomalous_pair[w]):
      pair_entropy[w] = info_gain(normal_pair,anomalous_pair,w,first_total_intervals,second_total_intervals)
  pair_list=[]
  for k,v in pair_entropy.items():
    if v>precision:
      pair_list.append(k)
  print >> sys.stderr, "Writing second output file"
  w_file = open(second_output,'w')
  #strg = "Word Pair" + "\t" + "Count_Normal" + "\t" + "Count_Anom" + "\t" + "Entropy" + "\n"
  #w_file.write(strg)
  items =pair_entropy.items()
  items.sort(compareItems)
  if len(items)>0:
    for i in range(len(items)):
      if items[i][1]>0:
        c1,c2=-1,-1
        if normal_pair.has_key(items[i][0]):
          c1 = len(normal_pair[items[i][0]])
        if anomalous_pair.has_key(items[i][0]):
          c2 = len(anomalous_pair[items[i][0]])

        strg = {'word_pair': items[i][0],\
                'count_normal': c1,\
                'count_anom': c2,\
                'entropy': items[i][1]}
        #strg = str(items[i][0]) + "\t" + str(c1) + "\t" + str(c2) + "\t" + str(items[i][1]) + "\n"
        #w_file.write(strg)
        print >> w_file, strg
  else:
      #print "\nNo match found for the keywords entered."
      pass
  w_file.close()
  tweet_file = open(third_output,'w')
  id_seen = []
  t_cnt=0

  items =pair_entropy.items()
  items.sort(compareItems)
  print >> sys.stderr, "Writing third output file"
  for w in items:
    if w[1]>precision:
      for z in tweet_anomalous:
        if not z[1] in id_seen:
          z_0 = string.lower(z[0].encode('ascii','ignore'))
          z_0 = re.sub(r'http:(.*)|https:(.*)', '', z_0)
          z_0 = re.sub(r'@([A-Za-z0-9_]+)', '', z_0)
          for ch in """!"~#$%&()*+,-./:;<=>?@[\\]?_'`{|}?""":
              z_0 = string.replace(z_0, ch,' ')
          z_0 = string.replace(z_0,'\n',' ')
          tokens_0 = z_0.split(' ')
          #tokens_0 = [lancaster.stem(i) for i in tokens_0]
          if ((w[0][0] in tokens_0) and (w[0][1] in tokens_0)):
            t_cnt += 1
            #tweet_strg = string.replace(str(z[0].encode('ascii','ignore')),'\n','')
            #strg = "Anomalous"+"\t"+str(z[2])+"\t"+str(w[0])+"\t"+str(pair_entropy[w[0]])+"\t"+str(tweet_strg)+"\n"
            id_seen.append(z[1])
            word_Pair = (w[0][0],w[0][1])
            normal_Count = len(normal_pair[word_Pair])
            anom_Count =  len(anomalous_pair[word_Pair])
            strg = {'time': z[2], 'text': z[0],'id': z[1],'score':w[1],'pair':word_Pair,'nCount':normal_Count,'aCount':anom_Count}
            print >> tweet_file, strg
  tweet_file.close()




def main():
  print >> sys.stderr, "Started Anomaly Analysis"

  global intervals

  present_file = sys.argv[1]
  previous_file = sys.argv[2]
  intervals = int(sys.argv[3])
  first_output = sys.argv[4]
  second_output = sys.argv[5]
  third_output = sys.argv[6]
  generate_output(present_file, previous_file, \
                  first_output, second_output, third_output)



if __name__ == "__main__":
  main()
