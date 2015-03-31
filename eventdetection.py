#!/usr/bin/env python2.7
import string
import ast
# from sets import Set
from math import log, ceil
from nltk.tag import pos_tag
from nltk.corpus import stopwords
import re
import sys
from apollo_lib import util

# stop = stopwords.words()
precesion = 0.0001
# len_thresh = 4


def compare_items((w1, c1), (w2, c2)):
    if c1 > c2:
        return -1
    elif c1 == c2:
        return cmp(w1, w2)
    else:
        return 1


def p_logp(p1, p2):
    if p1 == 0:
        return 0
    if p2 == 0:
        return sys.float_info.max
    return p1 * log(p2, 2)


def cal_info_gain(norm_inclusive_intervals, anorm_inclusive_intervals,
                  norm_total_intervals, anorm_total_intervals):
    '''
              Y
           1      0
     X 0   c      d
       1   a      b
    '''
    a = norm_inclusive_intervals
    b = anorm_inclusive_intervals
    c = norm_total_intervals - a
    d = anorm_total_intervals - b

    sigma = float(a + b + c + d)
    py1 = float(a + c) / sigma
    py0 = float(b + d) / sigma
    hy = -1 * (p_logp(py1, py1) + p_logp(py0, py0))

    px0y0 = float(d) / sigma
    px1y0 = float(b) / sigma
    px0y1 = float(c) / sigma
    px1y1 = float(a) / sigma
    if c == 0 and d == 0:
        hxy = -1 * (p_logp(px1y0, float(b) / float(a + b))
                    + p_logp(px1y1, float(a) / float(a + b)))
    elif a == 0 and b == 0:
        hxy = -1 * (p_logp(px0y0, float(d) / float(c + d))
                    + p_logp(px0y1, float(c) / float(c + d)))
    else:
        hxy = -1 * (p_logp(px0y0, float(d) / float(c + d))
                    + p_logp(px1y0, float(b) / float(a + b))
                    + p_logp(px0y1, float(c) / float(c + d))
                    + p_logp(px1y1, float(a) / float(a + b)))
    return (hy - hxy)


def preprecess_text(text):
    text = text.lower()
    text = re.sub(r'http:(.*)|https:(.*)', '', text)
    text = re.sub(r'@([A-Za-z0-9_])+', '', text)
    for ch in '''!~"#$%&()*+,-./:;<=>?@[\\]?_'`{|}?''':
        text = string.replace(text, ch, ' ')
    return text


def extract_keywords(tweets_file):
    '''
    Get all keywords based on predefined rules from the raw tweet text file.
    '''
    stop = stopwords.words()
    len_thresh = 5
    popular_thresh = 5

    print >> sys.stderr, 'Start getting the keywords from {}...'\
        .format(tweets_file)
    fp = open(tweets_file)
    appearence = {}

    debug_progress = 0
    line_cnt = 0

    for line in fp:
        debug_progress = debug_progress + 1
        if debug_progress % 100 == 0:
            print >> sys.stderr, 'progress: {}'.format(str(debug_progress))
        line = line.strip()

        try:
            tweet = ast.literal_eval(line)
            tweet_id = util.get_tweet_id(tweet)
            tweet_text = util.get_tweet_text(tweet)
            tweet_text = preprecess_text(tweet_text)

            tokens = [i for i in tweet_text.split(' ') if i not in stop]
            postags = set([tag for tag in pos_tag(tokens)
                           if len(tag[0]) >= len_thresh
                           and tag[1].startswith('N')])

            for tag in postags:
                if tag[0] not in appearence.keys():
                    appearence[tag[0]] = []
                appearence[tag[0]].append(tweet_id)

            line_cnt = line_cnt + 1
        except:
            print >> sys.stderr, 'FOUND A TWEET WITH INVALID FORMAT!'
    appearence_trimmed = {}
    for k in appearence.keys():
        if len(appearence[k]) >= popular_thresh:
            appearence_trimmed[k] = appearence[k]
    return appearence_trimmed, line_cnt


def get_analysis_by_intervals(lines):
    line_no = 0
    appearence = {}
    orig_tweets = []
    print >> sys.stderr, "Total number of lines: {0}".format(str(len(lines)))
    cnt = 0
    for line in lines:
        cnt += 1
        if cnt % 100 == 0:
            print >> sys.stderr, 'analyzing line: {0}/{1}'\
                .format(str(cnt), str(len(lines)))

        try:
            tweet_dict = ast.literal_eval(line)
            orig_text = util.get_tweet_text(tweet_dict)
            tweet_id = util.get_tweet_id(tweet_dict)
            processed_text = preprecess_text(orig_text)
            tweet_time = str(util.get_tweet_created_at(tweet_dict))

            tokens = processed_text.split(' ')
            tokens = [i for i in tokens if i not in stop]
            postags = pos_tag(tokens)
            postags = set([tag for tag in postags
                           if len(tag[0]) > len_thresh
                           and tag[1].startswith('N')])

            orig_tweets.append((orig_text, tweet_id, tweet_time))

            for tag in postags:
                if tag[0] in appearence.keys():
                    appearence[tag[0]].add(line_no)
                else:
                    appearence[tag[0]] = set([line_no])
            line_no = line_no + 1
        except SyntaxError:
            pass
    return appearence, orig_tweets, line_no


def get_pair_info(norm_set, anorm_set, word_list):
    norm_pair = {}
    anorm_pair = {}
    pair_list = []
    n = len(word_list)
    for i in range(n):
        for j in range(i + 1, n):
            x, y = word_list[i], word_list[j]
            pair_list.append((x, y))
            if x in norm_set.keys() and y in norm_set.keys():
                norm_pair[(x, y)] = norm_set[x].intersection(norm_set[y])
            else:
                norm_pair[(x, y)] = set([])

            if x in anorm_set.keys() and y in anorm_set.keys():
                anorm_pair[(x, y)] = anorm_set[x].intersection(anorm_set[y])
            else:
                anorm_pair[(x, y)] = set([])
    return norm_pair, anorm_pair, pair_list


def is_word_considerable(word, norm_set, anorm_set):
    if word not in anorm_set.keys():
        return False
    if word not in norm_set.keys():
        return True
    if len(norm_set[word]) < len(anorm_set[word]):
        return True
    return False


def generate_output(cur_file, prev_file, keyword_output,
                    keyword_pair_output, tweets_output):
    print >> sys.stderr, "Reading the previous file..."
    fp = open(prev_file)
    lines = [line.strip() for line in fp]
    fp.close()
    print >> sys.stderr, "Read the previous file, analyzing..."
    prev_appearence, prev_tweets, pre_tweets_cnt = \
        get_analysis_by_intervals(lines)
    lines = None

    print >> sys.stderr, "Reading the current file..."
    fp = open(cur_file)
    lines = [line.strip() for line in fp]
    fp.close()
    print >> sys.stderr, "Read the current file, analyzing..."
    cur_appearence, cur_tweets, cur_tweets_cnt = \
        get_analysis_by_intervals(lines)
    lines = None

    word_list = set(cur_appearence.keys()).union(set(prev_appearence.keys()))
    ig_dic = {}

    print >> sys.stderr, "Finding entropy for each word..."
    for word in word_list:
        ig_dic[word] = cal_info_gain(prev_appearence, cur_appearence, word,
                                     pre_tweets_cnt, cur_tweets_cnt)

    print >> sys.stderr, "Writing down keyword output..."
    fp = open(keyword_output, 'w')
    rank_set = {}
    items = ig_dic.items()
    items.sort(compare_items)
    if len(items) > 0:
        for i in range(len(items)):
            rank_set[items[i][0]] = len(items) - i
            if items[i][1] > 0:
                c1, c2 = 0, 0
