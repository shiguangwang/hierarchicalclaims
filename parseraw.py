#!/usr/bin/env python2.7
from apollo_lib import util
from datetime import datetime
import sys
import json
import os
from clustering import do_clustering
from eventdetection import extract_keywords, compare_items, cal_info_gain


# This is the checkpoint name for chunking the raw file into 5000 lines.
checkpoint_tweet_chunked = 'tweet_chunked.checkpoint'
# This directory contains all the chunks of the raw file with <= 5000 lines.
chunk_file_dir = 'chunked_files'

# This is the checkpoint name for chunking the raw file by date.
checkpoint_tweet_date_chunked = 'tweet_date_chunked.checkpoint'
# This directory contains all the chunks of the raw file by creation date.
chunk_file_by_date_dir = 'date_chunked_files'

# This is the checkpoint name for clustering the dated raw files.
checkpoint_cluster = 'tweet_clustered.checkpoint'
# This is the directory contains all the clusters of the dated raw files.
clusters_dir = 'clustered_dated_files'
# The below are standard file names as defined in the clustering code.
plain_fn = 'plain'
head_fn = 'head'
plain_head_fn = 'plain_head'
source_fn = 'source'
desc_fn = 'desc'
author_cluster_fn = 'author_cluster'
output_tweets_fn = 'output_tweets'
tokens_fn = 'tokens'
length_fn = 'length'

# This is the checkpoint name for extracting the keywords from cluster heads.
checkpoint_extracting_keywords = 'extracting_keywords.checkpoint'
# This is the directory contains all the keywords for the cluster heads.
extracting_keywords_dir = 'keyword_file'
# Below is the standard file names defined for the extracted keywords.
appearence_fn = 'appearence'
tweet_cnt_fn = 'tweet_cnt'

# This is the checkpoint name for calculating IG for keyword pairs.
checkpoint_information_gain = 'information_gain.checkpoint'
# This is the directory contains all the keyword pair IG in consequtive slots.
information_gain_dir = 'information_gain_files'
# Standard file names
keyword_pair_fn = 'pairs'
# Threshold for top K most significant keyword pairs
information_gain_top_k = 40
# Customized dict keys
keyword_pair_key = 'kw_pair'
information_gain_key = 'info_gain'
tweet_ids_key = 'tweet_ids'


def processing_tweets(data_dir, tweets_file):
    if not os.path.exists(os.path.join(data_dir, checkpoint_tweet_chunked)):
        if not os.path.exists(os.path.join(data_dir, chunk_file_dir)):
            os.makedirs(os.path.join(data_dir, chunk_file_dir))
        _chunking_raw_file(data_dir, tweets_file)
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_tweet_chunked), 'w').close()

    if not os.path.exists(
            os.path.join(data_dir, checkpoint_tweet_date_chunked)):
        chunk_idx = 0
        if not os.path.exists(os.path.join(data_dir, chunk_file_by_date_dir)):
            os.makedirs(os.path.join(data_dir, chunk_file_by_date_dir))
        while os.path.exists(
                os.path.join(data_dir, chunk_file_dir, str(chunk_idx))):
            print chunk_idx
            _chunking_by_date_tweets(data_dir, chunk_idx)
            chunk_idx = chunk_idx + 1
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_tweet_date_chunked),
             'w').close()

    if not os.path.exists(os.path.join(data_dir, checkpoint_cluster)):
        # do the clustering
        if not os.path.exists(os.path.join(data_dir, clusters_dir)):
            os.makedirs(os.path.join(data_dir, clusters_dir))
        dated_chunk_files = os.listdir(
            os.path.join(data_dir, chunk_file_by_date_dir))
        for chunk_file in dated_chunk_files:
            dated_file_name = os.path.join(data_dir,
                                           chunk_file_by_date_dir,
                                           chunk_file)
            if os.path.isfile(dated_file_name):
                if not os.path.exists(os.path.join(data_dir,
                                                   clusters_dir,
                                                   chunk_file)):
                    os.makedirs(os.path.join(data_dir,
                                             clusters_dir,
                                             chunk_file))
                do_clustering(
                    dated_file_name,
                    os.path.join(data_dir, clusters_dir, chunk_file, plain_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file, head_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file,
                                 plain_head_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file,
                                 source_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file, desc_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file,
                                 author_cluster_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file,
                                 output_tweets_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file,
                                 tokens_fn),
                    os.path.join(data_dir, clusters_dir, chunk_file,
                                 length_fn))
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_cluster), 'w').close()

    if not os.path.exists(os.path.join(data_dir,
                                       checkpoint_extracting_keywords)):
        if not os.path.exists(os.path.join(data_dir, extracting_keywords_dir)):
            os.makedirs(os.path.join(data_dir, extracting_keywords_dir))
        dated_cluster_dirs = os.listdir(os.path.join(data_dir, clusters_dir))
        for date_dir in dated_cluster_dirs:
            cluster_head_file = os.path.join(data_dir,
                                             clusters_dir,
                                             date_dir,
                                             head_fn)
            appearence, tweet_cnt = extract_keywords(cluster_head_file)
            if not os.path.exists(os.path.join(data_dir,
                                               extracting_keywords_dir,
                                               date_dir)):
                os.makedirs(os.path.join(data_dir,
                                         extracting_keywords_dir,
                                         date_dir))
            fp = open(os.path.join(data_dir, extracting_keywords_dir, date_dir,
                                   appearence_fn), 'w')
            temp_str = json.JSONEncoder().encode(appearence)
            fp.write(temp_str)
            fp.close()

            fp = open(os.path.join(data_dir, extracting_keywords_dir, date_dir,
                                   tweet_cnt_fn), 'w')
            fp.write(str(tweet_cnt))
            fp.close()
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_extracting_keywords),
             'w').close()

    if not os.path.exists(os.path.join(data_dir,
                                       checkpoint_information_gain)):
        if not os.path.exists(os.path.join(data_dir, information_gain_dir)):
            os.makedirs(os.path.join(data_dir, information_gain_dir))
        dated_keywords_dir = os.listdir(os.path.join(data_dir,
                                        extracting_keywords_dir))
        dated_keywords_dir.sort(_compare_date_str)
        # TODO: only consecutive slots are considered here.
        for i in range(len(dated_keywords_dir) - 1):
            keyword_dir_base = os.path.join(data_dir, extracting_keywords_dir,
                                            dated_keywords_dir[i])
            keyword_dir_comp = os.path.join(data_dir, extracting_keywords_dir,
                                            dated_keywords_dir[i + 1])
            ig_dic, idx_dic = _gen_info_gain_for_two(keyword_dir_base,
                                                     keyword_dir_comp)
            ig_items = ig_dic.items()
            ig_items.sort(compare_items)
            ig_pair_dir =\
                dated_keywords_dir[i] + '-' + dated_keywords_dir[i + 1]
            if not os.path.exists(os.path.join(data_dir, information_gain_dir,
                                               ig_pair_dir)):
                os.makedirs(os.path.join(data_dir, information_gain_dir,
                                         ig_pair_dir))
            cnt = 0
            fp = open(os.path.join(data_dir, information_gain_dir,
                                   ig_pair_dir, keyword_pair_fn), 'w')
            for item in ig_items:
                if cnt > information_gain_top_k:
                    break
                dic_to_write = {}
                dic_to_write[keyword_pair_key] = item[0]
                dic_to_write[information_gain_key] = item[1]
                dic_to_write[tweet_ids_key] = idx_dic[item[0]]
                fp.write(json.JSONEncoder().encode(dic_to_write) + '\n')
            fp.close()
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_information_gain), 'w').close()


def _gen_info_gain_for_two(keyword_dir_base, keyword_dir_comp):
    keyword_set_base = json.load(open(os.path.join(keyword_dir_base,
                                                   appearence_fn)))
    keyword_set_comp = json.load(open(os.path.join(keyword_dir_comp,
                                                   appearence_fn)))

    base_total_intervals_len =\
        int(open(os.path.join(keyword_dir_base,
                              tweet_cnt_fn)).readline().strip())
    comp_total_intervals_len = \
        int(open(os.path.join(keyword_dir_comp,
                              tweet_cnt_fn)).readline().strip())

    key_list = [k for k in set(keyword_set_base.keys())
                .union(set(keyword_set_comp.keys()))]
    ig_dic = {}
    idx_dic = {}
    for i in range(len(key_list)):
        for j in range(i + 1, len(key_list)):
            pair = (key_list[i], key_list[j])
            if not (pair[0] in keyword_set_base.keys()
                    and pair[1] in keyword_set_base.keys()):
                base_inclusive_intervals = set([])
            else:
                base_inclusive_intervals = set(keyword_set_base[pair[0]])\
                    .intersection(set(keyword_set_comp[pair[1]]))
            if not (pair[0] in keyword_set_comp.keys()
                    and pair[1] in keyword_set_comp.keys()):
                comp_inclusive_intervals = set([])
            else:
                comp_inclusive_intervals = set(keyword_set_comp[pair[0]])\
                    .intersection(set(keyword_set_comp[pair[1]]))
            idx_dic[pair] = [i for i in comp_inclusive_intervals]
            ig_dic[pair] = cal_info_gain(len(base_inclusive_intervals),
                                         len(comp_inclusive_intervals),
                                         base_total_intervals_len,
                                         comp_total_intervals_len)
    return ig_dic, idx_dic


def _compare_date_str(date_str1, date_str2):
    dt1 = datetime.strptime(date_str1, '%Y-%m-%d')
    dt2 = datetime.strptime(date_str2, '%Y-%m-%d')
    return cmp(dt1.toordinal(), dt2.toordinal())


def _chunking_raw_file(data_dir, tweets_file):
    chunk_idx = 0
    chunk_size = 5000
    chunk_file_name = os.path.join(
        data_dir, chunk_file_dir, str(chunk_idx))
    chunk_file_fp = open(chunk_file_name, 'w')
    temp_idx = 0
    tweets_fp = open(os.path.join(data_dir, tweets_file))
    for line in tweets_fp:
        temp_idx = temp_idx + 1
        chunk_file_fp.write(line)
        if temp_idx == chunk_size:
            print chunk_idx
            chunk_file_fp.close()
            chunk_idx = chunk_idx + 1
            chunk_file_name = os.path.join(
                data_dir, chunk_file_dir, str(chunk_idx))
            chunk_file_fp = open(chunk_file_name, 'w')
            temp_idx = 0
    chunk_file_fp.close()


def _chunking_by_date_tweets(data_dir, chunk_index):
    tweets = util.read_and_parse_tweets_from_file(
        os.path.join(data_dir, chunk_file_dir, str(chunk_index)))

    date_format = '%a %b %d %H:%M:%S +0000 %Y'

    tweet_dict = {}

    for tweet in tweets:
        cur_time = datetime.strptime(tweet['created_at'], date_format)
        cur_time_str = str(cur_time.date())
        if cur_time_str not in tweet_dict.keys():
            tweet_dict[cur_time_str] = []
        tweet_dict[cur_time_str].append(tweet)

    for date_key in tweet_dict.keys():
        output_file_name = os.path.join(
            data_dir, chunk_file_by_date_dir, date_key)
        ofp = open(output_file_name, 'a')
        for tweet in tweet_dict[date_key]:
            line = json.JSONEncoder().encode(tweet)
            ofp.write(line + '\n')
        ofp.close()


if __name__ == '__main__':
    processing_tweets(sys.argv[1], sys.argv[2])
