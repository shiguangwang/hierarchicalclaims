#!/usr/bin/env python2.7
from apollo_lib import util
from datetime import datetime
import sys
import json
import os
from clustering import do_clustering
from eventdetection import extract_keywords, compare_items, cal_info_gain
from data_analyzer import analyze_tweets, summarize_events
from data_analyzer import analyze_consolidated_tweets
from caldist import method, parse_file, cal_dist_perday
from consolidation import consolidating


# This is the checkpoint name for chunking the raw file into 5000 lines.
checkpoint_tweet_chunked = 'tweet_chunked.checkpoint'
# This directory contains all the chunks of the raw file with <= 5000 lines.
chunk_file_dir = 'chunked_files'

# This is the checkpoint name for chunking the raw file by date.
checkpoint_tweet_date_chunked = 'tweet_date_chunked.checkpoint'
# This directory contains all the chunks of the raw file by creation date.
chunk_file_by_date_dir = 'date_chunked_files'
# This is the time slot length in hours
chunk_slot_in_hours = 6   # current the slot length is 6 hours


# This is the new chunking scheme specifically for event tracking. We call it
# the Sliding_Window_Chunking_Scheme
checkpoint_sliding_window_chunked = 'sliding_window_chunked.checkpoint'
checkpoint_comparing_windows_conf = 'comparing_window_conf.checkpoint'
sliding_window_dir = 'sliding_window_chunked_files'
comparing_windows_conf_dir = 'comparing_window_conf'
window_length_in_hours = 24  # We use 1 day as the window length
window_length_in_slots = window_length_in_hours / chunk_slot_in_hours

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
information_gain_top_k = 30
information_gain_thresh = 0
# Customized dict keys
keyword_pair_key = 'kw_pair'
information_gain_key = 'info_gain'
tweet_ids_key = 'tweet_ids'


# This is the checkpoint name for extracting token frequencies for events.
checkpoint_token_frequency = 'token_frequency.checkpoint'
# This is the directory contains all the token frequencies
token_frequency_dir = 'token_frequency'
# Standard file names
token_frequency_fn = 'token_frequency'
token_summary_fn = 'event_token_summary'
# Threshold for top K most significant events
token_frequency_top_k = information_gain_top_k
# json file names
event = 'event'
tokens = 'token_counter'

# This is the checkpoint for calculating the distance between events.
checkpoint_cal_dist = 'cal_dist.checkpoint'
cal_dist_dir = 'cal_dist'

# This is the checkpoint for cosolidating events.
checkpoint_consolidation = 'consolidation.checkpoint'
consolidation_dir = 'consolidated_events'
consolidation_signature_key = 'signature'
consolidation_tweets_set_key = 'tweet_ids'
consolidation_thresh = 0.85
consolidation_alg = 'jacard'

# Thi sis the extracking token frequency for consolidated events.
checkpoint_consolidated_events_token_frequency =\
    'cosolidated_token_frequency.checkpoint'


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
            _chunking_by_date_and_hour_tweets(data_dir, chunk_idx)
            chunk_idx = chunk_idx + 1
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_tweet_date_chunked),
             'w').close()

    if not os.path.exists(
            os.path.join(data_dir, checkpoint_sliding_window_chunked)):
        if not os.path.exists(os.path.join(data_dir, sliding_window_dir)):
            os.makedirs(os.path.join(data_dir, sliding_window_dir))
        _grouping_dated_chunks_to_sliding_window(data_dir)
        # Create the checkpoint marker
        open(os.path.join(data_dir, checkpoint_sliding_window_chunked),
             'w').close()

    if not os.path.exists(
            os.path.join(data_dir, checkpoint_comparing_windows_conf)):
        if not os.path.exists(os.path.join(data_dir,
                                           comparing_windows_conf_dir)):
            os.makedirs(os.path.join(data_dir, comparing_windows_conf_dir))
        _comparing_windows_conf(data_dir)
        open(os.path.join(data_dir, checkpoint_comparing_windows_conf),
             'w').close()
        print >> sys.stderr, "comparing windows_conf finished."

    if not os.path.exists(os.path.join(data_dir, checkpoint_cluster)):
        # do the clustering
        print >> sys.stderr, "clustering..."
        if not os.path.exists(os.path.join(data_dir, clusters_dir)):
            os.makedirs(os.path.join(data_dir, clusters_dir))
        dated_chunk_files = os.listdir(
            os.path.join(data_dir, sliding_window_dir))
        for chunk_file in dated_chunk_files:
            dated_file_name = os.path.join(data_dir,
                                           sliding_window_dir,
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
        print >> sys.stderr, "extracting keywords..."
        if not os.path.exists(os.path.join(data_dir, extracting_keywords_dir)):
            os.makedirs(os.path.join(data_dir, extracting_keywords_dir))
        dated_cluster_dirs = os.listdir(os.path.join(data_dir, clusters_dir))
        for date_dir in dated_cluster_dirs:
            print >> sys.stderr, date_dir
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

        comparing_windows_dir = os.listdir(os.path.join(data_dir,
                                           comparing_windows_conf_dir))
        for fn in comparing_windows_dir:
            tokens = fn.split("_")
            keyword_dir_base = os.path.join(data_dir, extracting_keywords_dir,
                                            tokens[0])
            keyword_dir_comp = os.path.join(data_dir, extracting_keywords_dir,
                                            tokens[1])
            ig_pair_dir = fn
            print >> sys.stderr, ig_pair_dir
            ig_dic, idx_dic = _gen_info_gain_for_two(keyword_dir_base,
                                                     keyword_dir_comp)
            ig_items = ig_dic.items()
            ig_items.sort(compare_items)
            if not os.path.exists(os.path.join(data_dir, information_gain_dir,
                                               ig_pair_dir)):
                os.makedirs(os.path.join(data_dir, information_gain_dir,
                                         ig_pair_dir))
            cnt = 0
            fp = open(os.path.join(data_dir, information_gain_dir,
                                   ig_pair_dir, keyword_pair_fn), 'w')
            for item in ig_items:
                cnt = cnt + 1
                if cnt > information_gain_top_k:
                    break
                # if item[1] <= information_gain_thresh:
                #    break
                dic_to_write = {}
                dic_to_write[keyword_pair_key] = item[0]
                dic_to_write[information_gain_key] = item[1]
                dic_to_write[tweet_ids_key] = idx_dic[item[0]]
                fp.write(json.JSONEncoder().encode(dic_to_write) + '\n')
            fp.close()
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_information_gain), 'w').close()
    if not os.path.exists(os.path.join(data_dir, checkpoint_token_frequency)):
        if not os.path.exists(os.path.join(data_dir, token_frequency_dir)):
            os.makedirs(os.path.join(data_dir, token_frequency_dir))
        analyze_tweets(data_dir, information_gain_dir, token_frequency_dir,
                       keyword_pair_fn, token_frequency_fn,
                       token_frequency_top_k)
        summarize_events(data_dir, token_frequency_dir, token_frequency_fn,
                         token_summary_fn)
        # Create the checkpoint marker file
        open(os.path.join(data_dir, checkpoint_token_frequency), 'w').close()
    if not os.path.exists(os.path.join(data_dir, checkpoint_cal_dist)):
        if not os.path.exists(os.path.join(data_dir, cal_dist_dir)):
            os.makedirs(os.path.join(data_dir, cal_dist_dir))
        dlist = parse_file(os.path.join(data_dir, token_frequency_dir,
                           token_summary_fn))
        for i in range(len(dlist)):
            for alg in method.keys():
                results = cal_dist_perday(dlist[i], alg)
                fo = open(os.path.join(data_dir, cal_dist_dir,
                                       'd' + str(i + 1) + '_' + alg), 'w')
                print >> fo, results
        # Create the checkpoint marker
        open(os.path.join(data_dir, checkpoint_cal_dist), 'w').close()
    if not os.path.exists(os.path.join(data_dir, checkpoint_consolidation)):
        if not os.path.exists(os.path.join(data_dir, consolidation_dir)):
            os.makedirs(os.path.join(data_dir, consolidation_dir))
        _consolidating(data_dir)
        open(os.path.join(data_dir, checkpoint_consolidation), 'w').close()
    if not os.path.exists(
        os.path.join(data_dir,
                     checkpoint_consolidated_events_token_frequency)):
        analyze_consolidated_tweets(data_dir)
        open(os.path.join(data_dir,
             checkpoint_consolidated_events_token_frequency), 'w').close()


def _consolidating(data_dir):
    pair_dir = os.listdir(os.path.join(data_dir, information_gain_dir))

    def local_cmp(item1, item2):
        token1 = item1.split('_')[0]
        token2 = item2.split('_')[0]
        date_format = '%Y-%m-%d-%H'
        time1 = datetime.strptime(token1, date_format)
        time2 = datetime.strptime(token2, date_format)
        if time1 < time2:
            return -1
        if time1 > time2:
            return 1
        return 0
    pair_dir.sort(cmp=local_cmp)

    distance_fn = os.listdir(os.path.join(data_dir, cal_dist_dir))
    selected_alg_fn = []
    for item in distance_fn:
        if item.split('_')[1] == consolidation_alg:
            selected_alg_fn.append(item)

    def local_cmp1(item1, item2):
        idx1 = int(item1.split('_')[0][1:])
        idx2 = int(item2.split('_')[0][1:])
        return idx1 - idx2
    selected_alg_fn.sort(cmp=local_cmp1)

    assert(len(pair_dir) == len(selected_alg_fn))

    num_slots = len(pair_dir)
    for i in range(num_slots):
        print >> sys.stderr, pair_dir[i]
        raw_pairs_fn = os.path.join(data_dir, information_gain_dir,
                                    pair_dir[i], keyword_pair_fn)
        distance_scores_fn = os.path.join(data_dir, cal_dist_dir,
                                          selected_alg_fn[i])
        consolidated_pairs = consolidating(raw_pairs_fn, distance_scores_fn,
                                           consolidation_thresh)
        fo = open(os.path.join(data_dir, consolidation_dir, pair_dir[i]),
                  'w')
        print >> fo, consolidated_pairs
        fo.close()


def _gen_info_gain_for_two(keyword_dir_base, keyword_dir_comp):
    keyword_set_base = json.load(open(os.path.join(keyword_dir_base,
                                                   appearence_fn)))
    keyword_set_comp = json.load(open(os.path.join(keyword_dir_comp,
                                                   appearence_fn)))

    base_total_intervals_len =\
        int(open(os.path.join(keyword_dir_base,
                              tweet_cnt_fn)).readline().strip())
    print >> sys.stderr, 'base_total_intervals_len = '\
        + str(base_total_intervals_len)

    comp_total_intervals_len = \
        int(open(os.path.join(keyword_dir_comp,
                              tweet_cnt_fn)).readline().strip())
    print >> sys.stderr, 'comp_total_intervals_len = '\
        + str(comp_total_intervals_len)

    key_list = [k for k in set(keyword_set_base.keys())
                .union(set(keyword_set_comp.keys()))]
    print >> sys.stderr, 'key_list length is {}'.format(len(key_list))
    ig_dic = {}
    idx_dic = {}
    pair_list = []
    for i in range(len(key_list)):
        for j in range(i + 1, len(key_list)):
            pair = (key_list[i], key_list[j])
            pair_list.append(pair)
    print >> sys.stderr,\
        'extracted kw pairs done. {} pairs exracted.'.format(len(pair_list))

    cnt = 0
    for pair in pair_list:
        cnt = cnt + 1
        if (cnt % 10000) == 0:
            print >> sys.stderr,\
                'processed {0} / {1}'.format(cnt, len(pair_list))
        if not (pair[0] in keyword_set_base.keys()
                and pair[1] in keyword_set_base.keys()):
            base_inclusive_intervals = set([])
        else:
            base_inclusive_intervals = set(keyword_set_base[pair[0]])\
                .intersection(set(keyword_set_base[pair[1]]))
        if not (pair[0] in keyword_set_comp.keys()
                and pair[1] in keyword_set_comp.keys()):
            comp_inclusive_intervals = set([])
        else:
            comp_inclusive_intervals = set(keyword_set_comp[pair[0]])\
                .intersection(set(keyword_set_comp[pair[1]]))
        if len(base_inclusive_intervals) > len(comp_inclusive_intervals):
            continue
        idx_dic[pair] = [item for item in comp_inclusive_intervals]
        ig_dic[pair] = cal_info_gain(len(base_inclusive_intervals),
                                     len(comp_inclusive_intervals),
                                     base_total_intervals_len,
                                     comp_total_intervals_len)
    return ig_dic, idx_dic


def _compare_date_str(date_str1, date_str2):
    dt1 = datetime.strptime(date_str1, '%Y-%m-%d-%H')
    dt2 = datetime.strptime(date_str2, '%Y-%m-%d-%H')
    if dt1 < dt2:
        return -1
    if dt1 > dt2:
        return 1
    return 0


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


def _chunking_by_date_and_hour_tweets(data_dir, chunk_index):
    tweets = util.read_and_parse_tweets_from_file(
        os.path.join(data_dir, chunk_file_dir, str(chunk_index)))

    date_format = '%a %b %d %H:%M:%S +0000 %Y'

    tweet_dict = {}

    for tweet in tweets:
        cur_time = datetime.strptime(tweet['created_at'], date_format)
        cur_hour_idx =\
            cur_time.hour / chunk_slot_in_hours * chunk_slot_in_hours
        cur_time_str = str(cur_time.date()) + '-' + str(cur_hour_idx)
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


def _grouping_dated_chunks_to_sliding_window(data_dir):
    dated_chunk_fn = os.listdir(os.path.join(data_dir,
                                             chunk_file_by_date_dir))

    def local_cmp(item1, item2):
        date_format = '%Y-%m-%d-%H'
        time1 = datetime.strptime(item1, date_format)
        time2 = datetime.strptime(item2, date_format)
        if time1 < time2:
            return -1
        if time1 > time2:
            return 1
        return 0

    dated_chunk_fn.sort(cmp=local_cmp)

    for i in range(len(dated_chunk_fn) - window_length_in_slots + 1):
        print >> sys.stderr, dated_chunk_fn[i]
        fo = open(os.path.join(data_dir, sliding_window_dir,
                               dated_chunk_fn[i]),
                  'w')
        for j in range(window_length_in_slots):
            fi = open(os.path.join(data_dir, chunk_file_by_date_dir,
                                   dated_chunk_fn[i + j]))
            for line in fi:
                print >> fo, line.strip()
            fi.close()
        fo.close()


def _comparing_windows_conf(data_dir):
    sliding_window_fn = os.listdir(os.path.join(data_dir, sliding_window_dir))

    def local_cmp(item1, item2):
        date_format = '%Y-%m-%d-%H'
        time1 = datetime.strptime(item1, date_format)
        time2 = datetime.strptime(item2, date_format)
        if time1 < time2:
            return -1
        if time1 > time2:
            return 1
        return 0
    sliding_window_fn.sort(cmp=local_cmp)
    for i in range(window_length_in_slots, len(sliding_window_fn)):
        print >> sys.stderr,\
            sliding_window_fn[i-window_length_in_slots] +\
            '_' + sliding_window_fn[i]
        open(os.path.join(data_dir, comparing_windows_conf_dir,
                          sliding_window_fn[i-window_length_in_slots]
                          + '_' + sliding_window_fn[i]), 'w').close()


if __name__ == '__main__':
    processing_tweets(sys.argv[1], sys.argv[2])
