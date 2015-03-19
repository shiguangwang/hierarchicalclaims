#!/usr/bin/env python2.7
from apollo_lib import util
from datetime import datetime
import sys
import json
import os
from clustering import do_clustering


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
                os.makedirs(os.path.join(data_dir, clusters_dir, chunk_file))
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
