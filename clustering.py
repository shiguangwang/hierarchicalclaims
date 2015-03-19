import re
import os
import datetime
import sys
import codecs
import getopt
import cjson
from apollo_lib import util
from collections import defaultdict

# regular expression to parse url
#token_pattern = r"""
#(^|\s)((https?:\/\/)?[\w-]+(\.[\w-]+)+\.?(:\d+)?(\/\S*)?)
#"""
#token_re = re.compile(token_pattern, re.VERBOSE)
#text="the url is http://t.co/qbVast and ni8 go"
#print token_re.findall(text, 0)


# #------------------------------------------------------------------------------
# # clusters after sorted based on size. each cluster sorted on timestamp
# sorted_claims = [] # sorted_claims[0] is the cluster with cluster_id = 0
# claim_sources = [] # sources of each claims in the same order as sorted_claims
# all_sources = set() # all sources
# source_ranks = {} # source_id => { cluster_id: rank }
# # rank = how many sources preceede me
# # in cluster: cluster_id, source: source_id appears in
# # claim_sources[cluster_id][?]: ? = source_rank[source_id][cluster_id]

# social_links = {} # source_id => list of links
# tweets_found = set() # tweet_id

CONFIG = {
  'MIN_TOKEN_LEN': 4,
  'MAX_TOKEN_NUM': 8,
  'MIN_TOKEN_NUM': 2,
};


#------------------------------------------------------------------------------
def usage():
    print """
$python filename.py [options]
Options:
    Input:
    tweets plain head plain_head source desc author_cluster output_tweets tokens length [social]
    Output:
"""
#------------------------------------------------------------------------------
def hash_tokens(tokens):
  return hash(str(tokens))
#------------------------------------------------------------------------------
# Takes a unicode text and returns the tokens in it
# Tokens are loosely like words in a sentence, but more well-defined.
# Tokens are separated by characters in word_sep
def my_tweet_tokens(text):
  # get rid of URL
  original_text = text
  tok = text.split(' ')
  text = u''
  for x in tok:
    if x[0:5] == 'http:' or x[0:6] == 'https:': continue
    text = text + ' ' + x
  translate_to = u' '
  # dont add _ @ as separator
  # do not add # as separator if we want to preserve the hash-tags
  # presently hashtags  preserved
  #word_sep = u",.#/<>?;:'\"`!$%^&*()-=+~[]\\|{}()\n\t"
  word_sep = u",./<>?;:'\"`!$%^&*()-=+~[]\\|{}()\n\t"
  translate_table = dict((ord(char), translate_to) for char in word_sep)
  tokens = text.translate(translate_table).split(' ')
  return filter(None, tokens)
#------------------------------------------------------------------------------
# filter the small words from the token
def filter_tokens(tokens):
  # get rid of @user or RT or RT@ etc
  tokens_new = filter(lambda x: x[0]!='@' and x!='RT' and x[0:3] != 'RT@',
                      tokens)
  # get rid of tokens with length < MIN_TOKEN_LEN
  # this will get rid of of, and, is, are, to, for, who, me etc
  tokens_new = filter(lambda x:len(x) >= CONFIG['MIN_TOKEN_LEN'], tokens_new)

  # create two version

  #### version 1 #####
  # in one version, get rid of the hashtags (# + word)
  # "#syria #checmical attack" => "attack"
  tokens_hashtag_removed = filter(lambda x: x[0] != '#', tokens_new)
  # if it contains less than MIN_TOKEN_NUM tokens, then its insignificant
  if len(tokens_hashtag_removed) < CONFIG['MIN_TOKEN_NUM']:
    tokens_hashtag_removed = []
  # further process each of these to make lower case and take on
  # at most MAX_TOKEN_NUM tokens
  tokens_hashtag_removed_new = \
    tokens_hashtag_removed[0:CONFIG['MAX_TOKEN_NUM']]
  tokens_hashtag_removed_new = \
    map(lambda x: x.lower(), tokens_hashtag_removed_new)
  tuple_hashtag_removed = tuple(tokens_hashtag_removed_new)

  #### version 2 #####
  # in another version, get rid of just the # symbol (keep the words intact)
  # "#syria #chemical attack" => "syria chemical attack"
  tokens_hash_removed = map(lambda x: x[1:] if x[0] == '#' else x, tokens_new)
  # if it contains less than MIN_TOKEN_NUM tokens, then its insignificant
  if len(tokens_hash_removed) < CONFIG['MIN_TOKEN_NUM']:
    tokens_hash_removed = []
  # further process each of these to make lower case and take on
  # at most MAX_TOKEN_NUM tokens
  tokens_hash_removed_new = tokens_hash_removed[0:CONFIG['MAX_TOKEN_NUM']]
  tokens_hash_removed_new = map(lambda x: x.lower(), tokens_hash_removed_new)
  tuple_hash_removed = tuple(tokens_hash_removed_new)

  # if length of both of these are same, it means both of these are same
  if len(tokens_hashtag_removed) == len(tokens_hash_removed):
    return (tuple_hash_removed, ())
  else:
    # two different
    if tuple_hashtag_removed == tuple_hash_removed:
      return (tuple_hash_removed, ())
    elif tuple_hashtag_removed == ():
      return (tuple_hash_removed, ())
    elif tuple_hashtag_removed > tuple_hash_removed:
      return (tuple_hash_removed, tuple_hashtag_removed)
    else:
      return (tuple_hashtag_removed, tuple_hash_removed)

#------------------------------------------------------------------------------
def write_social_links():
  f = open(sys.argv[11], 'w')
  # sort based on outdegree
  L = sorted (social_links.items(), key = lambda(x): -len(x[1]))
  for tuple in L:
    if len(tuple[1]) == 0:
      break
    u = tuple[0]
    for v in tuple[1]:
      print >> f, "%d,%d" % (u,v)
  f.close()

#------------------------------------------------------------------------------
# derive the links for source i
def derive_social_links(source_id):
  sc_rank = source_ranks[source_id].copy()
  social_links[source_id] = []

  while sc_rank:
    hs_tally = {}
    for cluster_id in sc_rank: # what are the clusters this source appears
      # what is the rank of this source in cluster: cluster_id
      this_rank = sc_rank[cluster_id]
      # what are the sources ranked higher than me
      for i in range(0, this_rank):
        hs = claim_sources[cluster_id - 1][i]
        if hs in hs_tally:
          hs_tally[hs].add(cluster_id) # keep track of cluster id
        else:
          hs_tally[hs] = set([cluster_id])

    if hs_tally: #hs_tally may be empty due to singleton clusters
      # find max from hs_tally
      max_source = max(hs_tally.iterkeys(), key = lambda(x): len(hs_tally[x]))
      # add a link from source_id to max_source
      social_links[source_id].append(max_source)
      # see what clustered are covered, remove those from consideration
      for covered_cluster in hs_tally[max_source]:
        del sc_rank[covered_cluster]
    else:
      return
#------------------------------------------------------------------------------
def sort_and_write_clusters(clusters, sorted_clusters, sources,
                            cluster_sources, rank):
  # sort the clusters by size
  print >> sys.stderr, "Clustering: Sorting clusters by size"
  sorted_pairs = sorted(clusters.items(), key = lambda(k,v):-len(v))

  plain_file = codecs.open(sys.argv[2], 'w', encoding = 'utf-8')
  #head_file = codecs.open(sys.argv[3], 'w', encoding ='utf-8')
  head_file = open(sys.argv[3], 'w')
  plain_head_file = codecs.open(sys.argv[4], 'w', encoding ='utf-8')
  source_file = open(sys.argv[5], 'w')
  desc_file = open(sys.argv[6], 'w')
  author_cluster_file = open(sys.argv[7], 'w')
  output_tweets_file = open(sys.argv[8], 'w')
  tokens_file = codecs.open(sys.argv[9], 'w', encoding = 'utf-8')
  length_file = open(sys.argv[10], 'w')

  cluster_id = 1
  cumulative_lengths = 0
  print >> sys.stderr, "Clustering: Found", len(sorted_pairs), "clusters"

  # print some info about clustering statistics
  print >> sys.stderr, "Clustering: Largest clusters contain",
  for i in xrange(0, min(10, len(sorted_pairs))):
    print >> sys.stderr, len(sorted_pairs[i][1]),
  print >> sys.stderr, "tweets"

  print >> sys.stderr, "Clustering: Writing clusters to output files"
  for pair in sorted_pairs:
    cluster = pair[1]
    # sort each cluster by timestamp (actually id which is equivalent)
    sorted_cluster = sorted(cluster, key = lambda(k):util.get_tweet_id(k))
    sorted_clusters.append(sorted_cluster)
    # the actual cluster
    # cluster id and length
    print >> head_file, sorted_cluster[0]
    print >> tokens_file, cluster_id, pair[0]
    cumulative_lengths += len(sorted_cluster)
    print >> length_file, cluster_id, len(sorted_cluster), cumulative_lengths

    # cluster id and the list of tweets in it
    cluster_id_str = str(cluster_id)
    desc_file.write(cluster_id_str)
    source_file.write(cluster_id_str)

    known_sources = set()
    this_cluster_sources = []
    this_cluster_source_count = 0

    print >> plain_file, "=== Cluster", cluster_id, "==="

    count = 0

    for tweet in sorted_cluster:
      count += 1
      desc_file.write(' ' + util.get_tweet_id_str(tweet))

      plain_text = util.get_tweet_text(tweet)
      plain_text = util.convert_text_unicode(plain_text)
      plain_text = util.renderTweetForPlain(plain_text)
      print >> plain_file, plain_text
      print >> output_tweets_file, tweet

      if count == 1:
        print >> plain_head_file, plain_text

      source_id = util.get_tweet_source_id(tweet)
      if source_id not in known_sources: # sources for this particular cluster
        this_cluster_sources.append(source_id)
        if source_id not in rank:
          rank[source_id] = {}
        # what is the rank of this source in this cluster
        rank[source_id][cluster_id] = this_cluster_source_count
        source_file.write(' ' + str(source_id))
        print >> author_cluster_file, "%d\t%d" % (source_id, cluster_id)
        known_sources.add(source_id)
        this_cluster_source_count = this_cluster_source_count + 1

      if source_id not in sources: # all sources
        sources.add(source_id)

    desc_file.write('\n')
    source_file.write('\n')
    cluster_sources.append(this_cluster_sources)
    cluster_id = cluster_id + 1

  source_file.close()
  desc_file.close()
  head_file.close()
  plain_head_file.close()
  plain_file.close()
  author_cluster_file.close()
  output_tweets_file.close()
  tokens_file.close()
  length_file.close()

#------------------------------------------------------------------------------
def sort_and_write_clusters_shell(clusters, sorted_clusters, sources,
                            cluster_sources, rank, plain_file, head_file, plain_head_file, source_file, desc_file, author_cluster_file, output_tweets_file, tokens_file, length_file):
  # sort the clusters by size
  print >> sys.stderr, "Clustering: Sorting clusters by size"
  sorted_pairs = sorted(clusters.items(), key = lambda(k,v):-len(v))

  # plain_file = codecs.open(sys.argv[2], 'w', encoding = 'utf-8')
  # #head_file = codecs.open(sys.argv[3], 'w', encoding ='utf-8')
  # head_file = open(sys.argv[3], 'w')
  # plain_head_file = codecs.open(sys.argv[4], 'w', encoding ='utf-8')
  # source_file = open(sys.argv[5], 'w')
  # desc_file = open(sys.argv[6], 'w')
  # author_cluster_file = open(sys.argv[7], 'w')
  # output_tweets_file = open(sys.argv[8], 'w')
  # tokens_file = codecs.open(sys.argv[9], 'w', encoding = 'utf-8')
  # length_file = open(sys.argv[10], 'w')

  cluster_id = 1
  cumulative_lengths = 0
  print >> sys.stderr, "Clustering: Found", len(sorted_pairs), "clusters"

  # print some info about clustering statistics
  print >> sys.stderr, "Clustering: Largest clusters contain",
  for i in xrange(0, min(10, len(sorted_pairs))):
    print >> sys.stderr, len(sorted_pairs[i][1]),
  print >> sys.stderr, "tweets"

  print >> sys.stderr, "Clustering: Writing clusters to output files"
  for pair in sorted_pairs:
    cluster = pair[1]
    # sort each cluster by timestamp (actually id which is equivalent)
    sorted_cluster = sorted(cluster, key = lambda(k):util.get_tweet_id(k))
    sorted_clusters.append(sorted_cluster)
    # the actual cluster
    # cluster id and length
    print >> head_file, sorted_cluster[0]
    print >> tokens_file, cluster_id, pair[0]
    cumulative_lengths += len(sorted_cluster)
    print >> length_file, cluster_id, len(sorted_cluster), cumulative_lengths

    # cluster id and the list of tweets in it
    cluster_id_str = str(cluster_id)
    desc_file.write(cluster_id_str)
    source_file.write(cluster_id_str)

    known_sources = set()
    this_cluster_sources = []
    this_cluster_source_count = 0

    print >> plain_file, "=== Cluster", cluster_id, "==="

    count = 0

    for tweet in sorted_cluster:
      count += 1
      desc_file.write(' ' + util.get_tweet_id_str(tweet))

      plain_text = util.get_tweet_text(tweet)
      plain_text = util.convert_text_unicode(plain_text)
      plain_text = util.renderTweetForPlain(plain_text)
      print >> plain_file, plain_text
      print >> output_tweets_file, tweet

      if count == 1:
        print >> plain_head_file, plain_text

      source_id = util.get_tweet_source_id(tweet)
      if source_id not in known_sources: # sources for this particular cluster
        this_cluster_sources.append(source_id)
        if source_id not in rank:
          rank[source_id] = {}
        # what is the rank of this source in this cluster
        rank[source_id][cluster_id] = this_cluster_source_count
        source_file.write(' ' + str(source_id))
        print >> author_cluster_file, "%d\t%d" % (source_id, cluster_id)
        known_sources.add(source_id)
        this_cluster_source_count = this_cluster_source_count + 1

      if source_id not in sources: # all sources
        sources.add(source_id)

    desc_file.write('\n')
    source_file.write('\n')
    cluster_sources.append(this_cluster_sources)
    cluster_id = cluster_id + 1

  # source_file.close()
  # desc_file.close()
  # head_file.close()
  # plain_head_file.close()
  # plain_file.close()
  # author_cluster_file.close()
  # output_tweets_file.close()
  # tokens_file.close()
  # length_file.close()
#------------------------------------------------------------------------------
# assumed that both node1 and node2 actually contain clusters
# i.e they were found by calling find_root_path_compress
def merge_root(linked_bucket, node1, node2):
  cluster1 = linked_bucket[node1] # these are references
  cluster2 = linked_bucket[node2]
  if (type(cluster1) is not list) or (type(cluster2) is not list):
    print >> sys.stderr, "Clustering: logic error"
  if len(cluster1) >= len(cluster2):
    cluster1 += cluster2 # be careful we are updating the linked_bucket
    linked_bucket[node2] = node1 # create the new link
    return node1
  else:
    cluster2 += cluster1
    linked_bucket[node1] = node2
    return node2
#------------------------------------------------------------------------------
def find_root_path_compress(linked_bucket, key):
  new_thing = linked_bucket[key]
  if type(new_thing) is list:
    return key
  elif type(new_thing) is tuple:
    real_key = find_root_path_compress(linked_bucket, new_thing)
    linked_bucket[key] = real_key
    return real_key
  else:
    print >> sys.stderr, "Clustering: Something wrong with linked structure"
#------------------------------------------------------------------------------
def doit():
  # it is important to work with the unicode representation of text
  # the ascii representation will break the tokenization
  input_file = open(sys.argv[1], 'r')
  # opening file in unicode mode is slow and makes problem
  # so decided to change text to unicode after initial processing
  #input_file = codecs.open(sys.argv[1], 'r', encoding='utf-8')
  progress = 0

  linked_buckets = defaultdict(list)
  # maps from tuple to clusters. clusters are list of parsed tweets
  # linked_buckets[k] is a tuple if it links to another place
  # linked_bucket[k] is a list if it contains actual clusters

  while 1:
    try:
      line = input_file.readline()[:-1]
    except Exception, e:
      print e
      input_file.seek(1,1) # the first argument is advance 1 byte
      # the second argument means from current position
      continue
    progress = progress + 1
    if progress % 1000 == 0: print >> sys.stderr, "Clustering:",  progress
    if not line:
      print >> sys.stderr, "Clustering: End of File"
      break

    try:
      parsed = util.parse_eval_json(line)
    except Exception, e:
      #print >> sys.stderr, progress, e
      continue

    if not util.is_valid_tweet(parsed):
      #print >> sys.stderr, "503", progress, "Bad tweet"
      #print >> sys.stderr, "504", parsed
      continue

    # register tweet
    tweet_id = util.get_tweet_id(parsed)
    if tweet_id in tweets_found:
      #print >> sys.stderr, "Duplicate tweet"
      continue
    else:
      tweets_found.add(tweet_id)


    text = util.get_tweet_text(parsed)
    # it is important to work with the unicode representation of text
    # the ascii representation will break the tokenization
    text = util.convert_text_unicode(text)

    tokens =  my_tweet_tokens(text)
    # now tokens are the tokens
    (parent_tuple, child_tuple) = filter_tokens(tokens)
    # the return value of filter_tokens
    # can be (parent, None) or (parent, child), or (None, None)
    # (None, child) is never a return value
    if not parent_tuple:
      #print >> sys.stderr, "501", progress, "Dropped tweet due to lack of tokens"
      #print >> sys.stderr, "502", util.renderTweetForPlain(text)
      continue

    # if only one cluster resulted from filter
    # then child cluser is []
    if not child_tuple:
      # parent_cluster is surely not None
      root_of_parent = find_root_path_compress(linked_buckets, parent_tuple)
      linked_buckets[root_of_parent].append(parsed)
    else:
      # as child cluster is not None, parent cluster is also not None
      # merge them
      root_of_parent = find_root_path_compress(linked_buckets, parent_tuple)
      root_of_child = find_root_path_compress(linked_buckets, child_tuple)
      if root_of_parent == root_of_child:
        new_root = root_of_parent
      else:
        new_root = merge_root(linked_buckets, root_of_parent, root_of_child)

      linked_buckets[new_root].append(parsed)

  input_file.close()
  # build "buckets" from linked_buckets
  buckets = {}
  for k in linked_buckets:
    if type(linked_buckets[k]) is list:
      buckets[k] = linked_buckets[k]

  #write clusters to files
  sort_and_write_clusters(buckets, sorted_claims, all_sources, claim_sources, \
      source_ranks)

#------------------------------------------------------------------------------
def doit_shell(input_file, plain_file, head_file, plain_head_file, source_file, desc_file, author_cluster_file, output_tweets_file, tokens_file, length_file):
  #------------------------------------------------------------------------------
  # clusters after sorted based on size. each cluster sorted on timestamp
  sorted_claims = [] # sorted_claims[0] is the cluster with cluster_id = 0
  claim_sources = [] # sources of each claims in the same order as sorted_claims
  all_sources = set() # all sources
  source_ranks = {} # source_id => { cluster_id: rank }
  # rank = how many sources preceede me
  # in cluster: cluster_id, source: source_id appears in
  # claim_sources[cluster_id][?]: ? = source_rank[source_id][cluster_id]

  social_links = {} # source_id => list of links
  tweets_found = set() # tweet_id

  # it is important to work with the unicode representation of text
  # the ascii representation will break the tokenization
  # input_file = open(sys.argv[1], 'r')
  # opening file in unicode mode is slow and makes problem
  # so decided to change text to unicode after initial processing
  #input_file = codecs.open(sys.argv[1], 'r', encoding='utf-8')
  progress = 0

  linked_buckets = defaultdict(list)
  # maps from tuple to clusters. clusters are list of parsed tweets
  # linked_buckets[k] is a tuple if it links to another place
  # linked_bucket[k] is a list if it contains actual clusters

  while 1:
    try:
      line = input_file.readline()[:-1]
    except Exception, e:
      print e
      input_file.seek(1,1) # the first argument is advance 1 byte
      # the second argument means from current position
      continue
    progress = progress + 1
    if progress % 1000 == 0: print >> sys.stderr, "Clustering:",  progress
    if not line:
      print >> sys.stderr, "Clustering: End of File"
      break

    try:
      parsed = util.parse_eval_json(line)
    except Exception, e:
      #print >> sys.stderr, progress, e
      continue

    if not util.is_valid_tweet(parsed):
      #print >> sys.stderr, "503", progress, "Bad tweet"
      #print >> sys.stderr, "504", parsed
      continue

    # register tweet
    tweet_id = util.get_tweet_id(parsed)
    if tweet_id in tweets_found:
      #print >> sys.stderr, "Duplicate tweet"
      continue
    else:
      tweets_found.add(tweet_id)


    text = util.get_tweet_text(parsed)
    # it is important to work with the unicode representation of text
    # the ascii representation will break the tokenization
    text = util.convert_text_unicode(text)

    tokens =  my_tweet_tokens(text)
    # now tokens are the tokens
    (parent_tuple, child_tuple) = filter_tokens(tokens)
    # the return value of filter_tokens
    # can be (parent, None) or (parent, child), or (None, None)
    # (None, child) is never a return value
    if not parent_tuple:
      #print >> sys.stderr, "501", progress, "Dropped tweet due to lack of tokens"
      #print >> sys.stderr, "502", util.renderTweetForPlain(text)
      continue

    # if only one cluster resulted from filter
    # then child cluser is []
    if not child_tuple:
      # parent_cluster is surely not None
      root_of_parent = find_root_path_compress(linked_buckets, parent_tuple)
      linked_buckets[root_of_parent].append(parsed)
    else:
      # as child cluster is not None, parent cluster is also not None
      # merge them
      root_of_parent = find_root_path_compress(linked_buckets, parent_tuple)
      root_of_child = find_root_path_compress(linked_buckets, child_tuple)
      if root_of_parent == root_of_child:
        new_root = root_of_parent
      else:
        new_root = merge_root(linked_buckets, root_of_parent, root_of_child)

      linked_buckets[new_root].append(parsed)

  input_file.close()
  # build "buckets" from linked_buckets
  buckets = {}
  for k in linked_buckets:
    if type(linked_buckets[k]) is list:
      buckets[k] = linked_buckets[k]

  #write clusters to files
  sort_and_write_clusters_shell(buckets, sorted_claims, all_sources, claim_sources, \
      source_ranks, plain_file, head_file, plain_head_file, source_file, desc_file, author_cluster_file, output_tweets_file, tokens_file, length_file)

  # clear all the variables in memory
  buckets = None
  linked_buckets = None
  tweets_found = None
  sorted_claims = None
  all_sources = None
  claim_sources = None
  source_ranks = None


#------------------------------------------------------------------------------
def main():
  print len(sys.argv), "arguments given"
  if len(sys.argv) < 11 or len(sys.argv) > 12:
    usage()
    sys.exit(0)
  doit()
  #optional: add the social network (latent influence) : SIGMETRICS
  if len(sys.argv) == 12:
    for src in all_sources:
      derive_social_links(src)
    write_social_links()


def do_clustering(
    input_file_name,
    plain_file_name,
    head_file_name,
    plain_head_file_name,
    source_file_name,
    desc_file_name,
    author_cluster_file_name,
    output_tweets_file_name,
    tokens_file_name,
    length_file_name):
  input_file = open(input_file_name, 'r')
  plain_file = codecs.open(plain_file_name, 'w', encoding='utf-8')
  head_file = open(head_file_name, 'w')
  plain_head_file = codecs.open(plain_head_file_name, 'w', encoding='utf-8')
  source_file = open(source_file_name, 'w')
  desc_file = open(desc_file_name, 'w')
  author_cluster_file = open(author_cluster_file_name, 'w')
  output_tweets_file = open(output_tweets_file_name, 'w')
  tokens_file = codecs.open(tokens_file_name, 'w', encoding='utf-8')
  length_file = open(length_file_name, 'w')

  doit_shell(input_file, plain_file, head_file, plain_head_file, source_file, desc_file, author_cluster_file, output_tweets_file, tokens_file, length_file)

  input_file.close()
  source_file.close()
  desc_file.close()
  head_file.close()
  plain_head_file.close()
  plain_file.close()
  author_cluster_file.close()
  output_tweets_file.close()
  tokens_file.close()
  length_file.close()


#------------------------------------------------------------------------------
def main_shell():
  print len(sys.argv), "arguments given"
  if len(sys.argv) < 11 or len(sys.argv) > 12:
    usage()
    sys.exit(0)

  do_clustering(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], sys.argv[6], sys.argv[7], sys.argv[8], sys.argv[9], sys.argv[10])

  # input_file = open(sys.argv[1], 'r')
  # plain_file = codecs.open(sys.argv[2], 'w', encoding = 'utf-8')
  # head_file = open(sys.argv[3], 'w')
  # plain_head_file = codecs.open(sys.argv[4], 'w', encoding ='utf-8')
  # source_file = open(sys.argv[5], 'w')
  # desc_file = open(sys.argv[6], 'w')
  # author_cluster_file = open(sys.argv[7], 'w')
  # output_tweets_file = open(sys.argv[8], 'w')
  # tokens_file = codecs.open(sys.argv[9], 'w', encoding = 'utf-8')
  # length_file = open(sys.argv[10], 'w')

  # doit_shell(input_file, plain_file, head_file, plain_head_file, source_file, desc_file, author_cluster_file, output_tweets_file, tokens_file, length_file)

  # input_file.close()
  # source_file.close()
  # desc_file.close()
  # head_file.close()
  # plain_head_file.close()
  # plain_file.close()
  # author_cluster_file.close()
  # output_tweets_file.close()
  # tokens_file.close()
  # length_file.close()

  #optional: add the social network (latent influence) : SIGMETRICS
  if len(sys.argv) == 12:
    for src in all_sources:
      derive_social_links(src)
    write_social_links()

if __name__ == "__main__":
  #raw = raw_input()
  #text = raw.decode("utf-8")
  #print word_tokens(text)
  main_shell()
