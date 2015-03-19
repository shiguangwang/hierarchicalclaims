# -*- coding: utf-8 -*-
from datetime import timedelta,datetime
import getopt
import iso8601
import random
import time
import simplejson

import os
import sys
import urllib2
import cjson
import codecs
import cchardet
#------------------------------------------------------------------------------
def getApolloHome():
  return os.environ['APOLLO_HOME']

#------------------------------------------------------------------------------
def readVersions():
  vfile = open(getApolloHome() + '/version', 'r')
  versions = vfile.readlines()
  vfile.close()
  return versions
#------------------------------------------------------------------------------
def getApolloMajor():
  return readVersions()[0][:-1]

#------------------------------------------------------------------------------
def getApolloMinor():
  return readVersions()[0][:-1]
#------------------------------------------------------------------------------
def getApolloVersion():
  versions = readVersions()
  return versions[0][:-1] + '.' + versions[1][:-1] + '.' + versions[2][:-1]
#------------------------------------------------------------------------------
def renderTweetForBrowser(text):

  # text may have html entities, correct those?
  # probably no, because it should be in that form in the browser
  # text = urllib2.unquote(text)

  # the text may have backslashed url, correct it
  # some data has been wrongly escaped this was due to a bug in cjson
  # cjson.decode('{"x": "\/"}') returned {'x': '\\/'} instead of {'x': '/'}
  text = text.replace('\/','/')
  return text

#------------------------------------------------------------------------------
def renderTweetForPlain(text):

  # text may have html entities, correct those?
  # probably no, because it should be in that form in the browser
  text = urllib2.unquote(text)

  # the text may have backslashed url, correct it
  # some data has been wrongly escaped this was due to a bug in cjson
  # cjson.decode('{"x": "\/"}') returned {'x': '\\/'} instead of {'x': '/'}
  text = text.replace('\/','/')
  text = text.replace('\n', ' ') # get rid of newlines
  return text
#------------------------------------------------------------------------------
# Create point from tweet
def pointFromText(text):
    MIN_TOKEN_LEN = 4  # Token must be >= MIN_TOKEN_LEN
    MIN_TOKEN_NUM = 2  # len(tokens) >= MIN_TOKEN_NUM
    tokens = tweet_tokens(text)
    indices = {}
    for token in tokens:
        #Get rid of @user or RT or RT@
        if len(token) < MIN_TOKEN_LEN \
                or token[0] == '@' or token[0:2] == 'RT' or token[0:3] == "RT@":
            continue
        indices[token.lower()] = 1.0

    if len(indices) < MIN_TOKEN_NUM:
        indices = {}
    return indices
#------------------------------------------------------------------------------

# U . U
# Finds the square of the norm of the vector.
def calculateNormSquared(point):
    l2NormSquared = 0.0
    for index in point:
        l2NormSquared += point[index] * point[index]
    return l2NormSquared
#------------------------------------------------------------------------------
# U . V
# Finds the square of the norm of the vector.
# Dot product calculation should be small.dot(big) for efficiency.
def calculateDotProduct(u, v):
    sumProduct = 0.0;
    for index in u:
        if index in v:
            sumProduct += u[index] * v[index]
    return sumProduct
#------------------------------------------------------------------------------
def calculateDistance(u, v):
    # Dot prod calculation should be small.dot(big) for efficiency.
    dotProd = calculateDotProduct(u, v)
    return 1.0 - (dotProd \
            / ( calculateNormSquared(u) + calculateNormSquared(v) - dotProd))
#------------------------------------------------------------------------------
def calculateDistanceCached(u, v, u_norm_squared, v_norm_squared):
    dotProd = calculateDotProduct(u, v)
    return 1.0 - (dotProd / (u_norm_squared + v_norm_squared - dotProd))
#------------------------------------------------------------------------------
# Takes a unicode text and returns the words in it
# Words are separated by characters in word_sep
def tweet_tokens(text):
  # get rid of URL
  original_text = text
  tok = text.split(' ')
  text = u''
  for x in tok:
    if x[0:5] == 'http:' or x[0:6] == 'https:': continue
    text = text + ' ' + x
  translate_to = u' '
  # dont add _ @  as separator
  # do not add # as separator if we want to preserve the hash-tags
  # presently hashtags not preserved
  # word_sep = u",.#/<>?;:'\"`!$%^&*()-=+~[]\\|{}()\n\t"
  #word_sep = u",./<>?;:'\"`!$%^&*()-=+~[]\\|{}()\n\t"
  word_sep = u" ,.#?:;'\"/<>`!$%^&*()-=+~[]\\|{}()\n\t" \
    + u"©℗®℠™،、⟨⟩‒–—―…„“”–――»«›‹‘’：（）！？=【】　・" \
    + u"⁄·† ‡°″¡¿÷№ºª‰¶′″‴§|‖¦⁂❧☞‽⸮◊※⁀「」﹁﹂『』﹃﹄《》―—" \
    + u"“”‘’、，一。►…¿«「」ー⋘▕▕▔▏┈⋙一ー।;!؟"
  translate_table = dict((ord(char), translate_to) for char in word_sep)
  tokens = text.translate(translate_table).split(' ')
  return filter(None, tokens)
#------------------------------------------------------------------------------
# try to parse a line using eval and then cjson
def parse_eval_json(text):
  try:
    return eval(text)
  except Exception, e1:
    try:
      return cjson.decode(text)
    except Exception, e2:
      raise Exception("Parse Error")
#------------------------------------------------------------------------------
# get the source of a tweet
def get_tweet_source_id_str(tweet):
  if 'user' in tweet:
    if 'id_str' in tweet['user']:
      return tweet['user']['id_str']
    elif 'id' in tweet['user']:
      return str(tweet['user']['id'])
  
  if 'from_user_id_str' in tweet:
    return tweet['from_user_id_str']
  elif 'from_user_id' in tweet:
    return str(tweet['from_user_id'])

  raise Exception("Can not find user id in tweet")
#------------------------------------------------------------------------------
# get the tweet coordinates
def get_tweet_coordinates(tweet):
    if 'coordinates' in tweet:
        return tweet['coordinates']
    raise Exception("Cannot find tweet coordinates")


def get_tweet_geo(tweet):
    if 'geo' in tweet:
        return tweet['geo']
    raise Exception("Cannot find tweet geo")


def get_user_location(tweet):
    if 'user' in tweet:
        if 'location' in tweet['user']:
            return tweet['user']['location']
    raise Exception("Cannot find user location")


def get_user_profile_location(tweet):
    if 'user' in tweet:
        if 'profile_location' in tweet['user']:
            return tweet['user']['profile_location']
    raise Exception("Cannot find user profile location")
#------------------------------------------------------------------------------
# get the source of a tweet
def get_tweet_source_id(tweet):
  if 'user' in tweet:
    if 'id_str' in tweet['user']:
      return int(tweet['user']['id_str'])
    elif 'id' in tweet['user']:
      return tweet['user']['id']
  
  if 'from_user_id_str' in tweet:
    return int(tweet['from_user_id_str'])
  elif 'from_user_id' in tweet:
    return tweet['from_user_id']

  raise Exception("Can not find user id in tweet")
#------------------------------------------------------------------------------
# get the source of a tweet
def get_tweet_source_name(tweet):
  if 'user' in tweet:
    if 'screen_name' in tweet['user']:
      return tweet['user']['screen_name']
  
  if 'from_user' in tweet:
    return tweet['from_user']
    
  raise Exception("Can not find user name in tweet")
#==============================================================================
# id field is not reliable, use id_str whenever possible

def get_tweet_id(tweet):
  if 'id_str' in tweet:
    return int(tweet['id_str'])
  elif 'id' in tweet:
    return tweet['id']
  raise Exception("Can not find id in tweet")
#==============================================================================
def get_tweet_id_str(tweet):
  if 'id_str' in tweet:
    return tweet['id_str']
  elif 'id' in tweet:
    return str(tweet['id'])
  raise Exception("Can not find id str in tweet")
#==============================================================================
def convert_text_unicode(text, with_warning = False):
  if type(text) is str:
    try:
      text = text.decode('utf-8')
    except:
      # detect encoding
      try:
        enc = cchardet.detect(text)['encoding']
        text = text.decode(enc)
        if with_warning:
          print >> sys.stderr, "Customized encoding", enc
      except:
        text = text.decode('utf-8', errors = 'ignore')
        if with_warning:
          print >> sys.stderr, "Unicode decode error ignoring"

  return text
#==============================================================================
def convert_tweet_text_unicode(tweet):
  if 'retweeted_status' in tweet:
    temp = tweet['retweeted_status']['text']
    tweet['retweeted_status']['text'] = convert_text_unicode(temp)
    
  temp = tweet['text']
  tweet['text'] = convert_text_unicode(temp)
#==============================================================================
def get_tweet_text(tweet):
  if 'retweeted_status' in tweet:
    text = tweet['retweeted_status']['text']
  else:
    text = tweet['text']
  return text
#==============================================================================
def get_tweet_created_at(tweet):
  return tweet['created_at']
#==============================================================================
# Parse datatime in tweet to datetime of Python since standard parsing doesn't
# work
#
def parse_datetime(s):
  return get_tweet_datetime(s)
#==============================================================================

def get_tweet_datetime(s):
  # check for garbage
  if s[0] == '%':
    s = s[1:]
  if s.find(',') == -1: #Twitter API v 1.1 time format
    try:
      # s = 'Mon Sep 24 03:35:21 +0000 2012'
      
      # is there a timezone?
      pluszone = s.find('+')
      minuszone = s.find('-')
      offset = 0
      if pluszone != -1 or minuszone != -1:
        # there is a timezone
        zonepos = max(pluszone, minuszone)
        # python can't parse time zone correctly
        next_space = s.find(' ', zonepos)
        offset = int(s[zonepos:next_space])
        s = s[:zonepos] + s[next_space + 1:]
      delta = timedelta(hours = offset / 100)
      fmt = "%a %b %d %H:%M:%S %Y"
      time = datetime.strptime(s, fmt)
      time -= delta
      return time
    except Exception, e:
      # 2012-11-04 05:12:34
      # assume simple format
      fmt = "%Y-%m-%d %H:%M:%S"
      time = datetime.strptime(s, fmt)
      return time
  else:
    offset = int(s[-5:])
    delta = timedelta(hours = offset / 100)
    fmt = "%a, %d %b %Y %H:%M:%S"
    time = datetime.strptime(s[:-6], fmt)
    time -= delta
    return time
#==============================================================================
def is_valid_tweet(tweet):
  try:
    p1 = get_tweet_source_id_str(tweet)
    p2 = get_tweet_source_id(tweet)
    p3 = get_tweet_text(tweet)
    p4 = get_tweet_created_at(tweet)
    p5 = get_tweet_datetime(p4)
    p6 = get_tweet_id(tweet)
    p7 = get_tweet_id_str(tweet)
    if p1 is not None and p2 is not None and p3 is not None and \
       p4 is not None and p5 is not None and p6 is not None and \
       p7 is not None:
      return True
    else:
      return False
  except Exception, e:
    return False 
#==============================================================================
# Read tweets from a file and parse those
# Unicode format
#
def read_and_parse_tweets_from_file(my_file, with_progress = False,
                                    continue_on_error = True, 
                                    with_warning = False):
  #input_file = codecs.open(file, 'r', encoding='utf-8')
  input_file = open(my_file, 'r')
  progress = 0
  tweets = []
  count_bad = 0
  # read and parse tweets
  while 1:
    line = input_file.readline()[:-1]
    progress = progress + 1
    if progress % 1000 == 0 and with_progress: print progress
    if not line: break
    try:
      parsed =  parse_eval_json(line)
      if not is_valid_tweet(parsed):
        continue
      convert_tweet_text_unicode(parsed)
    except Exception, e:
      if not continue_on_error:
        raise RuntimeError('Format error')
      else:
        count_bad += 1
        if with_warning:
          print >> sys.stderr, "Format error at line", progress
        continue
    tweets.append(parsed)
  input_file.close()
  return tweets
#------------------------------------------------------------------------------
def account_link_from_id_str(user_id):
  return "<a href=\"https://twitter.com/intent/user?user_id=" \
      + user_id + "\" target=\"_blank\">" + user_id + "</a>"
#------------------------------------------------------------------------------
def account_link_from_id_str(user_id):
  return "<a href=\"https://twitter.com/intent/user?user_id=" \
      + user_id + "\" target=\"_blank\">" + user_id + "</a>"
#------------------------------------------------------------------------------
def second_diff(t1, t2, date_format):
  if date_format == "twitter":
    return time.mktime(t1.utctimetuple()) - time.mktime(t2.utctimetuple())
  elif date_format == "topsy":
    return t1 - t2
#------------------------------------------------------------------------------
def datetime_2_unixtime(t):
  starttime = time.mktime(t.timetuple())+1e-6*t.microsecond
  return starttime
#------------------------------------------------------------------------------
def unixtime_2_datetime(unix_time):
  return datetime.fromtimestamp(unix_time)
#------------------------------------------------------------------------------
def gt_time(t1, t2, date_format):
  if date_format == "twitter":
    return t1.utctimetuple() > t2.utctimetuple()
  elif date_format == "topsy":
    return t1 > t2
#------------------------------------------------------------------------------
def ge_time(t1, t2, date_format):
  if date_format == "twitter":
    return t1.utctimetuple() >= t2.utctimetuple()
  elif date_format == "topsy":
    return t1 >= t2

#==============================================================================
def topsy_parse_datetime(topsy_time):
  return unixtime_2_datetime(topsy_time)
#==============================================================================
def topsy_parse_id(topsy_url):
  try:
    url_parts = topsy_url.split('/')
    return int(url_parts[len(url_parts) - 1])
  except Exception, e:
    print >> sys.stderr, topsy_url
    raise e
#------------------------------------------------------------------------------
# Get a random access token
def read_access_token():
    token_path = os.environ['APOLLO_HOME']
    token_file = '.access_token'
    token_file_path = token_path + '/' + token_file;
    try:
        file1 = open(token_file_path, 'r')
        lines = file1.readlines()
        # get rid of blank lines if there is any
        tokens = filter(lambda x: x != '', lines)
        tokenindex = random.randint(0, len(tokens) - 1)
    except Exception, e:
        print >> stderr, "Access Token not present or invalid"
        sys.exit(1)
    return tokens[tokenindex][:-1] # [:-1] gets rid of the newline
