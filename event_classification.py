import os
import sys
from datetime import datetime
from apollo_lib import util
from pprint import pprint as pp
import nltk
from nltk.corpus import stopwords
import re
import string
from collections import Counter
from caldist import caldist_cosine
import ast


keyword_bags = {
    "war": [
        'saudi',
        'yemen',
        'bombing',
        'attack',
        'boston',
        'trial',
        'phase',
        'campaign',
        'rebels',
        'penalty',
        'arabia',
        'marathon',
        'malala',
        'pakistan',
        'airstrikes',
        'police',
        'coalition',
        'court',
        'border',
        'church',
        'ukraine',
        'houthi',
        'paris',
        'french',
        'syria',
        'yemeni',
        'yousafzai',
        'jails',
        'begin',
        'kills',
        'dozens',
        'capital',
        'france',
        'south',
        'terror',
        'saudis',
        'soldiers',
        'clash',
        'death',
        'churches',
        'tsarnaev',
        'breaking',
        'shiite',
        'strikes',
        'arrested',
        'operation',
        'foiled',
        'extremist',
        'begins'
     ],

    'disaster': [
        'nepal',
        'disaster',
        'earthquake',
        'cross',
        'response',
        'captain',
        'relief',
        'chernobyl',
        'nepalearthquake',
        'donate',
        'ferry',
        'quake',
        'migrant',
        'south',
        'people',
        'korea',
        'humanitarian',
        'nuclear',
        'worst',
        'assistance',
        'ukraine',
        'india',
        'sending',
        'prayers',
        'nepalquake',
        'appeal',
        'victims',
        'years',
        'please',
        'today',
        'million',
        'epicentre',
        'natural',
        'malta',
        'experts',
        'donations',
        'devastation',
        'affected',
        'facebook',
        'towns',
        'google',
        'missing',
        'teams',
        'rescue',
        'thousands',
        'urgent',
        'since',
        'horror',
        'emerges',
        'death'
        ],

    # 'violence_kw': [
    #     'shooting', 'killing', 'explosion', 'bomb', 'death', 'suicide', 'kill',
    #     'dead', 'terror', 'horror', 'terrorist', 'gunman', 'gunmen', 'injured',
    #     'outcry', 'murdered', 'captive', 'massacre'],

    # 'political_kw': [
    #     'confilct', 'damage', 'confront', 'fascist', 'marxist', 'nazi', 'white',
    #     'religions', 'muslim', 'sheiite', 'unrest', 'extremist', 'extremism',
    #     'bigot', 'hatred', 'solidarity'],

    'protest': [
        'protest',
        'baltimore',
        'police',
        'death',
        'people',
        'violence',
        'violent',
        'peaceful',
        'freddie',
        'freddiegray',
        'ethiopian',
        'black',
        'racism',
        'turns',
        'thousands',
        'arrested',
        'blackpool',
        'custody',
        'brutality',
        'farmer',
        'march',
        'drone',
        'charlie',
        'israel',
        'hebdo',
        'today',
        'israeli',
        'nuclear',
        'suicide',
        'office',
        'union',
        'writers',
        'baltimoreriots',
        'square',
        'delhi',
        'mayday',
        'israelis',
        'hundreds',
        'landing',
        'solidarity',
        'congress',
        'pitch',
        'blacklivesmatter',
        'protests',
        'telaviv',
        'right',
        'protesters',
        'justice',
        'ferguson',
        'injured'
        ],

    # 'medical_kw': [
        # 'ebola', 'virus', 'epidemic', 'infection'],

    'traffic': [
        'traffic',
        'update',
        'accident',
        'sydney',
        'cruise',
        'congestion',
        'heavy',
        'along',
        'makati',
        'direction',
        'opposite',
        'angeles',
        'puyat',
        'carnival',
        'makatitraffic',
        'heads',
        'spirit',
        'ayala',
        'cleared',
        'south',
        'partners',
        'gridlocks',
        'north',
        'moving',
        'reporting',
        'drive',
        'lanes',
        'nearby',
        'swell',
        'helped',
        'social',
        'drivers',
        'crash',
        'duval',
        'closed',
        'highway',
        'towards',
        'delay',
        'morning',
        'blocked',
        'nashville',
        'minor',
        'incident',
        'harbour',
        'lights',
        'alert',
        'tractor',
        'broward',
        'vehicle',
        'delays'
        ]
    }

category_list = ['war', 'disaster', 'protest', 'traffic']

cluster_dir = 'clustered_dated_files'
cluster_head_fn = 'head'
information_gain_dir = 'information_gain_files'
token_freq_fn = 'token_frequency'


def cmp_dates(it1, it2):
    date_format = "%Y-%m-%d-%H"
    time1 = datetime.strptime(it1, date_format)
    time2 = datetime.strptime(it2, date_format)
    if time1 < time2:
        return -1
    if time1 > time2:
        return 1
    return 0


def cal_distance(dic, category_key):
    comp_dic = dict(Counter(keyword_bags[category_key]))
    base_dic = {}
    # for k in dic.keys():
    #     if k in comp_dic.keys():
    #         base_dic[k] = dic[k]
    base_dic = dic
    if len(base_dic) == 0:
        return 1.0
    return caldist_cosine(base_dic, comp_dic)


def tokenize(tweet_txt):
    stops = set(stopwords.words())
    text = string.lower(tweet_txt.encode('ascii', 'ignore'))
    text = re.sub(r'http:(.*)|https:(.*)', '', text)
    text = re.sub(r'@([A-Za-z0-9_]+)', '', text)
    text = re.sub(r'&amp;', 'and', text)
    text = string.replace(text, '\n', ' ')
    for ch in """":#%/;-?.()@!""":
        text = string.replace(text, ch, ' ')
    tokens = nltk.word_tokenize(text)
    # tokens = text.split(' ')
    tokens = [w for w in tokens if w not in stops]
    tokens = [w for w in tokens if len(w) > 4]
    return tokens


def classify(data_dir):
    event_classification_list = []
    fi_dir = os.path.join(data_dir, information_gain_dir)
    event_dirs = os.listdir(fi_dir)

    def _local_cmp(it1, it2):
        name1 = it1.split('_')[1]
        name2 = it2.split('_')[1]
        return cmp_dates(name1, name2)
    event_dirs.sort(cmp=_local_cmp)

    for event_dir in event_dirs:
        fi = open(os.path.join(fi_dir, event_dir, token_freq_fn))
        temp_classification = []
        for line in fi:
            temp_dic = ast.literal_eval(line.strip())
            temp_class = [temp_dic['event']]
            for i in range(len(category_list)):
                temp_class.append(cal_distance(temp_dic['token_counter'], category_list[i]))
            if min(temp_class[1:]) >= 0.88:
                temp_class.append(0)
            else:
                temp_class.append(temp_class.index(min(temp_class[1:])))
            temp_classification.append(temp_class)
        event_classification_list.append(temp_classification)
    return event_classification_list


def test(data_dir):
    date_dirs = os.listdir(os.path.join(data_dir, cluster_dir))
    date_dirs.sort(cmp=cmp_dates)
    term_freq_counter = Counter()
    for dd in date_dirs:
        print >> sys.stderr, dd
        fn = os.path.join(data_dir, cluster_dir, dd, cluster_head_fn)
        tweets = util.read_and_parse_tweets_from_file(fn)
        print >> sys.stderr, len(tweets)
        i = 0
        tokens = []
        for t in tweets:
            tweet_txt = t['text']
            tokens.extend(tokenize(tweet_txt))
            # temp_counter = Counter(tokens)
            # term_freq_counter += temp_counter
            i += 1
            if i % 100 == 0:
                print >> sys.stderr, i
        term_freq_counter += Counter(tokens)
    return term_freq_counter.most_common(50)


if __name__ == '__main__':
    event_classification_list = classify(sys.argv[1])
    fo = open(sys.argv[2], 'w')
    pp(event_classification_list)
    val_list = ['o', 'w', 'd', 'p', 't']
    for item in event_classification_list:
        cat_str = ''
        for i in item:
            cat_str += str(val_list[i[5]]) + ' '
        print >> sys.stderr, cat_str.strip()
        print >> fo, cat_str.strip()

#    fo = open(sys.argv[2], 'w')
#    category_key = sys.argv[3]
#    correctly_classified_cnt = 0.0
#    for item in event_classification_list:
#        if item[5] == category_list.index(category_key) + 1:
#            correctly_classified_cnt += 1.0
#    non_classifiable_cnt = 0.0
#    for item in event_classification_list:
#        if item[5] == -1:
#            non_classifiable_cnt += 1.0
#    print >> fo, 'No. correctly classified: {}'.format(correctly_classified_cnt)
#    print >> fo, 'No. unclassifiable: {}'.format(non_classifiable_cnt)
#    print >> fo, 'Total No.: {}'.format(len(event_classification_list))
#    print >> fo, 'Accuracy rate: {}'.format(correctly_classified_cnt / (len(event_classification_list) - non_classifiable_cnt))
#    print >> fo, 'Non classifiable rate: {}'.format(non_classifiable_cnt / len(event_classification_list))
#    print >> fo, 'Overall Accuracy: {}'.format(correctly_classified_cnt / len(event_classification_list))
#    print >> fo, event_classification_list
