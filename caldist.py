#!/usr/bin/python27

import sys
import numpy as np
import math

top_k = 15


def caldist_hist(dict1, dict2):
    from collections import Counter
    counter1 = Counter(dict1)
    counter2 = Counter(dict2)
    counter1.subtract(counter2)
    nume = sum(np.absolute(counter1.values()))
    denom = sum(dict1.values()) + sum(dict2.values())
    #print dict1
    #print dict2
    #print denom
    if denom == 0:
        return sys.float_info.max
    return nume * 1.0 / denom


def caldist_jacard(dict1, dict2):
    print >> sys.stderr, 'jaccard' + str(dict1)
    print >> sys.stderr, 'jaccard' + str(dict2)
    set1 = set(dict1.keys())
    set2 = set(dict2.keys())
    setu = set1.union(set2)
    seti = set1.intersection(set2)
    if len(setu) == 0:
        return sys.float_info.max
    ret = len(seti) * 1.0 / len(setu);
    print >> sys.stderr, 'jaccard' + str(ret)
    return 1 - len(seti) * 1.0 / len(setu)
    

def caldist_kl_core(dict1, dict2):
    '''
    caldist_kl_core function calculates the kl distance of dict2 from dict1. The 
    assumptions maded by the core function is that the keys in dict1 and dict2 
    are the same and the counts for any key in any dict is positive.

    The function used in calculating kl divergence is \sum_i p_i \log p_i/q_i, 
    and the log is natural log.
    '''
    import math
    sum1 = float(sum(dict1.values()))
    sum2 = float(sum(dict2.values()))
    kl = 0.0
    for k in dict1.keys():
        p = dict1[k] / sum1
        q = dict2[k] / sum2
        kl = kl + p * math.log(p/q)
    return kl
    

def caldist_kl(dic1, dic2):
    '''
    caldist_kl1 calculates the kl divergence of dict2 from dict1, and omit all 
    the keys that are not in common.
    
    Warning: This function will modify the data.
    '''
    import copy
    dict1 = copy.deepcopy(dic1)
    dict2 = copy.deepcopy(dic2)
    for k in dict1.keys():
        if k not in dict2.keys():
            dict1.pop(k)
    for k in dict2.keys():
        if k not in dict1.keys():
            dict2.pop(k)
    return caldist_kl_core(dict1, dict2)


def caldist_kl_norm(dic1, dic2):
    '''
    caldist_kl_norm1 add the count for each element by 1 if the key sets of 
    dict1 and dict2 are different. It calculates the kl divergence of dict2 from 
    dict1.

    Warning: This function will modify the data.
    '''
    import copy
    dict1 = copy.deepcopy(dic1)
    dict2 = copy.deepcopy(dic2)
    if not (dict1.keys() == dict2.keys()):
        key_union = set(dict1.keys()).union(set(dict2.keys()))
        for k in key_union:
            if k in dict1.keys():
                dict1[k] = dict1[k] + 1
            else: 
                dict1[k] = 1
            if k in dict2.keys():
                dict2[k] = dict2[k] + 1
            else:
                dict2[k] = 1
    return caldist_kl_core(dict1, dict2)


def caldist_cosine(dic1, dic2):
    keyset = set(dic1.keys()).intersection(set(dic2.keys()))
    nume = 0.0
    for key in keyset:
        nume = nume + dic1[key] * dic2[key]
    denom = 0.0
    for key in dic1.keys():
        denom = denom + dic1[key] * dic1[key]
    denom = math.sqrt(denom)
    denom2 = 0.0
    for key in dic2.keys():
        denom2 = denom2 + dic2[key] * dic2[key]
    denom2 = math.sqrt(denom2)
    return 1 - nume / (denom * denom2)


method = {
        'hist': caldist_hist,
        'jacard': caldist_jacard,
#        'kl': caldist_kl,
        'klnorm': caldist_kl_norm,
        'cosine': caldist_cosine
        }


def caldist(dict1, dict2, alg='hist'):
    '''
    caldist computes the distance value between two dictionaries, where the keys 
    are the tokens and the values are the corresponding appearance of each 
    token. The smaller the distance value is 
    the more similar the two dictionaries are.

    The user can select any of the define method to compute the distance 
    value, the default method to compute the distance value is 'hist'.
    The other possible methods are 'jacard', 'kl', and 'klnorm', 'cosine'.
    '''
    from collections import Counter
    c1 = Counter(dict1)
    dic1 = dict(c1.most_common(top_k))
    c2 = Counter(dict2)
    dic2 = dict(c2.most_common(top_k))
    if alg not in method: 
        print >> sys.stderr, '''the input method {0} is not defined, please 
        select 'hist', 'jacard', 'kl', or 'klnorm' as the distance
        computing method'''.format(alg)
    return method[alg](dic1, dic2)


def parse_file(input_file_name):
    import ast
    fi = open(input_file_name)
    dlist = []
    for line in fi:
        l = ast.literal_eval(line.strip())
        dlist.append(l)
    return dlist


def cal_dist_perday(day1, alg='hist'):
    results = []
    cnt = len(day1)
    for i in range(0, cnt):
        for j in range(i + 1, cnt):
            result = [day1[i]['event'], day1[j]['event']]
            result.append(caldist(day1[i]['token_counter'], 
            day1[j]['token_counter'], alg))
            results.append(result)

    def local_cmp(item1, item2):
        if item1[2] < item2[2]:
            return -1
        if item1[2] == item2[2]:
            return 0
        return 1

    results.sort(cmp=local_cmp)
    return results


def cal_dist_twoday(day1, day2, alg='hist'):
    results = []
    for item1 in day1:
        for item2 in day2:
            result = []
            result.append(item1['signature'])
            result.append(item2['signature'])
            result.append(caldist(item1['tokens'], 
                item2['tokens'], alg))
            results.append(result)

    def local_cmp(item1, item2):
        if item1[2] < item2[2]:
            return -1
        if item1[2] == item2[2]:
            return 0
        return 1

    results.sort(cmp=local_cmp)
    return results
        

def testing():
    dict1 = {'a':3, 'b':5, 'c':10, 'd':3, 'e': 6}
    dict2 = {'a':10, 'b':4, 'c':4, 'e':7, 'f':16, 'h':17, 'i':22}
    dict3 = {'a':3, 'b':5, 'c':10, 'd':3, 'e': 6}

    print caldist(dict1, dict2, alg='hist')
    print caldist(dict1, dict3, alg='hist')

    print caldist(dict1, dict2, alg='jacard')
    print caldist(dict1, dict3, alg='jacard')

    import copy as cp
    dic1 = cp.deepcopy(dict1)
    dic2 = cp.deepcopy(dict2)
    dic3 = cp.deepcopy(dict3)
    print caldist(dic1, dic2, alg='kl')
    print caldist(dic1, dic3, alg='kl')
    
    dic1 = cp.deepcopy(dict1)
    dic2 = cp.deepcopy(dict2)
    dic3 = cp.deepcopy(dict3)
    print caldist(dic1, dic2, alg='klnorm')
    print caldist(dic1, dic3, alg='klnorm')


if __name__ == '__main__':
    #testing()
    file_prefix = sys.argv[1]
    file_name = file_prefix + sys.argv[2] 
    dlist_raw = parse_file(file_name)
#    dlist = []
#    dlist.append(dlist_raw[4])
#    dlist.append(dlist_raw[1])
#    dlist.append(dlist_raw[2])
#    dlist.append(dlist_raw[0])
    dlist = dlist_raw
    for i in range(len(dlist)):
        d = dlist[i]
        results = cal_dist_perday(d, 'hist')
        fo = open(file_prefix + 'd' + str(i + 1) + '_hist.txt', 'w')
        print >> fo,  results
        results = cal_dist_perday(d, 'jacard')
        fo = open(file_prefix + 'd' + str(i + 1) + '_jacard.txt', 'w')
        print >> fo, results
        results = cal_dist_perday(d, 'kl')
        fo = open(file_prefix + 'd' + str(i + 1) + '_kl.txt', 'w')
        print >> fo, results
        results = cal_dist_perday(d, 'klnorm')
        fo = open(file_prefix + 'd' + str(i + 1) + '_klnorm.txt', 'w')
        print >>fo, results
    for i in range(len(dlist) - 1):
        d1 = dlist[i]
        d2 = dlist[i + 1]
        results = cal_dist_twoday(d1, d2, 'hist')
        fo = open(file_prefix + 'd' + str(i + 1) + 'd' + str(i + 2) + 
        '_hist.txt', 'w')
        print >> fo, results[0:20]
        results = cal_dist_twoday(d1, d2, 'jacard')
        fo = open(file_prefix + 'd' + str(i + 1) + 'd' + str(i + 2) + 
        '_jacard.txt', 'w')
        print >> fo, results[0:20]
        results = cal_dist_twoday(d1, d2, 'kl')
        fo = open(file_prefix + 'd' + str(i + 1) + 'd' + str(i + 2) + '_kl.txt', 
                'w')
        print >> fo, results[0:20]
        results = cal_dist_twoday(d1, d2, 'klnorm')
        fo = open(file_prefix + 'd' + str(i + 1) + 'd' + str(i + 2) + 
        '_klnorm.txt', 'w')
        print >> fo, results[0:20]
    
