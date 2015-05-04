import ast
from pprint import pprint as pp


def consolidating(raw_pairs_fn, distance_scores_fn, threshold):
    #  Here raw_pairs is the dictionary as defined in pairs.
    # The distance_scores, are the list tht defined in d1_jacard
    # threshold is the threshold selected.
    distance_scores_fi = open(distance_scores_fn)
    distance_scores = ast.literal_eval(distance_scores_fi.readline().strip())
    
#    kwpair_set = set([])
#    for item in distance_scores:
#        kwpair_set.add(item[0])
#        kwpair_set.add(item[1])

    raw_pairs = []
    raw_pairs_fi = open(raw_pairs_fn)
    for line in raw_pairs_fi:
        dic = ast.literal_eval(line.strip())
        kw_array = dic['kw_pair']
        key = kw_array[0] + '_' + kw_array[1]
#        if key in kwpair_set:
#            new_dic = {}
#            new_dic['tweet_ids'] = set(dic['tweet_ids'])
#            new_dic['signature'] = set([key])
#            raw_pairs.append(new_dic)
        new_dic = {}
        new_dic['tweet_ids'] = set(dic['tweet_ids'])
        new_dic['signature'] = set([key])
        raw_pairs.append(new_dic)
#    print len(raw_pairs)
#    pp([item['signature'] for item in raw_pairs])
    
    classified = {}
    for item in distance_scores:
        classified[(item[0], item[1])] = item[2] < threshold

    for key in classified:
        if classified[key] is True:
#            print key
            existing = False
            for item in raw_pairs:
                if key[0] in item['signature']:
                    existing = True
#                    print '''\tThe length of the raw_pairs is {}'''.format(len(raw_pairs))
#                    print '\tThe item signature is {}'.format(item['signature'])
                    if key[1] in item['signature']:
#                        print '\tkey[1] is also in the signature'
                        break
                    else:
#                        print '\tkey[1] is not in the signature'
                        for item2 in raw_pairs:
                            if key[1] in item2['signature']:
#                                print '''\t\tThe item signature is 
#                                    {}'''.format(item2['signature'])
                                raw_pairs.remove(item2)
#                                print len(raw_pairs)
#                                print item2['signature']
#                                print '''\t\tThe length of the raw_pairs is {}'''.format(len(raw_pairs))
                                item['tweet_ids'] = \
                                    item['tweet_ids'].union(item2['tweet_ids'])
                                item['signature'] = \
                                    item['signature'].union(item2['signature'])
#                                pp([item['signature'] for item in raw_pairs])
                                break
                    break
            if not existing:
                print 'Error got!!!!'
    for item in raw_pairs:
        item['tweet_ids'] = list(item['tweet_ids'])
        item['signature'] = list(item['signature'])
    return raw_pairs


def main():
    raw_pairs_fn = \
        '../data/protests/information_gain_files/2015-04-09-6_2015-04-10-6/pairs'
    distance_scores_fn = '../data/protests/cal_dist/d1_jacard'
    threshold = 0.9
    raw_pairs = consolidating(raw_pairs_fn, distance_scores_fn, threshold)
    from pprint import pprint as pp
    pp(raw_pairs)


if __name__ == '__main__':
    main()
