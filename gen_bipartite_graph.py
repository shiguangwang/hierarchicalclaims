import os
import ast
from datetime import datetime
from pprint import pprint as pp
import sys


def gen_signature(sig_list, slot_idx):
    set_sig = set([])
    for it in sig_list:
        tokens = it.split('_')
        set_sig.add(tokens[0])
        set_sig.add(tokens[1])
    signat = str(slot_idx) + '_'
    for elem in set_sig:
        signat += str(elem) + '_'
    return signat[:-1]


def gen_protests(data_dir):
    node_dir = 'consolidated_events_token_frequency'
    edge_dir = 'event_tracking'
    alg = 'jacard'
    thresh = 0.85

    out_dir = 'bipartite_temp'
    node_out_fn = 'nodes.txt'
    edge_out_fn = 'edges.txt'

    node_list = []
    edge_list = []

    if not os.path.exists(os.path.join(data_dir, out_dir)):
        os.makedirs(os.path.join(data_dir, out_dir))

    node_dir = os.path.join(data_dir, node_dir)
    node_fn_list = os.listdir(node_dir)

    def local_cmp(it1, it2):
        date_format = '%Y-%m-%d-%H'
        time1 = datetime.strptime(it1, date_format)
        time2 = datetime.strptime(it2, date_format)
        if time1 < time2:
            return -1
        if time1 < time2:
            return 1
        return 0
    node_fn_list.sort(cmp=local_cmp)

    slot_idx = 0
    for fn in node_fn_list:
        slot_idx += 1
        node_list.append([])
        fi = open(os.path.join(node_dir, fn))
        signature_list = ast.literal_eval(fi.readline().strip())
        for item in signature_list:
            temp_sig = item['signature']
            signat = gen_signature(temp_sig, slot_idx)
            node_list[len(node_list) - 1].append(signat)
    # pp(node_list)
    # print(len(node_list))
    print(slot_idx)

    fo = open(os.path.join(data_dir, out_dir, node_out_fn), 'w')
    print >> fo, node_list
    fo.close()

    edge_dir = os.path.join(data_dir, edge_dir)
    edge_fn_list = os.listdir(edge_dir)
    alg_fn_list = [fn for fn in edge_fn_list if fn.split('_')[2] == alg]

    def local_cmp1(it1, it2):
        date_format = '%Y-%m-%d-%H'
        time1 = datetime.strptime(it1.split('_')[0], date_format)
        time2 = datetime.strptime(it2.split('_')[0], date_format)
        if time1 < time2:
            return -1
        if time1 > time2:
            return 1
        return 0
    alg_fn_list.sort(cmp=local_cmp1)
    slot_idx = 0
    for fn in alg_fn_list:
        slot_idx += 1
        fi = open(os.path.join(edge_dir, fn))
        dist_list = ast.literal_eval(fi.readline().strip())
        for item in dist_list:
            if item[2] < thresh:
                edge_list.append((gen_signature(item[0], slot_idx), gen_signature(item[1], slot_idx + 1)))
        fi.close()
    print(slot_idx)
    # pp(edge_list)
    fo = open(os.path.join(data_dir, out_dir, edge_out_fn), 'w')
    print >> fo, edge_list
    fo.close()


if __name__ == '__main__':
    gen_protests(sys.argv[1])
