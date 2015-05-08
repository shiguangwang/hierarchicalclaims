import os
import sys
from datetime import datetime


def date_cmp(it1, it2):
    date_format = '%Y-%m-%d-%H'
    time1 = datetime.strptime(it1, date_format)
    time2 = datetime.strptime(it2, date_format)
    if time1 < time2:
        return -1
    if time1 > time2:
        return 1
    return 0


def preprocess(data_dir):
    localization_merged_chunks = 'localization_merged_date_chunked_files'
    if not os.path.exists(os.path.join(data_dir, localization_merged_chunks)):
        os.makedirs(os.path.join(data_dir, localization_merged_chunks))
    date_chunked_files = os.listdir(os.path.join(data_dir, 'date_chunked_files'))
    date_chunked_files.sort(cmp=date_cmp)
    for i in range(len(date_chunked_files) - 3):
        print >> sys.stderr, date_chunked_files[i]
        fo = open(os.path.join(data_dir, localization_merged_chunks, date_chunked_files[i]), 'w')
        for j in range(4):
            fi = open(os.path.join(data_dir, 'date_chunked_files', date_chunked_files[i+j]))
            for line in fi:
                print >> fo, line.strip()

    localization_merged_desc = 'localization_merged_clustered_dated_files'
    if not os.path.exists(os.path.join(data_dir, localization_merged_desc)):
        os.makedirs(os.path.join(data_dir, localization_merged_desc))
    cluster_files = os.listdir(os.path.join(data_dir, 'clustered_dated_files'))
    cluster_files.sort(cmp=date_cmp)
    for i in range(len(cluster_files) - 3):
        print >> sys.stderr, cluster_files[i]
        if not os.path.exists(os.path.join(data_dir, localization_merged_desc, cluster_files[i])):
            os.makedirs(os.path.join(data_dir, localization_merged_desc, cluster_files[i]))
        fo = open(os.path.join(data_dir, localization_merged_desc, cluster_files[i], 'desc'), 'w')
        for j in range(4):
            fi = open(os.path.join(data_dir, 'clustered_dated_files', cluster_files[i + j], 'desc'))
            for line in fi:
                print >> fo, line.strip()


if __name__ == '__main__':
    preprocess(sys.argv[1])