import sys
import os
from clustering import do_clustering

in_dir = 'date_chunked_files'
out_dir = 'cluster_before_sliding'


def cluster(data_dir):
    if not os.path.exists(os.path.join(data_dir, out_dir)):
        os.makedirs(os.path.join(data_dir, out_dir))
    dated_chunk_files = os.listdir(
        os.path.join(data_dir, in_dir))
    for chunk_file in dated_chunk_files:
        dated_file_name = os.path.join(
            data_dir, in_dir, chunk_file)
        if os.path.isfile(dated_file_name):
            if not os.path.exists(os.path.join(data_dir,
                                               out_dir,
                                               chunk_file)):
                os.makedirs(os.path.join(data_dir, out_dir, chunk_file))
            do_clustering(
                dated_file_name,
                os.path.join(data_dir, out_dir, chunk_file, 'plain'),
                os.path.join(data_dir, out_dir, chunk_file, 'head'),
                os.path.join(data_dir, out_dir, chunk_file, 'plain_head'),
                os.path.join(data_dir, out_dir, chunk_file, 'source'),
                os.path.join(data_dir, out_dir, chunk_file, 'desc'),
                os.path.join(data_dir, out_dir, chunk_file, 'author_cluster'),
                os.path.join(data_dir, out_dir, chunk_file, 'output_tweets'),
                os.path.join(data_dir, out_dir, chunk_file, 'tokens'),
                os.path.join(data_dir, out_dir, chunk_file, 'length'))

if __name__ == '__main__':
    cluster(sys.argv[1])
