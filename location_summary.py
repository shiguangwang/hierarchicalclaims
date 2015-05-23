import os
import sys
import ast
from pprint import pprint as pp

in_dir = 'consolidated_localization'


def summary(data_dir):
    dlist = os.listdir(os.path.join(data_dir, in_dir))
    location_list = set([])
    
    for fn in dlist:
        fi = open(os.path.join(data_dir, in_dir, fn))
        for line in fi:
            d = ast.literal_eval(line.strip())
            location_list.add(d['address_formatted'])

    pp(location_list)
    print len(location_list) - 1


if __name__ == '__main__':
    summary(sys.argv[1])
