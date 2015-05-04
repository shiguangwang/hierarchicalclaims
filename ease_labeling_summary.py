import os
import sys


def process(fn, data_dir):
  fi = open(os.path.join(data_dir, fn))
  outdir = 'event_raw_tweets_summary'
  if not os.path.exists(os.path.join(data_dir, outdir)):
      os.makedirs(os.path.join(data_dir, outdir))
  tokens = fn.split('_')
  fo1 = open(os.path.join(data_dir, outdir, tokens[0] + '_content.txt'), 'w')


  content_dic = {}
  for line in fi:
    line = line.strip()
    tline = line.split('\t')
    if len(tline) ==2:
      if tline[0] not in content_dic.keys():
        content_dic[tline[0]] = set([])
      content_dic[tline[0]].add(tline[1])

  for key in content_dic.keys():
    print >> fo1, key
    for item in content_dic[key]:
      print >> fo1, item
    print >> fo1, '\n'

  fi.close()
  fo1.close()


def process_shell(data_dir):
  flist = os.listdir(data_dir)
  for fn in flist:
    if not os.path.isdir(os.path.join(data_dir, fn)):
      tokens = fn.split('_')
      if tokens[1] == 'hist':
        print >> sys.stderr, fn
        process(fn, data_dir)


if __name__ == '__main__':
  data_dir = sys.argv[1]
  process_shell(data_dir)
