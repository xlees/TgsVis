#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

root_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.pardir))
os.chdir(root_dir)
sys.path.insert(0,root_dir)
# print root_dir

# os.chdir("..")
# sys.path.insert(0,".")

import re
from collections import Counter
import pandas as pd
from app.helper import EvilTransform

def transform(row):
    loc = EvilTransform.transform(row['Y'],row['X'])

    # print type(row['KKMC']), row['KKMC']

    return (str(row['CLOUD_ID']), row['KKMC'].decode("gbk").encode("utf8"))
    # return (row['KKID'], row['KKMC'].decode("gbk").encode("utf8"), repr(int(row['CLOUD_ID'])), loc[0], loc[1])

def get_tgs_info(fname):
    cols = ['KKID','KKMC','CLOUD_ID','X','Y']

    tgs_info = pd.read_csv(fname)[cols]
    res = tgs_info.apply(transform, axis=1).tolist()

    return dict(res)


def read_tgs_volume(fname):
    ret = Counter()
    cnt = 3

    with open(fname,"r") as f:
        for line in f.readlines():
            tmp = re.split("\s+",line)
            # if cnt > 0:
            #     print tmp,":",line
            #     cnt -= 1

            term = eval(tmp[1])
            ret[str(term[0])] = int(term[1])

    return ret

def calc_assoc(volume, adj):
    assoc = Counter()
    for edge,vol in adj.iteritems():
        try:
            assoc[edge] = float(vol) / max(volume[edge[0]], volume[edge[1]])
        except ZeroDivisionError,e:
            print e.args

    return assoc

def read_adj(fname):
    ret = Counter()
    cnt = 2

    p_main = re.compile("\d+,")
    p_indegree = re.compile("\d+-\d+")

    with open(fname,"r") as f:
        for line in f.readlines():
            try:
                main = p_main.search(line).group()[:-1]
            except AttributeError,e:
                print e.args[0], ":", line
                continue

            indegree = p_indegree.findall(line)
            for d in indegree:
                item = d.split("-")
                ret[(item[0],main)] = float(item[1])

            if cnt > 0:
                print main
                print ret.most_common(2)
                cnt -= 1

    return ret


if __name__ == '__main__':
    tgs_info = get_tgs_info("tgsinfo.csv")
    print "%d tgs fetched." % (len(tgs_info))

    fvolume = "data/volume.txt"
    volume = read_tgs_volume(fvolume)

    volume0 = volume.most_common()
    for i in xrange(10):
        print "%s,%s: %d" % (volume0[i][0], tgs_info[volume0[i][0]], volume0[i][1])

    # read edges
    adj = read_adj("data/week_adj.txt")
    print '\nmost common edges:'
    for val in adj.most_common(10):
        try:
            print "%s,%s -> %s,%s: %d" % (val[0][0], tgs_info[val[0][0]],
                                          val[0][1], tgs_info[val[0][1]],
                                          val[1])
        except KeyError,e:
            print e.args
            # print adj[i]
            #

    print '\nmost common assoc...'
    assoc = calc_assoc(volume,adj)
    for val in assoc.most_common(20):
        if val[0][0]=='0' or val[0][1]=='0':
            continue

        try:
            # print adj[i][0][0], adj[i][0][1]
            print "%s,%s -> %s,%s: %.6f" % (val[0][0], tgs_info[val[0][0]],
                                          val[0][1], tgs_info[val[0][1]],
                                          val[1])
        except KeyError,e:
            print e.args

    with open("assoc.txt","w") as f:
        for val in assoc.most_common():
            if val[0][0]=='0' or val[0][1]=='0':
                continue

            if val[0][0] == val[0][1]:
                continue

            line = "%s,%s -> %s,%s: %.8f\n" % (val[0][0], tgs_info[val[0][0]],
                                              val[0][1], tgs_info[val[0][1]],
                                              val[1])
            f.write(line)
    print 'write finished.'
