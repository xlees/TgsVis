#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

# print sys.path

from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from helper import EvilTransform
from collections import Counter

def transform(row):
    loc = EvilTransform.transform(row['Y'],row['X'])

    # print type(row['KKMC']), row['KKMC']

    return (row['KKID'], row['KKMC'].decode("gbk").encode("utf8"), repr(int(row['CLOUD_ID'])), loc[0], loc[1])


if __name__ == '__main__':

    cols = ['KKID','KKMC','CLOUD_ID','X','Y']
    tgs_info = pd.read_csv("../tgsinfo.csv")[cols]
    res = tgs_info.apply(transform, axis=1).tolist()

    print "%d rows." % (len(res))

    kkid = Counter()
    cid = Counter()
    for row in res:
        kkid[row[0]] += 1
        cid[row[2]] += 1

    print 'len of kkid: %d' % (len(kkid))
    print 'len of cid: %d' % (len(cid))

    print "kkid detail: ", kkid
    print "cid detail: ", cid
