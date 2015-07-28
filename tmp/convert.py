#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

root_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.pardir))
os.chdir(root_dir)
sys.path.insert(0,root_dir)

import re
# from shared import *
from dateutil.parser import parse


def wrangle(o,d):
    odata = []
    with open(o,"r") as f:
        for line in f.readlines():
            tmp = line[:-1].split(",")
            odata.append((parse(tmp[0]),tmp[1]))

    print 'o data: %d records.' % (len(odata))

    ddata = []
    with open(d,"r") as f:
        for line in f.readlines():
            tmp = line[:-1].split(",")
            ddata.append((parse(tmp[0]),tmp[1]))

    print 'o data: %d records.' % (len(ddata))

    odata.sort(key=lambda x: x[0], reverse=False)
    ddata.sort(key=lambda x: x[0], reverse=False)

    with open(os.path.join(root_dir,"result","o1.txt"),"w") as fo, open(os.path.join(root_dir,"result","d1.txt"),"w") as fd:
        for elem in odata:
            fo.write("%s,%s\n" % (elem[0].strftime("%Y-%m-%d %H:%M:%S"),elem[1]))
        for elem in ddata:
            fd.write("%s,%s\n" % (elem[0].strftime("%Y-%m-%d %H:%M:%S"),elem[1]))

    print 'finished.'


if __name__ == '__main__':
    oname = os.path.join(root_dir,"result","o.txt")
    dname = os.path.join(root_dir,"result","d.txt")

    wrangle(oname,dname)

    # fout = open("kk2.txt","w")
    # fin = open("kk.txt","r")

    # p = re.compile("\d+,")

    # result = []
    # for line in fin.readlines():
    #     # line = fp.readline()

    #     # segs = line.strip().split(",")
    #     cid = p.search(line.strip()).group()
    #     # print cid
    #     result.append(cid[:-1])

    # print result[-10:]

    # fout.write("\n".join(result))

    # fin.close()
    # fout.close()
