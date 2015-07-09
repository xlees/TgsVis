#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

import re


if __name__ == '__main__':
    fout = open("kk2.txt","w")
    fin = open("kk.txt","r")

    p = re.compile("\d+,")

    result = []
    for line in fin.readlines():
        # line = fp.readline()

        # segs = line.strip().split(",")
        cid = p.search(line.strip()).group()
        # print cid
        result.append(cid[:-1])

    print result[-10:]

    fout.write("\n".join(result))

    fin.close()
    fout.close()
