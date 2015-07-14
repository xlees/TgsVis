#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

os.chdir("..")
sys.path.insert(0,".")


if __name__ == '__main__':
    with open("data/week_adj.txt","r") as f:
        first = f.readline()
        print first[:10]
