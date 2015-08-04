#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

root_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.pardir))
os.chdir(root_dir)
sys.path.insert(0,root_dir)
sys.path.insert(0, os.path.join(root_dir,"app","gen-py"))


import numpy as np
# np.seterr(divide='ignore', invalid='ignore')


import multiprocessing as mp
import random
# from app.helper import EvilTransform
import math
import shared as shd
from datetime import datetime
import hbase.ttypes as htt
import re
import struct

host, port = "10.2.25.115", 9090


def load_adj_info(indegree=True):
    """
    0--indegree
    1--outdegree
    return adjcency list.
    """
    tgsinfo = shd.read_tgs_info()

    p_main = re.compile("\d+,")
    p_degree = re.compile("\d+-\d+")

    if indegree:
        fname = os.path.join(root_dir,"data","adj_indegree.txt")
    else:
        fname = os.path.join(root_dir,"data","adj_outdegree.txt")

    adj = dict([(cid,[]) for cid in tgsinfo.keys()])
    tgsinfo.clear()

    with open(fname,"r") as f:
        for line in f.readlines():
            try:
                main = p_main.search(line).group()[:-1]
            except AttributeError,e:
                print e.args[0], ":", line
                continue

            degree = p_degree.findall(line)
            for d in degree:
                item = d.split("-")

                if item[0]=='0' or item[0]==main:
                    continue

                adj[main].append((item[0], int(item[1])))

            # adj[main].sort(key=lambda x: x[1], reverse=True)

    return adj

def load_od_data(index, odtype="o"):
    if not odtype in ["o","d"]:
        print 'error: neither o nor d.'
        return []

    fname = os.path.join(root_dir,"data","od",odtype,str(index*10),"part-00000")
    print "open %s " % (fname)

    result = []
    with open(fname,"r") as f:
        for line in f.readlines():
            tmp = line[1:-2].split(",")
            # print tmp

            o = tmp[0]
            cnt = int(tmp[1])

            result.append((o,cnt))

    # sort
    result.sort(key=lambda x:x[1],reverse=True)

    return result

def load_edge_info(indegree=True):
    """
    0 -- indegree
    1 -- outdegree
    return adjcency list according to edges list.
    """
    fname = os.path.join(root_dir,"data","adj.txt")

    degree = {}
    with open(fname,"r") as f:
        if indegree:
            for line in f.readlines():
                tmp = line[:-1].split(",")

                start = tmp[0]
                end = tmp[1]
                assoc = float(tmp[2])

                if not degree.has_key(end):
                    degree[end] = [(start,assoc)]
                else:
                    degree[end].append((start,assoc))
        else:
            for line in f.readlines():
                tmp = line[:-1].split(",")

                start = tmp[0]
                end = tmp[1]
                assoc = float(tmp[2])

                if not degree.has_key(start):
                    degree[start] = [(end,assoc)]
                else:
                    degree[start].append((end,assoc))

    # sort
    for cid in degree.keys():
        degree[cid].sort(key=lambda x:x[1], reverse=True)

    return degree

def query_traj(stime,etime, numb):
    tbl = 'tr_plate_jun'

    (client,trpt) = shd.get_thrift_client(host,port)

    begtime = long(time.mktime(stime.timetuple())*1000)
    endtime = long(time.mktime(etime.timetuple())*1000)

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = "RowFilter(=, 'substring:%s') AND KeyOnlyFilter()" % (numb)
    scan.filterString = "KeyOnlyFilter()"
    scan.startRow = struct.pack(">10sQ", numb, begtime)
    scan.stopRow = struct.pack(">10sQ", numb, endtime)

    trpt.open()

    scanner = client.scannerOpenWithScan(tbl, scan, None)

    result = []
    while 1:
        dataset = client.scannerGetList(scanner, buf_max_size)

        for elem in dataset:
            result.append({
                'passtime': datetime.fromtimestamp(struct.unpack('>Q',elem.row[10:18])[0]/1000.0).strftime('%Y-%m-%d %H:%M:%S'),
                'cid': unbyte_cid(elem.row[18:20]),
            })

        if len(dataset) < buf_max_size:
            break

    trpt.close()

    result.sort(key=lambda x: x['passtime'],reverse=False)

    print 'traj: %d records fetched.' % (len(result))

    return result

def _rad(d):
   return d * math.pi / 180.0

def _round(d):
  return math.floor(d + 0.5)

def _calc_dist(lng1,lat1, lng2,lat2):
    radius = 6378.137       # km

    radLat1 = _rad(lat1)
    radLat2 = _rad(lat2)

    a = radLat1 - radLat2
    b = _rad(lng1) - _rad(lng2)
    s = 2 * math.asin(math.sqrt(pow(math.sin(a/2),2) +
        math.cos(radLat1)*math.cos(radLat2)*math.pow(math.sin(b/2),2)))

    s *= radius
    s = _round(s * 10000) / 10000

    return s

def reject_outliers(data, m=2.8):
    """
    Simple reject outlier function
    return: the index of non-outliers
    data: numpy array
    """
    if len(data) < 3:
        return np.where(data)

    d = np.abs(data - np.median(data))      # 用中位数来代替平均值
    mdev = np.median(d)

    if np.isnan(d).any():
        print 'nan', mdev

    if not np.isfinite(d).all():
        print 'finite', mdev

    if np.abs(mdev) < 1e-6:
        s = 0.0
    else:
        s = d / mdev

    return np.where(s<m)[0]

def read_traveltime(fname, n_line=-1):
    traveltime = {}

    cnt = 0
    with open(fname,"r") as f:
        for line in f.readlines():
            cnt += 1
            if cnt == n_line:
                break

            tmp = line.split(":")
            pair = eval(tmp[0])

            if "," in tmp[1]:
                traveltime[pair] = np.array(list(eval(tmp[1])))
            elif len(tmp[1]) == 0:
                continue
            else:
                traveltime[pair] = np.array([int(tmp[1])])

    print 'totally %d pairs.' % (len(traveltime))

    return traveltime

def row_handler(row):
    min_val = np.min(row)
    max_val = np.max(row)

    freq, bins = np.histogram(row,
                              bins=np.arange(min_val-10, max_val+10, step=2),
                              density=True)
    amax = np.argmax(freq)
    mu = (bins[amax]+bins[amax+1]) / 2.0
    sigma = (bins[amax+1]-bins[amax]) / 2.0

    cv = np.std(row) / np.mean(row)

    return mu,sigma,cv

def calc_pair(pair,traveltime,thresh=100):
    indx = reject_outliers(traveltime, 3.0)
    if len(indx) < thresh:
        return None

    mu,sigma,cv = row_handler(traveltime[indx])

    return (pair, mu, len(indx), len(traveltime), cv)

def transform(row):
    loc = EvilTransform.transform(row['Y'],row['X'])

    # return (str(row['CLOUD_ID']), row['KKMC'].decode("gbk").encode("utf8"))
    return (row['KKID'],
            row['KKMC'].decode("gbk").encode("utf-8"),
            repr(int(row['CLOUD_ID'])),
            loc[0],
            loc[1])

def filter_pairs():
    # adj = read_adj(os.path.join(root_dir,"adj.txt"))
    # adj1 = adj.most_common(3)
    # print adj1

    speed = Counter()
    with open(os.path.join(root_dir,"pairs.txt"),"r") as f:
        for line in f.readlines():
            tmp = line.split(" ")
            pair = eval(tmp[0][:-1])
            spd = float(tmp[1])

            speed[pair] = spd

    print '%d speed pair fetched.' % (len(speed))

    fspeed = filter(lambda x: x[0]>1 and x[1]<60, speed.most_common())
    print '%d pair filtered.' % (len(fspeed))

    fspeed0 = dict(fspeed)

    # with open(os.path.join(root_dir,"filterd_pairs.txt"),"w") as f:
    #     cnt = 3
    #     for item in fspeed:
    #         pair = (str(item[0][0]),str(item[0][1]))
    #         if cnt > 0:
    #             print pair
    #             cnt -= 1

    #         line = "%s,%s: %.3f\n" % (item[0][0],item[0][1],
    #                                         item[1])
    #         f.write(line)

    p_main = re.compile("\d+,")
    p_indegree = re.compile("\d+-\d+-\d+")
    ret = {}

    with open(os.path.join(root_dir,"adj.txt"),"r") as f:
        for line in f.readlines():
            main = p_main.search(line).group()[:-1]
            indegree = p_indegree.findall(line)

            for item in indegree:
                cid = item.split("-")[0]
                ratio = float(item.split("-")[2])

                if fspeed0.has_key((int(main),int(cid))):
                    ret[(int(main),int(cid))] = {
                        'speed': fspeed0[(int(main),int(cid))],
                        'ratio': ratio,
                    }

    with open(os.path.join(root_dir,"filterd_pairs.txt"),"w") as f:
        for key,val in ret.iteritems():
            line = "%d,%d:%.1f,%.3f\n" % (key[0],key[1],val['ratio'],val['speed'])
            f.write(line)

    print '%d pairs filterd finally.' % (len(ret))

def _stat_first_tgs_single(cid, begtime,endtime):
    tbl = "tr_bay61"

    (client,trpt) = get_thrift_client(host,port)

    trpt.open()

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = "RowFilter(=, 'substring:%s') AND KeyOnlyFilter()" % (numb)
    # scan.filterString = "KeyOnlyFilter()"
    scan.startRow = struct.pack(">2sQ", byte_cid(cid), begtime)
    scan.stopRow = struct.pack(">2sQ", byte_cid(cid), endtime)

    scanner = client.scannerOpenWithScan(tbl,scan,None)

    veh = {}
    while 1:
        dataset = client.scannerGetList(scanner,buf_max_size)
        # print '%d: %d records fetched.' % (cid, len(dataset))

        for elem in dataset:
            vehicle = (elem.row[10:], struct.unpack(">B",elem.columns["cf:"].value[35:36]))

            info = (struct.unpack(">Q", elem.row[2:10])[0], unbyte_cid(elem.row[:2]))
            if not veh.has_key(elem.row[10:]):
                veh[vehicle] = info

            if info[0] < veh[vehicle][0]:
                veh[vehicle] = info

        if len(dataset) < buf_max_size:
            break

    trpt.close()

    print '%s,%s: %d vehicles.' % (mp.current_process().name, cid, len(veh))

    return veh

def _combine(x,y):
    res = {}

    s = set(x) | set(y)
    for veh in s:
        if x.has_key(veh) and y.has_key(veh):
            res[veh] = x[veh] if x[veh][0] < y[veh][0] else y[veh]
        elif x.has_key(veh) and (not y.has_key(veh)):
            res[veh] = x[veh]
        elif y.has_key(veh) and (not x.has_key(veh)):
            res[veh] = y[veh]

    # for veh,info in x.iteritems():
    #     if not y.has_key(veh):
    #         res[veh] = info
    #     else:
    #         if info[0] < y[veh][0]:
    #             res[veh] = info
    #         else:
    #             res[veh] = y[veh]

    return res

import random
def stat_first_tgs(stime,etime):

    begtime = long(time.mktime(stime.timetuple())*1000)
    endtime = long(time.mktime(etime.timetuple())*1000)

    tgsinfo = read_tgs_info()
    vehicles = {}

    test = random.sample(tgsinfo.keys(), 100)

    from multiprocessing.pool import Pool

    pool = Pool()
    result = [pool.apply_async(_stat_first_tgs_single, args=(int(cid),begtime,endtime)) for cid in tgsinfo.keys()]
    pool.close()
    pool.join()

    result1 = [elem.get() for elem in result]

    print 'joining....'
    result2 = reduce(_combine, result1)

    print 'totally %d vehicles. ' % (len(result2))
    # print type(result2)

    c = Counter()
    for veh, info in result2.iteritems():
        c[info[1]] += 1

    print 'writing result into file...'
    with open(os.path.join(root_dir,"result","first_tgs.txt"),"w") as f:
        c1 = c.most_common()
        # print c1[0]
        # for cid,count in c1.iteritems():
        for elem in c1:
            line = "%5d,%6d\n" % (elem[0],elem[1])
            f.write(line)
    print 'finished.'

def query_vehicle_trajetory(numb,ptype,stime,etime):
    tbl = "tr_plate_jun"

    begtime = long(time.mktime(stime.timetuple())*1000)
    endtime = long(time.mktime(etime.timetuple())*1000)

    (client,trpt) = shd.get_thrift_client(host,port)

    trpt.open()

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = "RowFilter(=, 'substring:%s') AND KeyOnlyFilter()" % (numb)
    # scan.filterString = "KeyOnlyFilter()"
    scan.startRow = struct.pack(">10sQ", numb, begtime)
    scan.stopRow = struct.pack(">10sQ", numb, endtime)

    scanner = client.scannerOpenWithScan(tbl,scan,None)

    traj = []
    while 1:
        dataset = client.scannerGetList(scanner,shd.buf_max_size)
        for elem in dataset:
            numb_type = struct.unpack("B",elem.columns['cf:'].value[35:36])[0]
            if numb_type != int(ptype):
                continue

            passtime = datetime.fromtimestamp(struct.unpack(">Q",elem.row[10:18])[0]/1000.0).strftime("%Y-%m-%d %H:%M:%S")
            cid = shd.unbyte_cid(elem.row[18:20])
            drivedir = struct.unpack("B",elem.columns['cf:'].value[37:38])[0]

            traj.append((passtime,cid,drivedir))

        if len(dataset) < shd.buf_max_size:
            break

    trpt.close()

    return traj

def stat_tgs_volume(cid, stime,etime):
    tbl = "tr_bay_jun"

    b_cid = byte_cid(cid)
    begtime = long(time.mktime(stime.timetuple())*1000)
    endtime = long(time.mktime(etime.timetuple())*1000)

    (client,trpt) = get_thrift_client(host,port)

    trpt.open()

    scan = htt.TScan()
    scan.columns = ['cf:']
    scan.caching = 110
    # scan.filterString = "RowFilter(=, 'substring:%s') AND KeyOnlyFilter()" % (numb)
    # scan.filterString = "KeyOnlyFilter()"
    scan.startRow = struct.pack(">2sQ", b_cid, begtime)
    scan.stopRow = struct.pack(">2sQ", b_cid, endtime)

    scanner = client.scannerOpenWithScan(tbl,scan,None)

    n_records = 0
    while 1:
        dataset = client.scannerGetList(scanner,buf_max_size)
        n_records += len(dataset)

        if len(dataset) < buf_max_size:
            break

        del dataset[:]

    trpt.close()

    return n_records

def calc_vehicle_traveltime(numb,ptype,stime,etime):
    from dateutil.parser import parse

    traj = query_vehicle_trajetory(numb,ptype,stime,etime)
    # print traj[0]

    traveltime = []
    for i in xrange(1,len(traj)):
        elem = (traj[i][0], traj[i][1], traj[i][2], (parse(traj[i][0])-parse(traj[i-1][0])).total_seconds())
        traveltime.append(elem)

    fname = os.path.join(root_dir,"result", "%s_traveltime.txt" % (numb.decode("gbk")))
    with open(fname, "w") as f:
        for elem in traveltime:
            line = "%s,%d,%d,%.1f\n" % (elem[0],elem[1],elem[2],elem[3])
            f.write(line)

    print "%d records written to '%s'." % (len(traveltime),fname)


if __name__ == '__main__':
    begtime = datetime(2015,6,1,0,0,0)
    endtime = datetime(2015,7,1,0,0,0)

    numb = u"鄂A78B07".encode("gbk")
    calc_vehicle_traveltime(numb,"02",begtime,endtime)

    # adj = load_edge_info(False)
    # print adj['1']

    # adj2 = load_adj_info(indegree=False)
    # print adj2['589']

    # cid = 128
    # n = stat_tgs_volume(cid,begtime,endtime)
    # print '%d: %d records.' % (cid,n)


