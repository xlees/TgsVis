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
from test.test_asso import *
from app.helper import EvilTransform
import math
from shared import *
import hbase.ttypes as htt

host, port = "10.2.25.110", 9090


def load_edge_info():
    fname = os.path.join(root_dir,"data","adj_0601.txt")

    indegree = {}
    with open(fname,"r") as f:
        for line in f.readlines():
            tmp = line[:-1].split(",")

            start = tmp[0]
            end = tmp[1]
            assoc = float(tmp[2])
            # edge = (start,end)

            if not indegree.has_key(end):
                indegree[end] = [(start,assoc)]
            else:
                indegree[end].append((start,assoc))

    # sort
    for cid in indegree.keys():
        indegree[cid].sort(key=lambda x:x[1], reverse=True)

    return indegree

def query_traj(stime,etime, numb):
    tbl = 'tr_plate61'

    (client,trpt) = get_thrift_client(host,port)

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


def read_tgs_info(fname):
    cols = ['KKID','KKMC','CLOUD_ID','X','Y']

    tgs_info = pd.read_csv(fname)[cols]
    res = tgs_info.apply(transform, axis=1).tolist()

    ret = {}
    for item in res:
        ret[item[2]] = (item[3],item[4])

    return ret

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



if __name__ == '__main__':
    # filter_pairs()

    begtime = datetime(2015,6,1)
    endtime = datetime(2015,6,2)

    numb = u"鄂AF8R13".encode('gbk')
    query_traj(begtime,endtime,numb)

    # s = time.time()

    # ttime = read_traveltime(os.path.join(root_dir,"data","pair_all.txt"), n_line=-1)

    # pool = mp.Pool(4)
    # result = []
    # for pair,val in ttime.iteritems():
    #     result.append(pool.apply_async(calc_pair,(pair,val,)))

    # print 'waiting for joining...'
    # pool.close()
    # pool.join()

    # e = time.time()

    # print 'filtering available edges...'

    # avail = [item.get() for item in result if item.get() is not None]
    # print '%d available edges in %.3f secs.' % (len(avail), (e-s))

    # print 'writing to file...'
    # tgsinfo = read_tgs_info(os.path.join(root_dir,"tgsinfo.csv"))

    # # tgs_info = get_tgs_info(os.path.join(root_dir,"tgsinfo.csv"))
    # volume = read_tgs_volume(os.path.join(root_dir,"data","volume.txt"))
    # adj = read_adj(os.path.join(root_dir,"data","week_adj.txt"))
    # link = calc_assoc(volume,adj)

    # pairs = Counter()
    # with open("avail_pairs.txt","w") as f:
    #     for item in avail:
    #         _from = repr(item[0][0])
    #         _to = repr(item[0][1])
    #         edge = (_from, _to)
    #         # print 'from,to:',tgsinfo[_from],tgsinfo[_to]

    #         dist = _calc_dist(tgsinfo[_from][0],tgsinfo[_from][1],
    #                           tgsinfo[_to][0],tgsinfo[_to][1])
    #         # line = "%s,%s: %.3f, %d, %d, %.6f\n"\
    #                 # % (item[0][0],item[0][1],
    #                    # (dist*3600)/item[1]*3.6, item[2], item[3], link[edge])
    #         line = "%s,%s: %.4f, %.1f, %d, %d, %.6f\n"\
    #                 % (item[0][0],item[0][1],
    #                    dist, item[1], item[2], item[3], link[edge])

    #         try:
    #             pairs[edge] = (dist*1000)/item[1]*3.6
    #         except ZeroDivisionError,e:
    #             continue

    #         f.write(line)
    # print 'write to file finished.'

    # with open("pairs.txt","w") as f:
    #     for item in pairs.most_common():
    #         line = "%s,%s: %.3f\n" % (item[0][0],item[0][1], item[1])
    #         f.write(line)
    # print "done."

    # indx = random.sample(range(len(avail)), 5)
    # for i in indx:
    #     print avail[i]


    # host, port = "10.2.25.115", 9090
    # host, port = "10.2.15.3", 9090

    # (client,trpt) = get_client(host,port)

    # trpt.open()

    # tbls = client.getTableNames()
    # print tbls

    # # res = client.getStandardTime()
    # # print 'res=',res

    # # for i in xrange(1,8):
    # #     print client.statVehicles("2015,06,0%d" % (i),"y-m-d-h-30m",True)

    # # print client.statVehicles("2015,23","y-w-D",False)

    # # loc = client.statLocation("2015,06,03|y-m-d-h-30m","中国,湖北|z-p-c","")
    # # print loc

    # trpt.close()
