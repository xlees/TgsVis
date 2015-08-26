#coding: utf-8

import sys,os,time
reload(sys).setdefaultencoding('utf-8')

root_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)),os.pardir))
os.chdir(root_dir)
sys.path.insert(0,root_dir)
sys.path.insert(0, os.path.join(root_dir,"app","gen-py"))

import pandas as pd
from dateutil.rrule import *
from datetime import datetime,timedelta
import struct
import requests as req
import base64
import json
import re

# from twisted.internet import reactor, defer, task
# from twisted.internet.threads import deferToThread
# from twisted.python.failure import Failure

# from app.helper import EvilTransform

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
from collections import defaultdict


buf_max_size = 5000
ak = "Dafb6wSsEnWv8QnT3TOcAfk7"

def tree(): return defaultdict(tree)


def byte_cid(cid):
    high = (cid>>8) & 0xff
    # low = (-128) + (255-cid&0xff)

    r = cid%256
    low = -(r-127) if r>127 else r

    return struct.pack('bb',high,low)

def unbyte_cid(bstr):
    r = struct.unpack("bb", bstr)

    low = 127-r[1] if r[1]<0  else r[1]
    rs = "%s%s" % (chr(r[0]), chr(low))

    return struct.unpack(">H",rs)[0]

def daily(begtime,endtime):
    day_secs = 86400
    interval = 1

    duration = long((endtime-begtime).total_seconds())
    if duration < interval*day_secs:
        tseries = [begtime, endtime]
    else:
        tseries = list(rrule(DAILY,interval=interval,dtstart=begtime,until=endtime))
        if (duration % (interval*day_secs)) > 0:
            tseries.append(endtime)

    return tseries

def get_tseries(begtime,endtime,freq=HOURLY,interval=1):
    """
    get time series given freqency and interval.
    """
    two_list = list(rrule(freq,dtstart=begtime,interval=interval, count=2))
    secs = (two_list[1]-two_list[0]).total_seconds()

    duration = (endtime-begtime).total_seconds()
    if duration <= 0.0:
        tseries = []
    elif duration > secs:
        tseries = list(rrule(freq,interval=interval,dtstart=begtime,until=endtime))
        if tseries[-1] < endtime:
            tseries.append(endtime)
    else:
        tseries = [begtime, endtime]

    return tseries if len(tseries)>0 else []

def transform(row):
    # loc = EvilTransform.transform(row['Y'],row['X'])

    # return (str(row['CLOUD_ID']), row['KKMC'].decode("gbk").encode("utf8"))
    return (row['KKID'].decode('gbk'),
            row['KKMC'].decode("gbk"),
            str(row['CLOUD_ID']),
            float(row['X']),
            float(row['Y']))

def _conv(glist):
    url = "http://api.map.baidu.com/geoconv/v1/"

    l = []
    for coord in glist:
        loc = ','.join(map(str,coord))
        l.append(loc)
    coords = ";".join(l)

    params = {
        'coords': coords,
        'from': 1,      # GPS设备获取的坐标
        'to': 5,        # bd09ll(百度经纬度坐标)
        # 'output': 'json',
        'ak': ak,
    }

    r = req.get(url, params=params)

    return r.json()['result']

def geoconv_bd(geolist):
    from multiprocessing.dummy import Pool as ThreadPool

    max_geo = 50

    pool = ThreadPool(5)

    grp = [geolist[i:i+max_geo] for i in xrange(0,len(geolist),max_geo)]
    result = pool.map_async(_conv, grp)

    pool.close()
    pool.join()

    ret = []
    for elem in result.get():
        ret.extend(elem)

    print '%d geo converted.' % (len(ret))

    return ret

def read_tgs_info():
    """
    read tgs information.
    """
    cols = ['KKID','KKMC','CLOUD_ID','X','Y']

    # read avail tgs
    avail_tgs = set()
    p_main = re.compile("\d+,")
    with open(os.path.join(root_dir, "data", "adj_indegree.txt"), "r") as f:
        for line in f.readlines():
            try:
                main = p_main.search(line).group()[:-1]
            except AttributeError,e:
                print e.args[0], ":", line
                continue

            avail_tgs.add(main)

    cache_file = os.path.join(root_dir,"data","tgs_info_bd.txt")
    ret = {}
    if os.path.exists(cache_file):
        with open(cache_file,"r") as f:
            for line in f.readlines():
                tmp = line[:-1].split(",")
                info = {
                    'cid': tmp[0],
                    'kkid': tmp[1].decode('gbk'),
                    'kkmc': tmp[2].decode('gbk'),
                    'lng': float(tmp[3]),
                    'lat': float(tmp[4]),
                    'avail': True if tmp[0] in avail_tgs else False,
                }
                ret[tmp[0]] = info

        print '%d records loaded from %s' % (len(ret), cache_file)
    else:
        tgs_info = pd.read_csv(os.path.join(root_dir,"data","tgs_info.csv"))[cols]
        res = tgs_info.apply(transform, axis=1).tolist()

        # convert gps to baidu
        locs = [(elem[3],elem[4]) for elem in res]
        locs0 = geoconv_bd(locs)

        duplicated = {}

        for i,loc in enumerate(locs0):
            info = {
                'kkid': res[i][0],
                'kkmc': res[i][1],
                'lng': loc['x'],
                'lat': loc['y'],
                'cid': res[i][2],
                'avail': True if res[i][2] in avail_tgs else False,
            }

            if ret.has_key(res[i][2]):
                if not duplicated.has_key(res[i][2]):
                    duplicated[res[i][2]] = [ret[res[i][2]], info]
                else:
                    duplicated[res[i][2]].append(info)
                continue

            ret[res[i][2]] = info

        # print type(ret['10588']['kkid']), type(ret['10588']['kkid'].decode('gbk'))

        print '%d duplicate items.' % (len(duplicated))

        with open(os.path.join(root_dir,"result","duplicate.txt"),"w") as f:
            for key,val in duplicated.iteritems():
                for item in val:
                    line = "%s,%s,%s,%.12f,%.12f\n" % (item['cid'],
                                                       item['kkmc'].encode('gbk'),
                                                       item['kkid'].encode('gbk'),
                                                       item['lng'],
                                                       item['lat'])
                    f.write(line)
                f.write("\n")

        # write it into bd file
        with open(os.path.join(root_dir,"data","tgs_info_bd.txt"),"w") as f:
            for cid,info in ret.iteritems():
                line = "%s,%s,%s,%.12f,%.12f\n" % (cid,
                                                   info['kkid'].encode('gbk'),
                                                   info['kkmc'].encode('gbk'),
                                                   info['lng'],
                                                   info['lat'])
                f.write(line)

    n_avail = 0
    for cid,info in ret.iteritems():
        if info['avail']:
            n_avail += 1

    print '%d available tgs.' % (n_avail)

    return ret

def timeit(func, *args, **kwargs):
    def wrapper():
        pass

def gps2baidu(loc):
    url = "http://api.map.baidu.com/ag/coord/convert"
    params = {'from':0, 'to':4, 'x':loc[0], 'y':loc[1]}
    r = req.get(url, params=params)

    loc1 = json.loads(r.text)
    if loc1['error'] != 0:
        return (None,None)

    ret = (float(base64.b64decode(loc1['x'])), float(base64.b64decode(loc1['y'])))

    return ret


def get_thrift_client(host,port):
    """
    get thrift client
    """
    try:
        # Make socket
        transport = TSocket.TSocket(host, port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # Create a client to use the protocol encoder
        client = Hbase.Client(protocol)

    except Thrift.TException, tx:
        print "tx='%s'" % (tx.message)
        print type(tx)

    return (client, transport)

if __name__ == '__main__':
    # glist = [
    #     (114.21892734521,29.575429778924),
    #     (114.21892734521,29.575429778924),
    #     (114.21892734521,29.575429778924),
    #     (114.21892734521,29.575429778924),
    # ]
    # r = geoconv_bd(glist)


    tgsinfo = read_tgs_info()
    print '%d tgs fetched.' % (len(tgsinfo))

    # now = datetime.now()
    # bound = now + timedelta(days=-4)
    # tseries = daily(bound,now)

    # print tseries
