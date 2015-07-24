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

from app.helper import EvilTransform

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

from hbase import Hbase
# from hbase.ttypes import TScan,ColumnDescriptor,AlreadyExists,Mutation,BatchMutation
# import hbase.ttypes as htt

buf_max_size = 5000


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

def transform(row):
    loc = EvilTransform.transform(row['Y'],row['X'])

    # return (str(row['CLOUD_ID']), row['KKMC'].decode("gbk").encode("utf8"))
    return (row['KKID'],
            row['KKMC'].decode("gbk").encode("utf-8"),
            str(row['CLOUD_ID']),
            loc[0],
            loc[1])

def read_tgs_info():
    cols = ['KKID','KKMC','CLOUD_ID','X','Y']

    tgs_info = pd.read_csv(os.path.join(root_dir,"data","tgs_info.csv"))[cols]
    res = tgs_info.apply(transform, axis=1).tolist()

    duplicated = {}

    ret = {}
    for item in res:
        info = {
            'kkid': item[0],
            'kkmc': item[1],
            'lng': item[3],
            'lat': item[4],
            'cid': item[2],
        }

        if ret.has_key(item[2]):
            if not duplicated.has_key(item[2]):
                duplicated[item[2]] = [ret[item[2]], info]
            else:
                duplicated[item[2]].append(info)
            continue

        ret[item[2]] = info

    print '%d duplicate items.' % (len(duplicated))

    with open(os.path.join(root_dir,"result","duplicate.txt"),"w") as f:
        for key,val in duplicated.iteritems():
            for item in val:
                line = "%s,%s,%s,%.12f,%.12f\n" % (item['cid'],
                                                   item['kkmc'].decode("utf-8"),
                                                   item['kkid'],
                                                   item['lng'],
                                                   item['lat'])
                f.write(line)
            f.write("\n")

    return ret

def timeit(func, *args, **kwargs):
    def wrapper():
        pass

def get_thrift_client(host,port):
    try:
        # Make socket
        transport = TSocket.TSocket(host, port)
        # Buffering is critical. Raw sockets are very slow
        transport = TTransport.TBufferedTransport(transport)
        # Wrap in a protocol
        protocol = TBinaryProtocol.TBinaryProtocol(transport)

        # srv2 = Demo2.Client(TMultiplexedProtocol(protocol,"Demo2"))
        # srv1 = Dai.Client(TMultiplexedProtocol(protocol,"Dai"))

        # Create a client to use the protocol encoder
        client = Hbase.Client(protocol)

    except Thrift.TException, tx:
        print "tx='%s'" % (tx.message)
        print type(tx)

    return (client, transport)

if __name__ == '__main__':
    print 'shared.'

    tgsinfo = read_tgs_info()
    print '%d tgs fetched.' % (len(tgsinfo))

    now = datetime.now()
    bound = now + timedelta(days=-4)
    tseries = daily(bound,now)

    print tseries
